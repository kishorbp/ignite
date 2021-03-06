/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.database.tree;

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.FixCountRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.FixLeftmostChildRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.FixRemoveId;
import org.apache.ignite.internal.pagemem.wal.record.delta.InsertRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageAddRootRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageCutRootRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRootInlineRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.NewRootInitRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.RemoveRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.ReplaceRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.SplitExistingPageRecord;
import org.apache.ignite.internal.processors.cache.database.DataStructure;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.GridArrays;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridTreePrinter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Bool.DONE;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Bool.FALSE;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Bool.READY;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Bool.TRUE;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Result.FOUND;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Result.GO_DOWN;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Result.GO_DOWN_X;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Result.NOT_FOUND;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Result.RETRY;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Result.RETRY_ROOT;

/**
 * Abstract B+Tree.
 */
@SuppressWarnings({"RedundantThrowsDeclaration", "ConstantValueVariableUse"})
public abstract class BPlusTree<L, T extends L> extends DataStructure implements IgniteTree<L, T> {
    /** */
    private static final Object[] EMPTY = {};

    /** */
    private static volatile boolean interrupted;

    /** */
    private final AtomicBoolean destroyed = new AtomicBoolean(false);

    /** */
    private final String name;

    /** */
    private final float minFill;

    /** */
    private final float maxFill;

    /** */
    protected final long metaPageId;

    /** */
    private boolean canGetRowFromInner;

    /** */
    private IOVersions<? extends BPlusInnerIO<L>> innerIos;

    /** */
    private IOVersions<? extends BPlusLeafIO<L>> leafIos;

    /** */
    private final AtomicLong globalRmvId;

    /** */
    private volatile TreeMetaData treeMeta;

    /** */
    private final GridTreePrinter<Long> treePrinter = new GridTreePrinter<Long>() {
        /** */
        private boolean keys = true;

        @Override protected List<Long> getChildren(final Long pageId) {
            if (pageId == null || pageId == 0L)
                return null;

            try {
                long page = acquirePage(pageId);

                try {
                    long pageAddr = readLock(pageId, page); // No correctness guaranties.

                    try {
                        BPlusIO io = io(pageAddr);

                        if (io.isLeaf())
                            return null;

                        int cnt = io.getCount(pageAddr);

                        assert cnt >= 0 : cnt;

                        List<Long> res;

                        if (cnt > 0) {
                            res = new ArrayList<>(cnt + 1);

                            for (int i = 0; i < cnt; i++)
                                res.add(inner(io).getLeft(pageAddr, i));

                            res.add(inner(io).getRight(pageAddr, cnt - 1));
                        }
                        else {
                            long left = inner(io).getLeft(pageAddr, 0);

                            res = left == 0 ? Collections.<Long>emptyList() : Collections.singletonList(left);
                        }

                        return res;
                    }
                    finally {
                        readUnlock(pageId, page, pageAddr);
                    }
                }
                finally {
                    releasePage(pageId, page);
                }
            }
            catch (IgniteCheckedException ignored) {
                throw new AssertionError("Can not acquire page.");
            }
        }

        @Override protected String formatTreeNode(final Long pageId) {
            if (pageId == null)
                return ">NPE<";

            if (pageId == 0L)
                return "<Zero>";

            try {
                long page = acquirePage(pageId);
                try {
                    long pageAddr = readLock(pageId, page); // No correctness guaranties.
                    try {
                        BPlusIO<L> io = io(pageAddr);

                        return printPage(io, pageAddr, keys);
                    }
                    finally {
                        readUnlock(pageId, page, pageAddr);
                    }
                }
                finally {
                    releasePage(pageId, page);
                }
            }
            catch (IgniteCheckedException e) {
                throw new IllegalStateException(e);
            }
        }
    };

    /** */
    private final GetPageHandler<Get> askNeighbor = new AskNeighbor();

    /**
     *
     */
    private class AskNeighbor extends GetPageHandler<Get> {
        /** {@inheritDoc} */
        @Override public Result run0(long pageId, long page, long pageAddr, BPlusIO<L> io, Get g, int isBack) {
            assert !io.isLeaf(); // Inner page.

            boolean back = isBack == TRUE.ordinal();

            long res = doAskNeighbor(io, pageAddr, back);

            if (back) {
                if (io.getForward(pageAddr) != g.backId) // See how g.backId is setup in removeDown for this check.
                    return RETRY;

                g.backId(res);
            }
            else {
                assert isBack == FALSE.ordinal() : isBack;

                g.fwdId(res);
            }

            return FOUND;
        }
    }

    /** */
    private final GetPageHandler<Get> search = new Search();

    /**
     *
     */
    private class Search extends GetPageHandler<Get> {
        /** {@inheritDoc} */
        @Override public Result run0(long pageId, long page, long pageAddr, BPlusIO<L> io, Get g, int lvl)
            throws IgniteCheckedException {
            // Check the triangle invariant.
            if (io.getForward(pageAddr) != g.fwdId)
                return RETRY;

            boolean needBackIfRouting = g.backId != 0;

            g.backId(0L); // Usually we'll go left down and don't need it.

            int cnt = io.getCount(pageAddr);

            int idx;

            if (g.findLast)
                idx = io.isLeaf() ? cnt - 1 : -cnt - 1; // (-cnt - 1) mimics not_found result of findInsertionPoint
                // in case of cnt = 0 we end up in 'not found' branch below with idx being 0 after fix() adjustment
            else
                idx = findInsertionPoint(lvl, io, pageAddr, 0, cnt, g.row, g.shift);

            boolean found = idx >= 0;

            if (found) { // Found exact match.
                assert g.getClass() != GetCursor.class;

                if (g.found(io, pageAddr, idx, lvl))
                    return FOUND;

                // Else we need to reach leaf page, go left down.
            }
            else {
                idx = fix(idx);

                if (g.notFound(io, pageAddr, idx, lvl)) // No way down, stop here.
                    return NOT_FOUND;
            }

            assert !io.isLeaf() : io;

            // If idx == cnt then we go right down, else left down: getLeft(cnt) == getRight(cnt - 1).
            g.pageId(inner(io).getLeft(pageAddr, idx));

            // If we see the tree in consistent state, then our right down page must be forward for our left down page,
            // we need to setup fwdId and/or backId to be able to check this invariant on lower level.
            if (idx < cnt) {
                // Go left down here.
                g.fwdId(inner(io).getRight(pageAddr, idx));
            }
            else {
                // Go right down here or it is an empty branch.
                assert idx == cnt;

                // Here child's forward is unknown to us (we either go right or it is an empty "routing" page),
                // need to ask our forward about the child's forward (it must be leftmost child of our forward page).
                // This is ok from the locking standpoint because we take all locks in the forward direction.
                long fwdId = io.getForward(pageAddr);

                // Setup fwdId.
                if (fwdId == 0)
                    g.fwdId(0L);
                else {
                    // We can do askNeighbor on forward page here because we always take locks in forward direction.
                    Result res = askNeighbor(fwdId, g, false);

                    if (res != FOUND)
                        return res; // Retry.
                }

                // Setup backId.
                if (cnt != 0) // It is not a routing page and we are going to the right, can get backId here.
                    g.backId(inner(io).getLeft(pageAddr, cnt - 1));
                else if (needBackIfRouting) {
                    // Can't get backId here because of possible deadlock and it is only needed for remove operation.
                    return GO_DOWN_X;
                }
            }

            return GO_DOWN;
        }
    }

    /** */
    private final GetPageHandler<Put> replace = new Replace();

    /**
     *
     */
    private class Replace extends GetPageHandler<Put> {
        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Result run0(long pageId, long page, long pageAddr, BPlusIO<L> io, Put p, int lvl)
            throws IgniteCheckedException  {
            // Check the triangle invariant.
            if (io.getForward(pageAddr) != p.fwdId)
                return RETRY;

            assert p.btmLvl == 0 : "split is impossible with replace";

            final int cnt = io.getCount(pageAddr);
            final int idx = findInsertionPoint(lvl, io, pageAddr, 0, cnt, p.row, 0);

            if (idx < 0) // Not found, split or merge happened.
                return RETRY;

            // Replace link at idx with new one.
            // Need to read link here because `p.finish()` will clear row.
            L newRow = p.row;

            // Detach the old row if we are on a leaf page.
            if (lvl == 0) {
                assert p.oldRow == null; // The old row must be set only once.

                // Inner replace state must be consistent by the end of the operation.
                assert p.needReplaceInner == FALSE || p.needReplaceInner == DONE : p.needReplaceInner;

                // Need to replace inner key if now we are replacing the rightmost row and have a forward page.
                if (canGetRowFromInner && idx + 1 == cnt && p.fwdId != 0L && p.needReplaceInner == FALSE) {
                    // Can happen only for invoke, otherwise inner key must be replaced on the way down.
                    assert p.invoke != null;

                    // We need to restart the operation from root to perform inner replace.
                    // On the second pass we will not get here (will avoid infinite loop) because
                    // needReplaceInner will be DONE or our key will not be the rightmost anymore.
                    return RETRY_ROOT;
                }

                // Get old row in leaf page to reduce contention at upper level.
                p.oldRow = p.needOld ? getRow(io, pageAddr, idx) : (T)Boolean.TRUE;

                p.finish();
            }

            boolean needWal = needWalDeltaRecord(pageId, page, null);

            byte[] newRowBytes = io.store(pageAddr, idx, newRow, null, needWal);

            if (needWal)
                wal.log(new ReplaceRecord<>(cacheId, pageId, io, newRowBytes, idx));

            return FOUND;
        }
    }

    /** */
    private final GetPageHandler<Put> insert = new Insert();

    /**
     *
     */
    private class Insert extends GetPageHandler<Put> {
        /** {@inheritDoc} */
        @Override public Result run0(long pageId, long page, long pageAddr, BPlusIO<L> io, Put p, int lvl)
            throws IgniteCheckedException {
            assert p.btmLvl == lvl : "we must always insert at the bottom level: " + p.btmLvl + " " + lvl;

            // Check triangle invariant.
            if (io.getForward(pageAddr) != p.fwdId)
                return RETRY;

            int cnt = io.getCount(pageAddr);
            int idx = findInsertionPoint(lvl, io, pageAddr, 0, cnt, p.row, 0);

            if (idx >= 0) // We do not support concurrent put of the same key.
                throw new IllegalStateException("Duplicate row in index.");

            idx = fix(idx);

            // Do insert.
            L moveUpRow = p.insert(pageId, page, pageAddr, io, idx, lvl);

            // Check if split happened.
            if (moveUpRow != null) {
                p.btmLvl++; // Get high.
                p.row = moveUpRow;

                if (p.invoke != null)
                    p.invoke.row = moveUpRow;

                // Here forward page can't be concurrently removed because we keep write lock on tail which is the only
                // page who knows about the forward page, because it was just produced by split.
                p.rightId = io.getForward(pageAddr);
                p.tail(pageId, page, pageAddr);

                assert p.rightId != 0;
            }
            else
                p.finish();

            return FOUND;
        }
    }

    /** */
    private final GetPageHandler<Remove> rmvFromLeaf = new RemoveFromLeaf();

    /**
     *
     */
    private class RemoveFromLeaf extends GetPageHandler<Remove> {
        /** {@inheritDoc} */
        @Override public Result run0(long leafId, long leafPage, long leafAddr, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            assert lvl == 0 : lvl; // Leaf.

            // Check the triangle invariant.
            if (io.getForward(leafAddr) != r.fwdId)
                return RETRY;

            final int cnt = io.getCount(leafAddr);

            assert cnt <= Short.MAX_VALUE: cnt;

            int idx = findInsertionPoint(lvl, io, leafAddr, 0, cnt, r.row, 0);

            if (idx < 0)
                return RETRY; // We've found exact match on search but now it's gone.

            assert idx >= 0 && idx < cnt: idx;

            // Need to do inner replace when we remove the rightmost element and the leaf have no forward page,
            // i.e. it is not the rightmost leaf of the tree.
            boolean needReplaceInner = canGetRowFromInner && idx == cnt - 1 && io.getForward(leafAddr) != 0;

            // !!! Before modifying state we have to make sure that we will not go for retry.

            // We may need to replace inner key or want to merge this leaf with sibling after the remove -> keep lock.
            if (needReplaceInner ||
                // We need to make sure that we have back or forward to be able to merge.
                ((r.fwdId != 0 || r.backId != 0) && mayMerge(cnt - 1, io.getMaxCount(leafAddr, pageSize())))) {
                // If we have backId then we've already locked back page, nothing to do here.
                if (r.fwdId != 0 && r.backId == 0) {
                    Result res = r.lockForward(0);

                    if (res != FOUND) {
                        assert r.tail == null;

                        return res; // Retry.
                    }

                    assert r.tail != null; // We've just locked forward page.
                }

                // Retry must reset these fields when we release the whole branch without remove.
                assert r.needReplaceInner == FALSE: "needReplaceInner";
                assert r.needMergeEmptyBranch == FALSE: "needMergeEmptyBranch";

                if (cnt == 1) // It was the last element on the leaf.
                    r.needMergeEmptyBranch = TRUE;

                if (needReplaceInner)
                    r.needReplaceInner = TRUE;

                Tail<L> t = r.addTail(leafId, leafPage, leafAddr, io, 0, Tail.EXACT);

                t.idx = (short)idx;

                // We will do the actual remove only when we made sure that
                // we've locked the whole needed branch correctly.
                return FOUND;
            }

            r.removeDataRowFromLeaf(leafId, leafPage, leafAddr, null, io, cnt, idx);

            return FOUND;
        }
    }

    /** */
    private final GetPageHandler<Remove> lockBackAndRmvFromLeaf = new LockBackAndRmvFromLeaf();

    /**
     *
     */
    private class LockBackAndRmvFromLeaf extends GetPageHandler<Remove> {
        /** {@inheritDoc} */
        @Override protected Result run0(long backId, long backPage, long backAddr, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            // Check that we have consistent view of the world.
            if (io.getForward(backAddr) != r.pageId)
                return RETRY;

            // Correct locking order: from back to forward.
            Result res = r.doRemoveFromLeaf();

            // Keep locks on back and leaf pages for subsequent merges.
            if (res == FOUND && r.tail != null)
                r.addTail(backId, backPage, backAddr, io, lvl, Tail.BACK);

            return res;
        }
    }

    /** */
    private final GetPageHandler<Remove> lockBackAndTail = new LockBackAndTail();

    /**
     *
     */
    private class LockBackAndTail extends GetPageHandler<Remove> {
        /** {@inheritDoc} */
        @Override public Result run0(long backId, long backPage, long backAddr, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            // Check that we have consistent view of the world.
            if (io.getForward(backAddr) != r.pageId)
                return RETRY;

            // Correct locking order: from back to forward.
            Result res = r.doLockTail(lvl);

            if (res == FOUND)
                r.addTail(backId, backPage, backAddr, io, lvl, Tail.BACK);

            return res;
        }
    }

    /** */
    private final GetPageHandler<Remove> lockTailForward = new LockTailForward();

    /**
     *
     */
    private class LockTailForward extends GetPageHandler<Remove> {
        /** {@inheritDoc} */
        @Override protected Result run0(long pageId, long page, long pageAddr, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            r.addTail(pageId, page, pageAddr, io, lvl, Tail.FORWARD);

            return FOUND;
        }
    }

    /** */
    private final GetPageHandler<Remove> lockTail = new LockTail();

    /**
     *
     */
    private class LockTail extends GetPageHandler<Remove> {
        /** {@inheritDoc} */
        @Override public Result run0(long pageId, long page, long pageAddr, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            assert lvl > 0 : lvl; // We are not at the bottom.

            // Check that we have a correct view of the world.
            if (io.getForward(pageAddr) != r.fwdId)
                return RETRY;

            // We don't have a back page, need to lock our forward and become a back for it.
            if (r.fwdId != 0 && r.backId == 0) {
                Result res = r.lockForward(lvl);

                if (res != FOUND)
                    return res; // Retry.
            }

            r.addTail(pageId, page, pageAddr, io, lvl, Tail.EXACT);

            return FOUND;
        }
    }

    /** */
    private final PageHandler<Void, Bool> cutRoot = new CutRoot();

    /**
     *
     */
    private class CutRoot extends PageHandler<Void, Bool> {
        /** {@inheritDoc} */
        @Override public Bool run(int cacheId, long metaId, long metaPage, long metaAddr, PageIO iox, Boolean walPlc, Void ignore, int lvl)
            throws IgniteCheckedException {
            // Safe cast because we should never recycle meta page until the tree is destroyed.
            BPlusMetaIO io = (BPlusMetaIO)iox;

            assert lvl == io.getRootLevel(metaAddr); // Can drop only root.

            io.cutRoot(metaAddr, pageSize());

            if (needWalDeltaRecord(metaId, metaPage, walPlc))
                wal.log(new MetaPageCutRootRecord(cacheId, metaId));

            int newLvl = lvl - 1;

            assert io.getRootLevel(metaAddr) == newLvl;

            treeMeta = new TreeMetaData(newLvl, io.getFirstPageId(metaAddr, newLvl));

            return TRUE;
        }
    }

    /** */
    private final PageHandler<Long, Bool> addRoot = new AddRoot();

    /**
     *
     */
    private class AddRoot extends PageHandler<Long, Bool> {
        /** {@inheritDoc} */
        @Override public Bool run(int cacheId, long metaId, long metaPage, long pageAddr, PageIO iox, Boolean walPlc, Long rootPageId, int lvl)
            throws IgniteCheckedException {
            assert rootPageId != null;

            // Safe cast because we should never recycle meta page until the tree is destroyed.
            BPlusMetaIO io = (BPlusMetaIO)iox;

            assert lvl == io.getLevelsCount(pageAddr);

            io.addRoot(pageAddr, rootPageId, pageSize());

            if (needWalDeltaRecord(metaId, metaPage, walPlc))
                wal.log(new MetaPageAddRootRecord(cacheId, metaId, rootPageId));

            assert io.getRootLevel(pageAddr) == lvl;
            assert io.getFirstPageId(pageAddr, lvl) == rootPageId;

            treeMeta = new TreeMetaData(lvl, rootPageId);

            return TRUE;
        }
    }

    /** */
    private final PageHandler<Long, Bool> initRoot = new InitRoot();

    /**
     *
     */
    private class InitRoot extends PageHandler<Long, Bool> {
        /** {@inheritDoc} */
        @Override public Bool run(int cacheId, long metaId, long metaPage, long pageAddr, PageIO iox, Boolean walPlc, Long rootId, int inlineSize)
            throws IgniteCheckedException {
            assert rootId != null;

            // Safe cast because we should never recycle meta page until the tree is destroyed.
            BPlusMetaIO io = (BPlusMetaIO)iox;

            io.initRoot(pageAddr, rootId, pageSize());
            io.setInlineSize(pageAddr, inlineSize);

            if (needWalDeltaRecord(metaId, metaPage, walPlc))
                wal.log(new MetaPageInitRootInlineRecord(cacheId, metaId, rootId, inlineSize));

            assert io.getRootLevel(pageAddr) == 0;
            assert io.getFirstPageId(pageAddr, 0) == rootId;

            treeMeta = new TreeMetaData(0, rootId);

            return TRUE;
        }
    }

    /**
     * @param name Tree name.
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @param globalRmvId Remove ID.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param innerIos Inner IO versions.
     * @param leafIos Leaf IO versions.
     * @throws IgniteCheckedException If failed.
     */
    protected BPlusTree(
        String name,
        int cacheId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        long metaPageId,
        ReuseList reuseList,
        IOVersions<? extends BPlusInnerIO<L>> innerIos,
        IOVersions<? extends BPlusLeafIO<L>> leafIos
    ) throws IgniteCheckedException {
        this(name, cacheId, pageMem, wal, globalRmvId, metaPageId, reuseList);
        setIos(innerIos, leafIos);
    }

    /**
     * @param name Tree name.
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @param globalRmvId Remove ID.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @throws IgniteCheckedException If failed.
     */
    protected BPlusTree(
        String name,
        int cacheId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        long metaPageId,
        ReuseList reuseList
    ) throws IgniteCheckedException {
        super(cacheId, pageMem, wal);

        assert !F.isEmpty(name);

        // TODO make configurable: 0 <= minFill <= maxFill <= 1
        minFill = 0f; // Testing worst case when merge happens only on empty page.
        maxFill = 0f; // Avoiding random effects on testing.

        assert metaPageId != 0L;

        this.metaPageId = metaPageId;
        this.name = name;
        this.reuseList = reuseList;
        this.globalRmvId = globalRmvId;
    }

    /**
     * @param innerIos Inner IO versions.
     * @param leafIos Leaf IO versions.
     */
    protected void setIos(IOVersions<? extends BPlusInnerIO<L>> innerIos,
        IOVersions<? extends BPlusLeafIO<L>> leafIos) {
        assert innerIos != null;
        assert leafIos != null;

        this.canGetRowFromInner = innerIos.latest().canGetRow(); // TODO refactor
        this.innerIos = innerIos;
        this.leafIos = leafIos;
    }

    /**
     * @return Tree name.
     */
    public final String getName() {
        return name;
    }

    /**
     * Initialize new tree.
     *
     * @param initNew {@code True} if new tree should be created.
     * @throws IgniteCheckedException If failed.
     */
    protected final void initTree(boolean initNew) throws IgniteCheckedException {
        initTree(initNew, 0);
    }

    /**
     * Initialize new tree.
     *
     * @param initNew {@code True} if new tree should be created.
     * @param inlineSize Inline size.
     * @throws IgniteCheckedException If failed.
     */
    protected final void initTree(boolean initNew, int inlineSize) throws IgniteCheckedException {
        if (initNew) {
            // Allocate the first leaf page, it will be our root.
            long rootId = allocatePage(null);

            init(rootId, latestLeafIO());

            // Initialize meta page with new root page.
            Bool res = write(metaPageId, initRoot, BPlusMetaIO.VERSIONS.latest(), rootId, inlineSize, FALSE);

            assert res == TRUE: res;

            assert treeMeta != null;
        }
    }

    /**
     * @return Tree meta data.
     * @throws IgniteCheckedException If failed.
     */
    private TreeMetaData treeMeta() throws IgniteCheckedException {
        TreeMetaData meta0 = treeMeta;

        if (meta0 != null)
            return meta0;

        final long metaPage = acquirePage(metaPageId);
        try {
            long pageAddr = readLock(metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" +
                U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(pageAddr);

                int rootLvl = io.getRootLevel(pageAddr);
                long rootId = io.getFirstPageId(pageAddr, rootLvl);

                treeMeta = meta0 = new TreeMetaData(rootLvl, rootId);
            }
            finally {
                readUnlock(metaPageId, metaPage, pageAddr);
            }
        }
        finally {
            releasePage(metaPageId, metaPage);
        }

        return meta0;
    }

    /**
     * @return Root level.
     * @throws IgniteCheckedException If failed.
     */
    private int getRootLevel() throws IgniteCheckedException {
        TreeMetaData meta0 = treeMeta();

        assert meta0 != null;

        return meta0.rootLvl;
    }

    /**
     * @param metaId Meta page ID.
     * @param metaPage Meta page pointer.
     * @param lvl Level, if {@code 0} then it is a bottom level, if negative then root.
     * @return Page ID.
     */
    private long getFirstPageId(long metaId, long metaPage, int lvl) {
        long pageAddr = readLock(metaId, metaPage); // Meta can't be removed.

        try {
            BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(pageAddr);

            if (lvl < 0)
                lvl = io.getRootLevel(pageAddr);

            if (lvl >= io.getLevelsCount(pageAddr))
                return 0;

            return io.getFirstPageId(pageAddr, lvl);
        }
        finally {
            readUnlock(metaId, metaPage, pageAddr);
        }
    }

    /**
     * @param upper Upper bound.
     * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
     * @return Cursor.
     * @throws IgniteCheckedException If failed.
     */
    private GridCursor<T> findLowerUnbounded(L upper, Object x) throws IgniteCheckedException {
        ForwardCursor cursor = new ForwardCursor(null, upper, x);

        long firstPageId;

        long metaPage = acquirePage(metaPageId);
        try  {
            firstPageId = getFirstPageId(metaPageId, metaPage, 0); // Level 0 is always at the bottom.
        }
        finally {
            releasePage(metaPageId, metaPage);
        }

        long firstPage = acquirePage(firstPageId);

        try {
            long pageAddr = readLock(firstPageId, firstPage); // We always merge pages backwards, the first page is never removed.

            try {
                cursor.init(pageAddr, io(pageAddr), 0);
            }
            finally {
                readUnlock(firstPageId, firstPage, pageAddr);
            }
        }
        finally {
            releasePage(firstPageId, firstPage);
        }

        return cursor;
    }

    /**
     * Check if the tree is getting destroyed.
     */
    private void checkDestroyed() {
        if (destroyed.get())
            throw new IllegalStateException("Tree is being concurrently destroyed: " + getName());
    }

    /**
     * @param lower Lower bound inclusive or {@code null} if unbounded.
     * @param upper Upper bound inclusive or {@code null} if unbounded.
     * @return Cursor.
     * @throws IgniteCheckedException If failed.
     */
    @Override public GridCursor<T> find(L lower, L upper) throws IgniteCheckedException {
        return find(lower, upper, null);
    }

    /**
     * @param lower Lower bound inclusive or {@code null} if unbounded.
     * @param upper Upper bound inclusive or {@code null} if unbounded.
     * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
     * @return Cursor.
     * @throws IgniteCheckedException If failed.
     */
    public final GridCursor<T> find(L lower, L upper, Object x) throws IgniteCheckedException {
        checkDestroyed();

        try {
            if (lower == null)
                return findLowerUnbounded(upper, x);

            ForwardCursor cursor = new ForwardCursor(lower, upper, x);

            cursor.find();

            return cursor;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Runtime failure on bounds: [lower=" + lower + ", upper=" + upper + "]", e);
        }
        catch (RuntimeException e) {
            throw new IgniteException("Runtime failure on bounds: [lower=" + lower + ", upper=" + upper + "]", e);
        }
        catch (AssertionError e) {
            throw new AssertionError("Assertion error on bounds: [lower=" + lower + ", upper=" + upper + "]", e);
        }
        finally {
            checkDestroyed();
        }
    }

    /** {@inheritDoc} */
    @Override public T findFirst() throws IgniteCheckedException {
        checkDestroyed();

        try {
            long firstPageId;

            long metaPage = acquirePage(metaPageId);
            try {
                firstPageId = getFirstPageId(metaPageId, metaPage, 0);
            }
            finally {
                releasePage(metaPageId, metaPage);
            }

            long page = acquirePage(firstPageId);

            try {
                long pageAddr = readLock(firstPageId, page);

                try {
                    BPlusIO<L> io = io(pageAddr);

                    int cnt = io.getCount(pageAddr);

                    if (cnt == 0)
                        return null;

                    return getRow(io, pageAddr, 0);
                }
                finally {
                    readUnlock(firstPageId, page, pageAddr);
                }
            }
            finally {
                releasePage(firstPageId, page);
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Runtime failure on first row lookup", e);
        }
        catch (RuntimeException e) {
            throw new IgniteException("Runtime failure on first row lookup", e);
        }
        catch (AssertionError e) {
            throw new AssertionError("Assertion error on first row lookup", e);
        }
        finally {
            checkDestroyed();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public T findLast() throws IgniteCheckedException {
        checkDestroyed();

        try {
            GetOne g = new GetOne(null, null, true);
            doFind(g);

            return (T)g.row;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Runtime failure on last row lookup", e);
        }
        catch (RuntimeException e) {
            throw new IgniteException("Runtime failure on last row lookup", e);
        }
        catch (AssertionError e) {
            throw new AssertionError("Assertion error on last row lookup", e);
        }
        finally {
            checkDestroyed();
        }
    }

    /**
     * @param row Lookup row for exact match.
     * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
     * @return Found result or {@code null}.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public final <R> R findOne(L row, Object x) throws IgniteCheckedException {
        checkDestroyed();

        try {
            GetOne g = new GetOne(row, x, false);

            doFind(g);

            return (R)g.row;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Runtime failure on lookup row: " + row, e);
        }
        catch (RuntimeException e) {
            throw new IgniteException("Runtime failure on lookup row: " + row, e);
        }
        catch (AssertionError e) {
            throw new AssertionError("Assertion error on lookup row: " + row, e);
        }
        finally {
            checkDestroyed();
        }
    }

    /**
     * @param row Lookup row for exact match.
     * @return Found row.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    @Override public final T findOne(L row) throws IgniteCheckedException {
        return findOne(row, null);
    }

    /**
     * @param g Get.
     * @throws IgniteCheckedException If failed.
     */
    private void doFind(Get g) throws IgniteCheckedException {
        for (;;) { // Go down with retries.
            g.init();

            switch (findDown(g, g.rootId, 0L, g.rootLvl)) {
                case RETRY:
                case RETRY_ROOT:
                    checkInterrupted();

                    continue;

                default:
                    return;
            }
        }
    }

    /**
     * @param g Get.
     * @param pageId Page ID.
     * @param fwdId Expected forward page ID.
     * @param lvl Level.
     * @return Result code.
     * @throws IgniteCheckedException If failed.
     */
    private Result findDown(final Get g, final long pageId, final long fwdId, final int lvl)
        throws IgniteCheckedException {
        long page = acquirePage(pageId);

        try {
            for (;;) {
                // Init args.
                g.pageId = pageId;
                g.fwdId = fwdId;

                Result res = read(pageId, page, search, g, lvl, RETRY);

                switch (res) {
                    case GO_DOWN:
                    case GO_DOWN_X:
                        assert g.pageId != pageId;
                        assert g.fwdId != fwdId || fwdId == 0;

                        // Go down recursively.
                        res = findDown(g, g.pageId, g.fwdId, lvl - 1);

                        switch (res) {
                            case RETRY:
                                checkInterrupted();

                                continue; // The child page got split, need to reread our page.

                            default:
                                return res;
                        }

                    case NOT_FOUND:
                        assert lvl == 0 : lvl;

                        g.row = null; // Mark not found result.

                        return res;

                    default:
                        return res;
                }
            }
        }
        finally {
            if (g.canRelease(pageId, lvl))
                releasePage(pageId, page);
        }
    }

    /**
     * @param instance Instance name.
     * @param type Tree type.
     * @return Tree name.
     */
    public static String treeName(String instance, String type) {
        return instance + "##" + type;
    }

    /**
     * For debug.
     *
     * @return Tree as {@link String}.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unused")
    public final String printTree() throws IgniteCheckedException {
        long rootPageId;

        long metaPage = acquirePage(metaPageId);
        try {
            rootPageId = getFirstPageId(metaPageId, metaPage, -1);
        }
        finally {
            releasePage(metaPageId, metaPage);
        }

        return treePrinter.print(rootPageId);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public final void validateTree() throws IgniteCheckedException {
        long rootPageId;
        int rootLvl;

        long metaPage = acquirePage(metaPageId);
        try  {
            rootLvl = getRootLevel();

            if (rootLvl < 0)
                fail("Root level: " + rootLvl);

            validateFirstPages(metaPageId, metaPage, rootLvl);

            rootPageId = getFirstPageId(metaPageId, metaPage, rootLvl);

            validateDownPages(rootPageId, 0L, rootLvl);

            validateDownKeys(rootPageId, null, rootLvl);
        }
        finally {
            releasePage(metaPageId, metaPage);
        }
    }

    /**
     * @param pageId Page ID.
     * @param minRow Minimum row.
     * @throws IgniteCheckedException If failed.
     */
    private void validateDownKeys(long pageId, L minRow, int lvl) throws IgniteCheckedException {
        long page = acquirePage(pageId);
        try {
            long pageAddr = readLock(pageId, page); // No correctness guaranties.

            try {
                BPlusIO<L> io = io(pageAddr);

                int cnt = io.getCount(pageAddr);

                if (cnt < 0)
                    fail("Negative count: " + cnt);

                if (io.isLeaf()) {
                    for (int i = 0; i < cnt; i++) {
                        if (minRow != null && compare(lvl, io, pageAddr, i, minRow) <= 0)
                            fail("Wrong sort order: " + U.hexLong(pageId) + " , at " + i + " , minRow: " + minRow);

                        minRow = io.getLookupRow(this, pageAddr, i);
                    }

                    return;
                }

                // To find our inner key we have to go left and then always go to the right.
                for (int i = 0; i < cnt; i++) {
                    L row = io.getLookupRow(this, pageAddr, i);

                    if (minRow != null && compare(lvl, io, pageAddr, i, minRow) <= 0)
                        fail("Min row violated: " + row + " , minRow: " + minRow);

                    long leftId = inner(io).getLeft(pageAddr, i);

                    L leafRow = getGreatestRowInSubTree(leftId);

                    int cmp = compare(lvl, io, pageAddr, i, leafRow);

                    if (cmp < 0 || (cmp != 0 && canGetRowFromInner))
                        fail("Wrong inner row: " + U.hexLong(pageId) + " , at: " + i + " , leaf:  " + leafRow +
                            " , inner: " + row);

                    validateDownKeys(leftId, minRow, lvl - 1);

                    minRow = row;
                }

                // Need to handle the rightmost child subtree separately or handle empty routing page.
                long rightId = inner(io).getLeft(pageAddr, cnt); // The same as getRight(cnt - 1)

                validateDownKeys(rightId, minRow, lvl - 1);
            }
            finally {
                readUnlock(pageId, page, pageAddr);
            }
        }
        finally {
            releasePage(pageId, page);
        }
    }

    /**
     * @param pageId Page ID.
     * @return Search row.
     * @throws IgniteCheckedException If failed.
     */
    private L getGreatestRowInSubTree(long pageId) throws IgniteCheckedException {
        long page = acquirePage(pageId);
        try {
            long pageAddr = readLock(pageId, page); // No correctness guaranties.

            try {
                BPlusIO<L> io = io(pageAddr);

                int cnt = io.getCount(pageAddr);

                if (io.isLeaf()) {
                    if (cnt <= 0) // This code is called only if the tree is not empty, so we can't see empty leaf.
                        fail("Invalid leaf count: " + cnt + " " + U.hexLong(pageId));

                    return io.getLookupRow(this, pageAddr, cnt - 1);
                }

                long rightId = inner(io).getLeft(pageAddr, cnt);// The same as getRight(cnt - 1), but good for routing pages.

                return getGreatestRowInSubTree(rightId);
            }
            finally {
                readUnlock(pageId, page, pageAddr);
            }
        }
        finally {
            releasePage(pageId, page);
        }
    }

    /**
     * @param metaId Meta page ID.
     * @param metaPage Meta page pointer.
     * @param rootLvl Root level.
     * @throws IgniteCheckedException If failed.
     */
    private void validateFirstPages(long metaId, long metaPage, int rootLvl) throws IgniteCheckedException {
        for (int lvl = rootLvl; lvl > 0; lvl--)
            validateFirstPage(metaId, metaPage, lvl);
    }

    /**
     * @param msg Message.
     */
    private static void fail(Object msg) {
        throw new AssertionError(msg);
    }

    /**
     * @param metaId Meta page ID.
     * @param metaPage Meta page pointer.
     * @param lvl Level.
     * @throws IgniteCheckedException If failed.
     */
    private void validateFirstPage(long metaId, long metaPage, int lvl) throws IgniteCheckedException {
        if (lvl == 0)
            fail("Leaf level: " + lvl);

        long pageId = getFirstPageId(metaId, metaPage, lvl);

        long leftmostChildId;

        long page = acquirePage(pageId);
        try {
            long pageAddr = readLock(pageId, page); // No correctness guaranties.

            try {
                BPlusIO<L> io = io(pageAddr);

                if (io.isLeaf())
                    fail("Leaf.");

                leftmostChildId = inner(io).getLeft(pageAddr, 0);
            }
            finally {
                readUnlock(pageId, page, pageAddr);
            }
        }
        finally {
            releasePage(pageId, page);
        }

        long firstDownPageId = getFirstPageId(metaId, metaPage, lvl - 1);

        if (firstDownPageId != leftmostChildId)
            fail(new SB("First: meta ").appendHex(firstDownPageId).a(", child ").appendHex(leftmostChildId));
    }

    /**
     * @param pageId Page ID.
     * @param fwdId Forward ID.
     * @param lvl Level.
     * @throws IgniteCheckedException If failed.
     */
    private void validateDownPages(long pageId, long fwdId, int lvl) throws IgniteCheckedException {
        long page = acquirePage(pageId);
        try {
            long pageAddr = readLock(pageId, page); // No correctness guaranties.

            try {
                long realPageId = BPlusIO.getPageId(pageAddr);

                if (realPageId != pageId)
                    fail(new SB("ABA on page ID: ref ").appendHex(pageId).a(", buf ").appendHex(realPageId));

                BPlusIO<L> io = io(pageAddr);

                if (io.isLeaf() != (lvl == 0)) // Leaf pages only at the level 0.
                    fail("Leaf level mismatch: " + lvl);

                long actualFwdId = io.getForward(pageAddr);

                if (actualFwdId != fwdId)
                    fail(new SB("Triangle: expected fwd ").appendHex(fwdId).a(", actual fwd ").appendHex(actualFwdId));

                int cnt = io.getCount(pageAddr);

                if (cnt < 0)
                    fail("Negative count: " + cnt);

                if (io.isLeaf()) {
                    if (cnt == 0 && getRootLevel() != 0)
                        fail("Empty leaf page.");
                }
                else {
                    // Recursively go down if we are on inner level.
                    for (int i = 0; i < cnt; i++)
                        validateDownPages(inner(io).getLeft(pageAddr, i), inner(io).getRight(pageAddr, i), lvl - 1);

                    if (fwdId != 0) {
                        // For the rightmost child ask neighbor.
                        long fwdId0 = fwdId;
                        long fwdPage = acquirePage(fwdId0);
                        try {
                            long fwdPageAddr = readLock(fwdId0, fwdPage); // No correctness guaranties.

                            try {
                                if (io(fwdPageAddr) != io)
                                    fail("IO on the same level must be the same");

                                fwdId = inner(io).getLeft(fwdPageAddr, 0);
                            }
                            finally {
                                readUnlock(fwdId0, fwdPage, fwdPageAddr);
                            }
                        }
                        finally {
                            releasePage(fwdId0, fwdPage);
                        }
                    }

                    long leftId = inner(io).getLeft(pageAddr, cnt); // The same as io.getRight(cnt - 1) but works for routing pages.

                    validateDownPages(leftId, fwdId, lvl - 1);
                }
            }
            finally {
                readUnlock(pageId, page, pageAddr);
            }
        }
        finally {
            releasePage(pageId, page);
        }
    }

    /**
     * @param io IO.
     * @param pageAddr Page address.
     * @param keys Keys.
     * @return String.
     * @throws IgniteCheckedException If failed.
     */
    private String printPage(BPlusIO<L> io, long pageAddr, boolean keys) throws IgniteCheckedException {
        StringBuilder b = new StringBuilder();

        b.append(formatPageId(PageIO.getPageId(pageAddr)));

        b.append(" [ ");
        b.append(io.isLeaf() ? "L " : "I ");

        int cnt = io.getCount(pageAddr);
        long fwdId = io.getForward(pageAddr);

        b.append("cnt=").append(cnt).append(' ');
        b.append("fwd=").append(formatPageId(fwdId)).append(' ');

        if (!io.isLeaf()) {
            b.append("lm=").append(formatPageId(inner(io).getLeft(pageAddr, 0))).append(' ');

            if (cnt > 0)
                b.append("rm=").append(formatPageId(inner(io).getRight(pageAddr, cnt - 1))).append(' ');
        }

        if (keys)
            b.append("keys=").append(printPageKeys(io, pageAddr)).append(' ');

        b.append(']');

        return b.toString();
    }

    /**
     * @param io IO.
     * @param pageAddr Page address.
     * @return Keys as String.
     * @throws IgniteCheckedException If failed.
     */
    private String printPageKeys(BPlusIO<L> io, long pageAddr) throws IgniteCheckedException {
        int cnt = io.getCount(pageAddr);

        StringBuilder b = new StringBuilder();

        b.append('[');

        for (int i = 0; i < cnt; i++) {
            if (i != 0)
                b.append(',');

            b.append(io.isLeaf() || canGetRowFromInner ? getRow(io, pageAddr, i) : io.getLookupRow(this, pageAddr, i));
        }

        b.append(']');

        return b.toString();
    }

    /**
     * @param x Long.
     * @return String.
     */
    private static String formatPageId(long x) {
        return U.hexLong(x);
    }

    /**
     * @param idx Index after binary search, which can be negative.
     * @return Always positive index.
     */
    private static int fix(int idx) {
        assert checkIndex(idx): idx;

        if (idx < 0)
            idx = -idx - 1;

        return idx;
    }

    /**
     * @param idx Index.
     * @return {@code true} If correct.
     */
    private static boolean checkIndex(int idx) {
        return idx > -Short.MAX_VALUE && idx < Short.MAX_VALUE;
    }

    /**
     * Check if interrupted.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private static void checkInterrupted() throws IgniteInterruptedCheckedException {
        // We should not interrupt operations in the middle, because otherwise we'll end up in inconsistent state.
        // Because of that we do not check for Thread.interrupted()
        if (interrupted)
            throw new IgniteInterruptedCheckedException("Interrupted.");
    }

    /**
     * Interrupt all operations on all threads and all indexes.
     */
    @SuppressWarnings("unused")
    public static void interruptAll() {
        interrupted = true;
    }

    /**
     * @param row Lookup row.
     * @return Removed row.
     * @throws IgniteCheckedException If failed.
     */
    @Override public final T remove(L row) throws IgniteCheckedException {
        return doRemove(row, true);
    }

    /**
     * @param row Lookup row.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if removed row.
     */
    public final boolean removex(L row) throws IgniteCheckedException {
        Boolean res = (Boolean)doRemove(row, false);

        return res != null ? res : false;
    }

    /** {@inheritDoc} */
    @Override public void invoke(L row, Object z, InvokeClosure<T> c) throws IgniteCheckedException {
        checkDestroyed();

        Invoke x = new Invoke(row, z, c);

        try {
            for (;;) {
                x.init();

                Result res = invokeDown(x, x.rootId, 0L, 0L, x.rootLvl);

                switch (res) {
                    case RETRY:
                    case RETRY_ROOT:
                        checkInterrupted();

                        continue;

                    default:
                        if (!x.isFinished()) {
                            res = x.tryFinish();

                            if (res == RETRY || res == RETRY_ROOT) {
                                checkInterrupted();

                                continue;
                            }

                            assert x.isFinished(): res;
                        }

                        return;
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Runtime failure on search row: " + row, e);
        }
        catch (RuntimeException e) {
            throw new IgniteException("Runtime failure on search row: " + row, e);
        }
        catch (AssertionError e) {
            throw new AssertionError("Assertion error on search row: " + row, e);
        }
        finally {
            x.releaseAll();
            checkDestroyed();
        }
    }

    /**
     * @param x Invoke operation.
     * @param pageId Page ID.
     * @param backId Expected backward page ID if we are going to the right.
     * @param fwdId Expected forward page ID.
     * @param lvl Level.
     * @return Result code.
     * @throws IgniteCheckedException If failed.
     */
    private Result invokeDown(final Invoke x, final long pageId, final long backId, final long fwdId, final int lvl)
        throws IgniteCheckedException {
        assert lvl >= 0 : lvl;

        if (x.isTail(pageId, lvl))
            return FOUND; // We've already locked this page, so return that we are ok.

        long page = acquirePage(pageId);

        try {
            for (;;) {
                // Init args.
                x.pageId(pageId);
                x.fwdId(fwdId);
                x.backId(backId);

                Result res = read(pageId, page, search, x, lvl, RETRY);

                switch (res) {
                    case GO_DOWN_X:
                        assert backId != 0;
                        assert x.backId == 0; // We did not setup it yet.

                        x.backId(pageId); // Dirty hack to setup a check inside of askNeighbor.

                        // We need to get backId here for our child page, it must be the last child of our back.
                        res = askNeighbor(backId, x, true);

                        if (res != FOUND)
                            return res; // Retry.

                        assert x.backId != pageId; // It must be updated in askNeighbor.

                        // Intentional fallthrough.
                    case GO_DOWN:
                        res = x.tryReplaceInner(pageId, page, fwdId, lvl);

                        if (res != RETRY)
                            res = invokeDown(x, x.pageId, x.backId, x.fwdId, lvl - 1);

                        if (res == RETRY_ROOT || x.isFinished())
                            return res;

                        if (res == RETRY) {
                            checkInterrupted();

                            continue;
                        }

                        // Unfinished Put does insertion on the same level.
                        if (x.isPut())
                            continue;

                        assert x.isRemove(); // Guarded by isFinished.

                        res = x.finishOrLockTail(pageId, page, backId, fwdId, lvl);

                        return res;

                    case NOT_FOUND:
                        if (lvl == 0)
                            x.invokeClosure();

                        return x.onNotFound(pageId, page, fwdId, lvl);

                    case FOUND:
                        if (lvl == 0)
                            x.invokeClosure();

                        return x.onFound(pageId, page, backId, fwdId, lvl);

                    default:
                        return res;
                }
            }
        }
        finally {
            x.levelExit();

            if (x.canRelease(pageId, lvl))
                releasePage(pageId, page);
        }
    }


    /**
     * @param row Lookup row.
     * @param needOld {@code True} if need return removed row.
     * @return Removed row.
     * @throws IgniteCheckedException If failed.
     */
    private T doRemove(L row, boolean needOld) throws IgniteCheckedException {
        checkDestroyed();

        Remove r = new Remove(row, needOld);

        try {
            for (;;) {
                r.init();

                Result res = removeDown(r, r.rootId, 0L, 0L, r.rootLvl);

                switch (res) {
                    case RETRY:
                    case RETRY_ROOT:
                        checkInterrupted();

                        continue;

                    default:
                        if (!r.isFinished()) {
                            res = r.finishTail();

                            // If not found, then the tree grew beyond our call stack -> retry from the actual root.
                            if (res == RETRY || res == NOT_FOUND) {
                                assert r.checkTailLevel(getRootLevel()) : "tail=" + r.tail + ", res=" + res;

                                checkInterrupted();

                                continue;
                            }

                            assert res == FOUND: res;
                        }

                        assert r.isFinished();

                        return r.rmvd;
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Runtime failure on search row: " + row, e);
        }
        catch (RuntimeException e) {
            throw new IgniteException("Runtime failure on search row: " + row, e);
        }
        catch (AssertionError e) {
            throw new AssertionError("Assertion error on search row: " + row, e);
        }
        finally {
            r.releaseAll();
            checkDestroyed();
        }
    }

    /**
     * @param r Remove operation.
     * @param pageId Page ID.
     * @param backId Expected backward page ID if we are going to the right.
     * @param fwdId Expected forward page ID.
     * @param lvl Level.
     * @return Result code.
     * @throws IgniteCheckedException If failed.
     */
    private Result removeDown(final Remove r, final long pageId, final long backId, final long fwdId, final int lvl)
        throws IgniteCheckedException {
        assert lvl >= 0 : lvl;

        if (r.isTail(pageId, lvl))
            return FOUND; // We've already locked this page, so return that we are ok.

        long page = acquirePage(pageId);

        try {
            for (;;) {
                // Init args.
                r.pageId = pageId;
                r.fwdId = fwdId;
                r.backId = backId;

                Result res = read(pageId, page, search, r, lvl, RETRY);

                switch (res) {
                    case GO_DOWN_X:
                        assert backId != 0;
                        assert r.backId == 0; // We did not setup it yet.

                        r.backId = pageId; // Dirty hack to setup a check inside of askNeighbor.

                        // We need to get backId here for our child page, it must be the last child of our back.
                        res = askNeighbor(backId, r, true);

                        if (res != FOUND)
                            return res; // Retry.

                        assert r.backId != pageId; // It must be updated in askNeighbor.

                        // Intentional fallthrough.
                    case GO_DOWN:
                        res = removeDown(r, r.pageId, r.backId, r.fwdId, lvl - 1);

                        if (res == RETRY) {
                            checkInterrupted();

                            continue;
                        }

                        if (res == RETRY_ROOT || r.isFinished())
                            return res;

                        res = r.finishOrLockTail(pageId, page, backId, fwdId, lvl);

                        return res;

                    case NOT_FOUND:
                        // We are at the bottom.
                        assert lvl == 0 : lvl;

                        r.finish();

                        return res;

                    case FOUND:
                        return r.tryRemoveFromLeaf(pageId, page, backId, fwdId, lvl);

                    default:
                        return res;
                }
            }
        }
        finally {
            r.page = 0L;

            if (r.canRelease(pageId, lvl))
                releasePage(pageId, page);
        }
    }

    /**
     * @param cnt Count.
     * @param cap Capacity.
     * @return {@code true} If may merge.
     */
    private boolean mayMerge(int cnt, int cap) {
        int minCnt = (int)(minFill * cap);

        if (cnt <= minCnt) {
            assert cnt == 0; // TODO remove

            return true;
        }

        assert cnt > 0;

        int maxCnt = (int)(maxFill * cap);

        if (cnt > maxCnt)
            return false;

        assert false; // TODO remove

        // Randomization is for smoothing worst case scenarios. Probability of merge attempt
        // is proportional to free space in our page (discounted on fill factor).
        return randomInt(maxCnt - minCnt) >= cnt - minCnt;
    }

    /**
     * @return Root level.
     * @throws IgniteCheckedException If failed.
     */
    public final int rootLevel() throws IgniteCheckedException {
        checkDestroyed();

        return getRootLevel();
    }

    /**
     * !!! For debug only! May produce wrong results on concurrent access.
     *
     * @return Size.
     * @throws IgniteCheckedException If failed.
     */
    @Override public final long size() throws IgniteCheckedException {
        checkDestroyed();

        long pageId;

        long metaPage = acquirePage(metaPageId);
        try {
            pageId = getFirstPageId(metaPageId, metaPage, 0); // Level 0 is always at the bottom.
        }
        finally {
            releasePage(metaPageId, metaPage);
        }

        BPlusIO<L> io = null;

        long cnt = 0;

        while (pageId != 0) {
            long curId = pageId;
            long curPage = acquirePage(curId);
            try {
                long curAddr = readLock(curId, curPage); // No correctness guaranties.

                try {
                    if (io == null) {
                        io = io(curAddr);

                        assert io.isLeaf();
                    }

                    cnt += io.getCount(curAddr);

                    pageId = io.getForward(curAddr);
                }
                finally {
                    readUnlock(curId, curPage, curAddr);
                }
            }
            finally {
                releasePage(curId, curPage);
            }
        }

        checkDestroyed();

        return cnt;
    }

    /**
     * {@inheritDoc}
     */
    @Override public final T put(T row) throws IgniteCheckedException {
        return doPut(row, true);
    }

    /**
     * @param row New value.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if replaced existing row.
     */
    public boolean putx(T row) throws IgniteCheckedException {
        Boolean res = (Boolean)doPut(row, false);

        return res != null ? res : false;
    }

    /**
     * @param row New value.
     * @param needOld {@code True} If need return old value.
     * @return Old row.
     * @throws IgniteCheckedException If failed.
     */
    private T doPut(T row, boolean needOld) throws IgniteCheckedException {
        checkDestroyed();

        Put p = new Put(row, needOld);

        try {
            for (;;) { // Go down with retries.
                p.init();

                Result res = putDown(p, p.rootId, 0L, p.rootLvl);

                switch (res) {
                    case RETRY:
                    case RETRY_ROOT:
                        checkInterrupted();

                        continue;

                    case FOUND:
                        // We may need to insert split key into upper level here.
                        if (!p.isFinished()) {
                            // It must be impossible to have an insert higher than the current root,
                            // because we are making decision about creating new root while keeping
                            // write lock on current root, so it can't concurrently change.
                            assert p.btmLvl <= getRootLevel();

                            checkInterrupted();

                            continue;
                        }

                        return p.oldRow;

                    default:
                        throw new IllegalStateException("Result: " + res);
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Runtime failure on row: " + row, e);
        }
        catch (RuntimeException e) {
            throw new IgniteException("Runtime failure on row: " + row, e);
        }
        catch (AssertionError e) {
            throw new AssertionError("Assertion error on row: " + row, e);
        }
        finally {
            checkDestroyed();
        }
    }

    /**
     * Destroys tree. This method is allowed to be invoked only when the tree is out of use (no concurrent operations
     * are trying to read or update the tree after destroy beginning).
     *
     * @return Number of pages recycled from this tree. If the tree was destroyed by someone else concurrently returns
     *     {@code 0}, otherwise it should return at least {@code 2} (for meta page and root page), unless this tree is
     *     used as metadata storage, or {@code -1} if we don't have a reuse list and did not do recycling at all.
     * @throws IgniteCheckedException If failed.
     */
    public final long destroy() throws IgniteCheckedException {
        return destroy(null);
    }

    /**
     * Destroys tree. This method is allowed to be invoked only when the tree is out of use (no concurrent operations
     * are trying to read or update the tree after destroy beginning).
     *
     * @param c Visitor closure. Visits only leaf pages.
     * @return Number of pages recycled from this tree. If the tree was destroyed by someone else concurrently returns
     *     {@code 0}, otherwise it should return at least {@code 2} (for meta page and root page), unless this tree is
     *     used as metadata storage, or {@code -1} if we don't have a reuse list and did not do recycling at all.
     * @throws IgniteCheckedException If failed.
     */
    public final long destroy(IgniteInClosure<L> c) throws IgniteCheckedException {
        if (!markDestroyed())
            return 0;

        if (reuseList == null)
            return -1;

        DestroyBag bag = new DestroyBag();

        long pagesCnt = 0;

        long metaPage = acquirePage(metaPageId);
        try {
            long metaPageAddr = writeLock(metaPageId, metaPage); // No checks, we must be out of use.

            assert metaPageAddr != 0L;

            try {
                for (long pageId : getFirstPageIds(metaPageAddr)) {
                    assert pageId != 0;

                    do {
                        long page = acquirePage(pageId);
                        try {
                            long pageAddr = writeLock(pageId, page); // No checks, we must be out of use.

                            try {
                                BPlusIO<L> io = io(pageAddr);

                                if (c != null && io.isLeaf())
                                    io.visit(pageAddr, c);

                                long fwdPageId = io.getForward(pageAddr);

                                bag.addFreePage(recyclePage(pageId, page, pageAddr, null));
                                pagesCnt++;

                                pageId = fwdPageId;
                            }
                            finally {
                                writeUnlock(pageId, page, pageAddr, true);
                            }
                        }
                        finally {
                            releasePage(pageId, page);
                        }

                        if (bag.size() == 128) {
                            reuseList.addForRecycle(bag);

                            assert bag.isEmpty() : bag.size();
                        }
                    }
                    while (pageId != 0);
                }

                bag.addFreePage(recyclePage(metaPageId, metaPage, metaPageAddr, null));
                pagesCnt++;
            }
            finally {
                writeUnlock(metaPageId, metaPage, metaPageAddr, true);
            }
        }
        finally {
            releasePage(metaPageId, metaPage);
        }

        reuseList.addForRecycle(bag);

        assert bag.size() == 0 : bag.size();

        return pagesCnt;
    }

    /**
     * @return {@code True} if state was changed.
     */
    private boolean markDestroyed() {
        return destroyed.compareAndSet(false, true);
    }

    /**
     * @param pageAddr Meta page address.
     * @return First page IDs.
     */
    protected Iterable<Long> getFirstPageIds(long pageAddr) {
        List<Long> res = new ArrayList<>();

        BPlusMetaIO mio = BPlusMetaIO.VERSIONS.forPage(pageAddr);

        for (int lvl = mio.getRootLevel(pageAddr); lvl >= 0; lvl--)
            res.add(mio.getFirstPageId(pageAddr, lvl));

        return res;
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address
     * @param io IO.
     * @param fwdId Forward page ID.
     * @param fwdBuf Forward buffer.
     * @param idx Insertion index.
     * @return {@code true} The middle index was shifted to the right.
     * @throws IgniteCheckedException If failed.
     */
    private boolean splitPage(
        long pageId, long page, long pageAddr, BPlusIO io, long fwdId, long fwdBuf, int idx
    ) throws IgniteCheckedException {
        int cnt = io.getCount(pageAddr);
        int mid = cnt >>> 1;

        boolean res = false;

        if (idx > mid) { // If insertion is going to be to the forward page, keep more in the back page.
            mid++;

            res = true;
        }

        // Update forward page.
        io.splitForwardPage(pageAddr, fwdId, fwdBuf, mid, cnt, pageSize());

        // Update existing page.
        io.splitExistingPage(pageAddr, mid, fwdId);

        if (needWalDeltaRecord(pageId, page, null))
            wal.log(new SplitExistingPageRecord(cacheId, pageId, mid, fwdId));

        return res;
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address
     * @param walPlc Full page WAL record policy.
     */
    private void writeUnlockAndClose(long pageId, long page, long pageAddr, Boolean walPlc) {
        try {
            writeUnlock(pageId, page, pageAddr, walPlc, true);
        }
        finally {
            releasePage(pageId, page);
        }
    }

    /**
     * @param pageId Inner page ID.
     * @param g Get.
     * @param back Get back (if {@code true}) or forward page (if {@code false}).
     * @return Operation result.
     * @throws IgniteCheckedException If failed.
     */
    private Result askNeighbor(long pageId, Get g, boolean back) throws IgniteCheckedException {
        return read(pageId, askNeighbor, g, back ? TRUE.ordinal() : FALSE.ordinal(), RETRY);
    }

    /**
     * @param p Put.
     * @param pageId Page ID.
     * @param fwdId Expected forward page ID.
     * @param lvl Level.
     * @return Result code.
     * @throws IgniteCheckedException If failed.
     */
    private Result putDown(final Put p, final long pageId, final long fwdId, final int lvl)
        throws IgniteCheckedException {
        assert lvl >= 0 : lvl;

        final long page = acquirePage(pageId);

        try {
            for (;;) {
                // Init args.
                p.pageId = pageId;
                p.fwdId = fwdId;

                Result res = read(pageId, page, search, p, lvl, RETRY);

                switch (res) {
                    case GO_DOWN:
                    case GO_DOWN_X:
                        assert lvl > 0 : lvl;
                        assert p.pageId != pageId;
                        assert p.fwdId != fwdId || fwdId == 0;

                        res = p.tryReplaceInner(pageId, page, fwdId, lvl);

                        if (res != RETRY) // Go down recursively.
                            res = putDown(p, p.pageId, p.fwdId, lvl - 1);

                        if (res == RETRY_ROOT || p.isFinished())
                            return res;

                        if (res == RETRY)
                            checkInterrupted();

                        continue; // We have to insert split row to this level or it is a retry.

                    case FOUND: // Do replace.
                        assert lvl == 0 : "This replace can happen only at the bottom level.";

                        return p.tryReplace(pageId, page, fwdId, lvl);

                    case NOT_FOUND: // Do insert.
                        assert lvl == p.btmLvl : "must insert at the bottom level";
                        assert p.needReplaceInner == FALSE : p.needReplaceInner + " " + lvl;

                        return p.tryInsert(pageId, page, fwdId, lvl);

                    default:
                        return res;
                }
            }
        }
        finally {
            if (p.canRelease(pageId, lvl))
                releasePage(pageId, page);
        }
    }

    /**
     * @param io IO.
     * @param pageAddr Page address.
     * @param back Backward page.
     * @return Page ID.
     */
    private long doAskNeighbor(BPlusIO<L> io, long pageAddr, boolean back) {
        long res;

        if (back) {
            // Count can be 0 here if it is a routing page, in this case we have a single child.
            int cnt = io.getCount(pageAddr);

            // We need to do get the rightmost child: io.getRight(cnt - 1),
            // here io.getLeft(cnt) is the same, but handles negative index if count is 0.
            res = inner(io).getLeft(pageAddr, cnt);
        }
        else // Leftmost child.
            res = inner(io).getLeft(pageAddr, 0);

        assert res != 0 : "inner page with no route down: " + U.hexLong(PageIO.getPageId(pageAddr));

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BPlusTree.class, this);
    }

    /**
     * Get operation.
     */
    private abstract class Get {
        /** */
        long rmvId;

        /** Starting point root level. May be outdated. Must be modified only in {@link Get#init()}. */
        int rootLvl;

        /** Starting point root ID. May be outdated. Must be modified only in {@link Get#init()}. */
        long rootId;

        /** */
        L row;

        /** In/Out parameter: Page ID. */
        long pageId;

        /** In/Out parameter: expected forward page ID. */
        long fwdId;

        /** In/Out parameter: in case of right turn this field will contain backward page ID for the child. */
        long backId;

        /** */
        int shift;

        /** If this operation is a part of invoke. */
        Invoke invoke;

        /** Ignore row passed, find last row */
        boolean findLast;

        /**
         * @param row Row.
         * @param findLast find last row.
         */
        Get(L row, boolean findLast) {
            assert findLast ^ row != null;

            this.row = row;
            this.findLast = findLast;
        }

        /**
         * @param g Other operation to copy from.
         */
        final void copyFrom(Get g) {
            rmvId = g.rmvId;
            rootLvl = g.rootLvl;
            pageId = g.pageId;
            fwdId = g.fwdId;
            backId = g.backId;
            shift = g.shift;
            findLast = g.findLast;
        }

        /**
         * Initialize operation.
         *
         * @throws IgniteCheckedException If failed.
         */
        final void init() throws IgniteCheckedException {
            TreeMetaData meta0 = treeMeta();

            assert meta0 != null;

            restartFromRoot(meta0.rootId, meta0.rootLvl, globalRmvId.get());
        }

        /**
         * @param rootId Root page ID.
         * @param rootLvl Root level.
         * @param rmvId Remove ID to be afraid of.
         */
        void restartFromRoot(long rootId, int rootLvl, long rmvId) {
            this.rootId = rootId;
            this.rootLvl = rootLvl;
            this.rmvId = rmvId;
        }

        /**
         * @param io IO.
         * @param pageAddr Page address.
         * @param idx Index of found entry.
         * @param lvl Level.
         * @return {@code true} If we need to stop.
         * @throws IgniteCheckedException If failed.
         */
        boolean found(BPlusIO<L> io, long pageAddr, int idx, int lvl) throws IgniteCheckedException {
            assert lvl >= 0;

            return lvl == 0; // Stop if we are at the bottom.
        }

        /**
         * @param io IO.
         * @param pageAddr Page address.
         * @param idx Insertion point.
         * @param lvl Level.
         * @return {@code true} If we need to stop.
         * @throws IgniteCheckedException If failed.
         */
        boolean notFound(BPlusIO<L> io, long pageAddr, int idx, int lvl) throws IgniteCheckedException {
            assert lvl >= 0;

            return lvl == 0; // Stop if we are at the bottom.
        }

        /**
         * @param pageId Page.
         * @param lvl Level.
         * @return {@code true} If we can release the given page.
         */
        boolean canRelease(long pageId, int lvl) {
            return pageId != 0L;
        }

        /**
         * @param backId Back page ID.
         */
        void backId(long backId) {
            this.backId = backId;
        }

        /**
         * @param pageId Page ID.
         */
        void pageId(long pageId) {
            this.pageId = pageId;
        }

        /**
         * @param fwdId Forward page ID.
         */
        void fwdId(long fwdId) {
            this.fwdId = fwdId;
        }

        /**
         * @return {@code true} If the operation is finished.
         */
        boolean isFinished() {
            throw new IllegalStateException();
        }
    }

    /**
     * Get a single entry.
     */
    private final class GetOne extends Get {
        /** */
        Object x;

        /**
         * @param row Row.
         * @param x Implementation specific argument.
         * @param findLast Ignore row passed, find last row
         */
        private GetOne(L row, Object x, boolean findLast) {
            super(row, findLast);

            this.x = x;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override boolean found(BPlusIO<L> io, long pageAddr, int idx, int lvl) throws IgniteCheckedException {
            // Check if we are on an inner page and can't get row from it.
            if (lvl != 0 && !canGetRowFromInner)
                return false;

            row = getRow(io, pageAddr, idx, x);

            return true;
        }
    }

    /**
     * Get a cursor for range.
     */
    private final class GetCursor extends Get {
        /** */
        ForwardCursor cursor;

        /**
         * @param lower Lower bound.
         * @param shift Shift.
         * @param cursor Cursor.
         */
        GetCursor(L lower, int shift, ForwardCursor cursor) {
            super(lower, false);

            assert shift != 0; // Either handle range of equal rows or find a greater row after concurrent merge.

            this.shift = shift;
            this.cursor = cursor;
        }

        /** {@inheritDoc} */
        @Override boolean found(BPlusIO<L> io, long pageAddr, int idx, int lvl) throws IgniteCheckedException {
            throw new IllegalStateException(); // Must never be called because we always have a shift.
        }

        /** {@inheritDoc} */
        @Override boolean notFound(BPlusIO<L> io, long pageAddr, int idx, int lvl) throws IgniteCheckedException {
            if (lvl != 0)
                return false;

            cursor.init(pageAddr, io, idx);

            return true;
        }
    }

    /**
     * Put operation.
     */
    private final class Put extends Get {
        /** Right child page ID for split row. */
        long rightId;

        /** Replaced row if any. */
        T oldRow;

        /**
         * This page is kept locked after split until insert to the upper level will not be finished.
         * It is needed because split row will be "in flight" and if we'll release tail, remove on
         * split row may fail.
         */
        long tailId;

        /** */
        long tailPage;

        /** */
        long tailAddr;

        /**
         * Bottom level for insertion (insert can't go deeper). Will be incremented on split on each level.
         */
        short btmLvl;

        /** */
        Bool needReplaceInner = FALSE;

        /** */
        final boolean needOld;

        /**
         * @param row Row.
         * @param needOld {@code True} If need return old value.
         */
        private Put(T row, boolean needOld) {
            super(row, false);

            this.needOld = needOld;
        }

        /** {@inheritDoc} */
        @Override boolean found(BPlusIO<L> io, long pageAddr, int idx, int lvl) {
            if (lvl == 0) // Leaf: need to stop.
                return true;

            assert btmLvl == 0; // It can not be insert.

            // If we can get full row from the inner page, we have to replace it with the new one. On the way down
            // we can not miss inner key even in presence of concurrent operations because of `triangle` invariant +
            // concurrent inner replace handling by retrying from root.
            if (canGetRowFromInner && needReplaceInner == FALSE)
                needReplaceInner = TRUE;

            return false;
        }

        /** {@inheritDoc} */
        @Override boolean notFound(BPlusIO<L> io, long pageAddr, int idx, int lvl) {
            assert btmLvl >= 0 : btmLvl;
            assert lvl >= btmLvl : lvl;

            return lvl == btmLvl;
        }

        /**
         * @param tailId Tail page ID.
         * @param tailPage Tail page pointer.
         * @param tailPageAddr Tail page address
         */
        private void tail(long tailId, long tailPage, long tailPageAddr) {
            assert (tailId == 0L) == (tailPage == 0L);
            assert (tailPage == 0L) == (tailPageAddr == 0L);

            if (this.tailPage != 0L)
                writeUnlockAndClose(this.tailId, this.tailPage, this.tailAddr, null);

            this.tailId = tailId;
            this.tailPage = tailPage;
            this.tailAddr = tailPageAddr;
        }

        /** {@inheritDoc} */
        @Override boolean canRelease(long pageId, int lvl) {
            return pageId != 0L && tailId != pageId;
        }

        /**
         * Finish put.
         */
        private void finish() {
            row = null;
            rightId = 0;

            tail(0L, 0L, 0L);
        }

        /** {@inheritDoc} */
        @Override boolean isFinished() {
            return row == null;
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param io IO.
         * @param idx Index.
         * @param lvl Level.
         * @return Move up row.
         * @throws IgniteCheckedException If failed.
         */
        private L insert(long pageId, long page, long pageAddr, BPlusIO<L> io, int idx, int lvl)
            throws IgniteCheckedException {
            int maxCnt = io.getMaxCount(pageAddr, pageSize());
            int cnt = io.getCount(pageAddr);

            if (cnt == maxCnt) // Need to split page.
                return insertWithSplit(pageId, page, pageAddr, io, idx, lvl);

            insertSimple(pageId, page, pageAddr, io, idx, null);

            return null;
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param io IO.
         * @param idx Index.
         * @param walPlc Full page WAL record policy.
         * @throws IgniteCheckedException If failed.
         */
        private void insertSimple(long pageId, long page, long pageAddr, BPlusIO<L> io, int idx, Boolean walPlc)
            throws IgniteCheckedException {
            boolean needWal = needWalDeltaRecord(pageId, page, walPlc);

            byte[] rowBytes = io.insert(pageAddr, idx, row, null, rightId, needWal);

            if (needWal)
                wal.log(new InsertRecord<>(cacheId, pageId, io, idx, rowBytes, rightId));
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param io IO.
         * @param idx Index.
         * @param lvl Level.
         * @return Move up row.
         * @throws IgniteCheckedException If failed.
         */
        private L insertWithSplit(long pageId, long page, long pageAddr, BPlusIO<L> io, int idx, int lvl)
            throws IgniteCheckedException {
            long fwdId = allocatePage(null);
            long fwdPage = acquirePage(fwdId);

            try {
                // Need to check this before the actual split, because after the split we will have new forward page here.
                boolean hadFwd = io.getForward(pageAddr) != 0;

                long fwdPageAddr = writeLock(fwdId, fwdPage); // Initial write, no need to check for concurrent modification.

                assert fwdPageAddr != 0L;

                // TODO GG-11640 log a correct forward page record.
                final Boolean fwdPageWalPlc = Boolean.TRUE;

                try {
                    boolean midShift = splitPage(pageId, page, pageAddr, io, fwdId, fwdPageAddr, idx);

                    // Do insert.
                    int cnt = io.getCount(pageAddr);

                    if (idx < cnt || (idx == cnt && !midShift)) { // Insert into back page.
                        insertSimple(pageId, page, pageAddr, io, idx, null);

                        // Fix leftmost child of forward page, because newly inserted row will go up.
                        if (idx == cnt && !io.isLeaf()) {
                            inner(io).setLeft(fwdPageAddr, 0, rightId);

                            if (needWalDeltaRecord(fwdId, fwdPage, fwdPageWalPlc)) // Rare case, we can afford separate WAL record to avoid complexity.
                                wal.log(new FixLeftmostChildRecord(cacheId, fwdId, rightId));
                        }
                    }
                    else // Insert into newly allocated forward page.
                        insertSimple(fwdId, fwdPage, fwdPageAddr, io, idx - cnt, fwdPageWalPlc);

                    // Do move up.
                    cnt = io.getCount(pageAddr);

                    // Last item from backward row goes up.
                    L moveUpRow = io.getLookupRow(BPlusTree.this, pageAddr, cnt - 1);

                    if (!io.isLeaf()) { // Leaf pages must contain all the links, inner pages remove moveUpLink.
                        io.setCount(pageAddr, cnt - 1);

                        if (needWalDeltaRecord(pageId, page, null)) // Rare case, we can afford separate WAL record to avoid complexity.
                            wal.log(new FixCountRecord(cacheId, pageId, cnt - 1));
                    }

                    if (!hadFwd && lvl == getRootLevel()) { // We are splitting root.
                        long newRootId = allocatePage(null);
                        long newRootPage = acquirePage(newRootId);

                        try {
                            if (io.isLeaf())
                                io = latestInnerIO();

                            long newRootAddr = writeLock(newRootId, newRootPage); // Initial write.

                            assert newRootAddr != 0L;

                            // Never write full new root page, because it is known to be new.
                            final Boolean newRootPageWalPlc = Boolean.FALSE;

                            try {
                                boolean needWal = needWalDeltaRecord(newRootId, newRootPage, newRootPageWalPlc);

                                byte[] moveUpRowBytes = inner(io).initNewRoot(newRootAddr,
                                    newRootId,
                                    pageId,
                                    moveUpRow,
                                    null,
                                    fwdId,
                                    pageSize(),
                                    needWal);

                                if (needWal)
                                    wal.log(new NewRootInitRecord<>(cacheId, newRootId, newRootId,
                                        inner(io), pageId, moveUpRowBytes, fwdId));
                            }
                            finally {
                                writeUnlock(newRootId, newRootPage, newRootAddr, newRootPageWalPlc, true);
                            }
                        }
                        finally {
                            releasePage(newRootId, newRootPage);
                        }

                        Bool res = write(metaPageId, addRoot, newRootId, lvl + 1, FALSE);

                        assert res == TRUE : res;

                        return null; // We've just moved link up to root, nothing to return here.
                    }

                    // Regular split.
                    return moveUpRow;
                }
                finally {
                    writeUnlock(fwdId, fwdPage, fwdPageAddr, fwdPageWalPlc, true);
                }
            }
            finally {
                releasePage(fwdId, fwdPage);
            }
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteCheckedException If failed.
         */
        private Result tryReplaceInner(long pageId, long page, long fwdId, int lvl)
            throws IgniteCheckedException {
            // Need to replace key in inner page. There is no race because we keep tail lock after split.
            if (needReplaceInner == TRUE) {
                needReplaceInner = FALSE; // Protect from retries.

                long oldFwdId = this.fwdId;
                long oldPageId = this.pageId;

                // Set old args.
                this.fwdId = fwdId;
                this.pageId = pageId;

                Result res = write(pageId, page, replace, this, lvl, RETRY);

                // Restore args.
                this.pageId = oldPageId;
                this.fwdId = oldFwdId;

                if (res == RETRY)
                    return RETRY;

                needReplaceInner = DONE; // We can have only a single matching inner key.

                return FOUND;
            }

            return NOT_FOUND;
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteCheckedException If failed.
         */
        private Result tryInsert(long pageId, long page, long fwdId, int lvl) throws IgniteCheckedException {
            // Init args.
            this.pageId = pageId;
            this.fwdId = fwdId;

            return write(pageId, page, insert, this, lvl, RETRY);
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteCheckedException If failed.
         */
        public Result tryReplace(long pageId, long page, long fwdId, int lvl) throws IgniteCheckedException {
            // Init args.
            this.pageId = pageId;
            this.fwdId = fwdId;

            return write(pageId, page, replace, this, lvl, RETRY);
        }
    }

    /**
     * Invoke operation.
     */
    private final class Invoke extends Get {
        /** */
        Object x;

        /** */
        InvokeClosure<T> clo;

        /** */
        Bool closureInvoked = FALSE;

        /** */
        T foundRow;

        /** */
        Get op;

        /**
         * @param row Row.
         * @param x Implementation specific argument.
         * @param clo Closure.
         */
        private Invoke(L row, Object x, final InvokeClosure<T> clo) {
            super(row, false);

            assert clo != null;

            this.clo = clo;
            this.x = x;
        }

        /** {@inheritDoc} */
        @Override void pageId(long pageId) {
            this.pageId = pageId;

            if (op != null)
                op.pageId = pageId;
        }

        /** {@inheritDoc} */
        @Override void fwdId(long fwdId) {
            this.fwdId = fwdId;

            if (op != null)
                op.fwdId = fwdId;
        }

        /** {@inheritDoc} */
        @Override void backId(long backId) {
            this.backId = backId;

            if (op != null)
                op.backId = backId;
        }

        /** {@inheritDoc} */
        @Override void restartFromRoot(long rootId, int rootLvl, long rmvId) {
            super.restartFromRoot(rootId, rootLvl, rmvId);

            if (op != null)
                op.restartFromRoot(rootId, rootLvl, rmvId);
        }

        /** {@inheritDoc} */
        @Override boolean found(BPlusIO<L> io, long pageAddr, int idx, int lvl) throws IgniteCheckedException {
            // If the operation is initialized, then the closure has been called already.
            if (op != null)
                return op.found(io, pageAddr, idx, lvl);

            if (lvl == 0) {
                if (closureInvoked == FALSE) {
                    closureInvoked = READY;

                    foundRow = getRow(io, pageAddr, idx, x);
                }

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override boolean notFound(BPlusIO<L> io, long pageAddr, int idx, int lvl) throws IgniteCheckedException {
            // If the operation is initialized, then the closure has been called already.
            if (op != null)
                return op.notFound(io, pageAddr, idx, lvl);

            if (lvl == 0) {
                if (closureInvoked == FALSE)
                    closureInvoked = READY;

                return true;
            }

            return false;
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void invokeClosure() throws IgniteCheckedException {
            if (closureInvoked != READY)
                return;

            closureInvoked = DONE;

            clo.call(foundRow);

            switch (clo.operationType()) {
                case PUT:
                    T newRow = clo.newRow();

                    assert newRow != null;

                    op = new Put(newRow, false);

                    break;

                case REMOVE:
                    assert foundRow != null;

                    op = new Remove(row, false);

                    break;

                case NOOP:
                    return;

                default:
                    throw new IllegalStateException();
            }

            op.copyFrom(this);

            op.invoke = this;
        }

        /** {@inheritDoc} */
        @Override boolean canRelease(long pageId, int lvl) {
            if (pageId == 0L)
                return false;

            if (op == null)
                return true;

            return op.canRelease(pageId, lvl);
        }

        /**
         * @return {@code true} If it is a {@link Put} operation internally.
         */
        private boolean isPut() {
            return op != null && op.getClass() == Put.class;
        }

        /**
         * @return {@code true} If it is a {@link Remove} operation internally.
         */
        private boolean isRemove() {
            return op != null && op.getClass() == Remove.class;
        }

        /**
         * @param pageId Page ID.
         * @param lvl Level.
         * @return {@code true} If it is a {@link Remove} and the page is in tail.
         */
        private boolean isTail(long pageId, int lvl) {
            return isRemove() && ((Remove)op).isTail(pageId, lvl);
        }

        /**
         */
        private void levelExit() {
            if (isRemove())
                ((Remove)op).page = 0L;
        }

        /**
         * Release all the resources by the end of operation.
         * @throws IgniteCheckedException if failed.
         */
        private void releaseAll() throws IgniteCheckedException {
            if (isRemove())
                ((Remove)op).releaseAll();
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteCheckedException If failed.
         */
        private Result onNotFound(long pageId, long page, long fwdId, int lvl)
            throws IgniteCheckedException {
            if (op == null)
                return NOT_FOUND;

            if (isRemove()) {
                assert lvl == 0;

                ((Remove)op).finish();

                return NOT_FOUND;
            }

            return ((Put)op).tryInsert(pageId, page, fwdId, lvl);
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param backId Back page ID.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteCheckedException If failed.
         */
        private Result onFound(long pageId, long page, long backId, long fwdId, int lvl)
            throws IgniteCheckedException {
            if (op == null)
                return FOUND;

            if (isRemove())
                return ((Remove)op).tryRemoveFromLeaf(pageId, page, backId, fwdId, lvl);

            return  ((Put)op).tryReplace(pageId, page, fwdId, lvl);
        }

        /**
         * @return Result.
         * @throws IgniteCheckedException If failed.
         */
        private Result tryFinish() throws IgniteCheckedException {
            assert op != null; // Must be guarded by isFinished.

            if (isPut())
                return RETRY;

            Result res = ((Remove)op).finishTail();

            if (res == NOT_FOUND)
                res = RETRY;

            assert res == FOUND || res == RETRY: res;

            return res;
        }

        /** {@inheritDoc} */
        @Override boolean isFinished() {
            if (closureInvoked != DONE)
                return false;

            if (op == null)
                return true;

            return op.isFinished();
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteCheckedException If failed.
         */
        Result tryReplaceInner(long pageId, long page, long fwdId, int lvl) throws IgniteCheckedException {
            if (!isPut())
                return NOT_FOUND;

            return ((Put)op).tryReplaceInner(pageId, page, fwdId, lvl);
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param backId Back page ID.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteCheckedException If failed.
         */
        public Result finishOrLockTail(long pageId, long page, long backId, long fwdId, int lvl)
            throws IgniteCheckedException {
            return ((Remove)op).finishOrLockTail(pageId, page, backId, fwdId, lvl);
        }
    }

    /**
     * Remove operation.
     */
    private final class Remove extends Get implements ReuseBag {
        /** We may need to lock part of the tree branch from the bottom to up for multiple levels. */
        Tail<L> tail;

        /** */
        Bool needReplaceInner = FALSE;

        /** */
        Bool needMergeEmptyBranch = FALSE;

        /** Removed row. */
        T rmvd;

        /** Current page absolute pointer. */
        long page;

        /** */
        Object freePages;

        /** */
        final boolean needOld;

        /**
         * @param row Row.
         * @param needOld {@code True} If need return old value.
         */
        private Remove(L row, boolean needOld) {
            super(row, false);

            this.needOld = needOld;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public long pollFreePage() {
            if (freePages == null)
                return 0L;

            if (freePages.getClass() == GridLongList.class) {
                GridLongList list = ((GridLongList)freePages);

                return list.isEmpty() ? 0L : list.remove();
            }

            long res = (long)freePages;

            freePages = null;

            return res;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void addFreePage(long pageId) {
            assert pageId != 0L;

            if (freePages == null)
                freePages = pageId;
            else {
                GridLongList list;

                if (freePages.getClass() == GridLongList.class)
                    list = (GridLongList)freePages;
                else {
                    list = new GridLongList(4);

                    list.add((Long)freePages);
                    freePages = list;
                }

                list.add(pageId);
            }
        }

        /** {@inheritDoc} */
        @Override boolean notFound(BPlusIO<L> io, long pageAddr, int idx, int lvl) {
            if (lvl == 0) {
                assert tail == null;

                return true;
            }

            return false;
        }

        /**
         * Finish the operation.
         */
        private void finish() {
            assert tail == null;

            row = null;
        }

        /**
         * @throws IgniteCheckedException If failed.
         * @return Tail to release if an empty branch was not merged.
         */
        private Tail<L> mergeEmptyBranch() throws IgniteCheckedException {
            assert needMergeEmptyBranch == TRUE: needMergeEmptyBranch;

            Tail<L> t = tail;

            // Find empty branch beginning.
            for (Tail<L> t0 = t.down; t0.lvl != 0; t0 = t0.down) {
                assert t0.type == Tail.EXACT : t0.type;

                if (t0.getCount() != 0)
                    t = t0; // Here we correctly handle empty windows in the middle of branch.
            }

            int cnt = t.getCount();
            int idx = fix(insertionPoint(t));

            assert cnt > 0: cnt;

            if (idx == cnt)
                idx--;

            // If at the join point we will not be able to merge, we need to retry.
            if (!checkChildren(t, t.getLeftChild(), t.getRightChild(), idx))
                return t;

            // Make the branch really empty before the merge.
            removeDataRowFromLeafTail(t);

            while (t.lvl != 0) { // If we've found empty branch, merge it top-down.
                boolean res = merge(t);

                // All the merges must succeed because we have only empty pages from one side.
                assert res : needMergeEmptyBranch + "\n" + printTail(true);

                if (needMergeEmptyBranch == TRUE)
                    needMergeEmptyBranch = READY; // Need to mark that we've already done the first (top) merge.

                t = t.down;
            }

            return null; // Succeeded.
        }

        /**
         * @param t Tail.
         * @throws IgniteCheckedException If failed.
         */
        private void mergeBottomUp(Tail<L> t) throws IgniteCheckedException {
            assert needMergeEmptyBranch == FALSE || needMergeEmptyBranch == DONE : needMergeEmptyBranch;

            if (t.down == null || t.down.sibling == null) {
                // Do remove if we did not yet.
                if (t.lvl == 0 && !isRemoved())
                    removeDataRowFromLeafTail(t);

                return; // Nothing to merge.
            }

            mergeBottomUp(t.down);
            merge(t);
        }

        /**
         * @return {@code true} If found.
         * @throws IgniteCheckedException If failed.
         */
        private boolean isInnerKeyInTail() throws IgniteCheckedException {
            assert tail.lvl > 0: tail.lvl;

            return insertionPoint(tail) >= 0;
        }

        /**
         * @return {@code true} If already removed from leaf.
         */
        private boolean isRemoved() {
            return rmvd != null;
        }

        /**
         * @param t Tail to release.
         * @return {@code true} If we need to retry or {@code false} to exit.
         */
        private boolean releaseForRetry(Tail<L> t) {
            // Try to simply release all first.
            if (t.lvl <= 1) {
                // We've just locked leaf and did not do the remove, can safely release all and retry.
                assert !isRemoved(): "removed";

                // These fields will be setup again on remove from leaf.
                needReplaceInner = FALSE;
                needMergeEmptyBranch = FALSE;

                releaseTail();

                return true;
            }

            // Release all up to the given tail with its direct children.
            if (t.down != null) {
                Tail<L> newTail = t.down.down;

                if (newTail != null) {
                    t.down.down = null;

                    releaseTail();

                    tail = newTail;

                    return true;
                }
            }

            // Here we wanted to do a regular merge after all the important operations,
            // so we can leave this invalid tail as is. We have no other choice here
            // because our tail is not long enough for retry. Exiting.
            assert isRemoved();
            assert needReplaceInner != TRUE && needMergeEmptyBranch != TRUE;

            return false;
        }

        /**
         * Process tail and finish.
         *
         * @return Result.
         * @throws IgniteCheckedException If failed.
         */
        private Result finishTail() throws IgniteCheckedException {
            assert !isFinished();
            assert tail.type == Tail.EXACT && tail.lvl >= 0: tail;

            if (tail.lvl == 0) {
                // At the bottom level we can't have a tail without a sibling, it means we have higher levels.
                assert tail.sibling != null : tail;

                return NOT_FOUND; // Lock upper level, we are at the bottom now.
            }
            else {
                // We may lock wrong triangle because of concurrent operations.
                if (!validateTail()) {
                    if (releaseForRetry(tail))
                        return RETRY;

                    // It was a regular merge, leave as is and exit.
                }
                else {
                    // Try to find inner key on inner level.
                    if (needReplaceInner == TRUE) {
                        // Since we setup needReplaceInner in leaf page write lock and do not release it,
                        // we should not be able to miss the inner key. Even if concurrent merge
                        // happened the inner key must still exist.
                        if (!isInnerKeyInTail())
                            return NOT_FOUND; // Lock the whole branch up to the inner key.

                        needReplaceInner = READY;
                    }

                    // Try to merge an empty branch.
                    if (needMergeEmptyBranch == TRUE) {
                        // We can't merge empty branch if tail is a routing page.
                        if (tail.getCount() == 0)
                            return NOT_FOUND; // Lock the whole branch up to the first non-empty.

                        // Top-down merge for empty branch. The actual row remove will happen here if everything is ok.
                        Tail<L> t = mergeEmptyBranch();

                        if (t != null) {
                            // We were not able to merge empty branch, need to release and retry.
                            boolean ok = releaseForRetry(t);

                            assert ok; // Here we must always retry because it is not a regular merge.

                            return RETRY;
                        }

                        needMergeEmptyBranch = DONE;
                    }

                    // The actual row remove may happen here as well.
                    mergeBottomUp(tail);

                    if (needReplaceInner == READY) {
                        replaceInner(); // Replace inner key with new max key for the left subtree.

                        needReplaceInner = DONE;
                    }

                    assert needReplaceInner != TRUE;

                    if (tail.getCount() == 0 && tail.lvl != 0 && getRootLevel() == tail.lvl) {
                        // Free root if it became empty after merge.
                        cutRoot(tail.lvl);
                        freePage(tail.pageId, tail.page, tail.buf, tail.walPlc, false);

                        // Exit: we are done.
                    }
                    else if (tail.sibling != null &&
                        tail.getCount() + tail.sibling.getCount() < tail.io.getMaxCount(tail.buf, pageSize())) {
                        // Release everything lower than tail, we've already merged this path.
                        doReleaseTail(tail.down);
                        tail.down = null;

                        return NOT_FOUND; // Lock and merge one level more.
                    }

                    // We don't want to merge anything more, exiting.
                }
            }

            // If we've found nothing in the tree, we should not do any modifications or take tail locks.
            assert isRemoved();

            releaseTail();
            finish();

            return FOUND;
        }

        /**
         * @param t Tail.
         * @throws IgniteCheckedException If failed.
         */
        private void removeDataRowFromLeafTail(Tail<L> t) throws IgniteCheckedException {
            assert !isRemoved();

            Tail<L> leaf = getTail(t, 0);

            removeDataRowFromLeaf(leaf.pageId, leaf.page, leaf.buf, leaf.walPlc, leaf.io, leaf.getCount(), insertionPoint(leaf));
        }

        /**
         * @param leafId Leaf page ID.
         * @param leafPage Leaf page pointer.
         * @param backId Back page ID.
         * @param fwdId Forward ID.
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        private Result removeFromLeaf(long leafId, long leafPage, long backId, long fwdId) throws IgniteCheckedException {
            // Init parameters.
            pageId = leafId;
            page = leafPage;
            this.backId = backId;
            this.fwdId = fwdId;

            // Usually this will be true, so in most cases we should not lock any extra pages.
            if (backId == 0)
                return doRemoveFromLeaf();

            // Lock back page before the remove, we'll need it for merges.
            long backPage = acquirePage(backId);

            try {
                return write(backId, backPage, lockBackAndRmvFromLeaf, this, 0, RETRY);
            }
            finally {
                if (canRelease(backId, 0))
                    releasePage(backId, backPage);
            }
        }

        /**
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        private Result doRemoveFromLeaf() throws IgniteCheckedException {
            assert page != 0L;

            return write(pageId, page, rmvFromLeaf, this, 0, RETRY);
        }

        /**
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        private Result doLockTail(int lvl) throws IgniteCheckedException {
            assert page != 0L;

            return write(pageId, page, lockTail, this, lvl, RETRY);
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param backId Back page ID.
         * @param fwdId Expected forward page ID.
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        private Result lockTail(long pageId, long page, long backId, long fwdId, int lvl)
            throws IgniteCheckedException {
            assert tail != null;

            // Init parameters for the handlers.
            this.pageId = pageId;
            this.page = page;
            this.fwdId = fwdId;
            this.backId = backId;

            if (backId == 0) // Back page ID is provided only when the last move was to the right.
                return doLockTail(lvl);

            long backPage = acquirePage(backId);

            try {
                return write(backId, backPage, lockBackAndTail, this, lvl, RETRY);
            }
            finally {
                if (canRelease(backId, lvl))
                    releasePage(backId, backPage);
            }
        }

        /**
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        private Result lockForward(int lvl) throws IgniteCheckedException {
            assert fwdId != 0: fwdId;
            assert backId == 0: backId;

            long fwdId = this.fwdId;
            long fwdPage = acquirePage(fwdId);

            try {
                return write(fwdId, fwdPage, lockTailForward, this, lvl, RETRY);
            }
            finally {
                // If we were not able to lock forward page as tail, release the page.
                if (canRelease(fwdId, lvl))
                    releasePage(fwdId, fwdPage);
            }
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param walPlc Full page WAL record policy.
         * @param io IO.
         * @param cnt Count.
         * @param idx Index to remove.
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("unchecked")
        private void removeDataRowFromLeaf(long pageId, long page, long pageAddr, Boolean walPlc, BPlusIO<L> io, int cnt,
            int idx)
            throws IgniteCheckedException {
            assert idx >= 0 && idx < cnt: idx;
            assert io.isLeaf(): "inner";
            assert !isRemoved(): "already removed";

            // Detach the row.
            rmvd = needOld ? getRow(io, pageAddr, idx) : (T)Boolean.TRUE;

            doRemove(pageId, page, pageAddr, walPlc, io, cnt, idx);

            assert isRemoved();
        }

        /**
         *
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param walPlc Full page WAL record policy.
         * @param io IO.
         * @param cnt Count.
         * @param idx Index to remove.
         * @throws IgniteCheckedException If failed.
         */
        private void doRemove(long pageId, long page, long pageAddr, Boolean walPlc, BPlusIO<L> io, int cnt,
            int idx)
            throws IgniteCheckedException {
            assert cnt > 0 : cnt;
            assert idx >= 0 && idx < cnt : idx + " " + cnt;

            io.remove(pageAddr, idx, cnt);

            if (needWalDeltaRecord(pageId, page, walPlc))
                wal.log(new RemoveRecord(cacheId, pageId, idx, cnt));
        }

        /**
         * @param tail Tail.
         * @return Insertion point. May be negative.
         * @throws IgniteCheckedException If failed.
         */
        private int insertionPoint(Tail<L> tail) throws IgniteCheckedException {
            assert tail.type == Tail.EXACT: tail.type;

            if (tail.idx == Short.MIN_VALUE) {
                int idx = findInsertionPoint(tail.lvl, tail.io, tail.buf, 0, tail.getCount(), row, 0);

                assert checkIndex(idx): idx;

                tail.idx = (short)idx;
            }

            return tail.idx;
        }

        /**
         * @return {@code true} If the currently locked tail is valid.
         * @throws IgniteCheckedException If failed.
         */
        private boolean validateTail() throws IgniteCheckedException {
            Tail<L> t = tail;

            if (t.down == null) {
                // It is just a regular merge in progress.
                assert needMergeEmptyBranch != TRUE;
                assert needReplaceInner != TRUE;

                return true;
            }

            Tail<L> left = t.getLeftChild();
            Tail<L> right = t.getRightChild();

            assert left.pageId != right.pageId;

            int cnt = t.getCount();

            if (cnt != 0) {
                int idx = fix(insertionPoint(t));

                if (idx == cnt)
                    idx--;

                // The locked left and right pages allowed to be children of the tail.
                if (isChild(t, left, idx, cnt, false) && isChild(t, right, idx, cnt, true))
                    return true;
            }

            // Otherwise they must correctly reside with respect to tail sibling.
            Tail<L> s = t.sibling;

            if (s == null)
                return false;

            // It must be the rightmost element.
            int idx = cnt == 0 ? 0 : cnt - 1;

            if (s.type == Tail.FORWARD)
                return isChild(t, left, idx, cnt, true) &&
                    isChild(s, right, 0, 0, false);

            assert s.type == Tail.BACK;

            if (!isChild(t, right, 0, 0, false))
                return false;

            cnt = s.getCount();
            idx = cnt == 0 ? 0 : cnt - 1;

            return isChild(s, left, idx, cnt, true);
        }

        /**
         * @param prnt Parent.
         * @param child Child.
         * @param idx Index.
         * @param cnt Count.
         * @param right Right or left.
         * @return {@code true} If they are really parent and child.
         */
        private boolean isChild(Tail<L> prnt, Tail<L> child, int idx, int cnt, boolean right) {
            if (right && cnt != 0)
                idx++;

            return inner(prnt.io).getLeft(prnt.buf, idx) == child.pageId;
        }

        /**
         * @param prnt Parent.
         * @param left Left.
         * @param right Right.
         * @param idx Index.
         * @return {@code true} If children are correct.
         */
        private boolean checkChildren(Tail<L> prnt, Tail<L> left, Tail<L> right, int idx) {
            assert idx >= 0 && idx < prnt.getCount(): idx;

            return inner(prnt.io).getLeft(prnt.buf, idx) == left.pageId &&
                inner(prnt.io).getRight(prnt.buf, idx) == right.pageId;
        }

        /**
         * @param prnt Parent tail.
         * @param left Left child tail.
         * @param right Right child tail.
         * @return {@code true} If merged successfully.
         * @throws IgniteCheckedException If failed.
         */
        private boolean doMerge(Tail<L> prnt, Tail<L> left, Tail<L> right)
            throws IgniteCheckedException {
            assert right.io == left.io; // Otherwise incompatible.
            assert left.io.getForward(left.buf) == right.pageId;

            int prntCnt = prnt.getCount();
            int prntIdx = fix(insertionPoint(prnt));

            // Fix index for the right move: remove the last item.
            if (prntIdx == prntCnt)
                prntIdx--;

            // The only case when the siblings can have different parents is when we are merging
            // top-down an empty branch and we already merged the join point with non-empty branch.
            // This happens because when merging empty page we do not update parent link to a lower
            // empty page in the branch since it will be dropped anyways.
            if (needMergeEmptyBranch == READY)
                assert left.getCount() == 0 || right.getCount() == 0; // Empty branch check.
            else if (!checkChildren(prnt, left, right, prntIdx))
                return false;

            boolean emptyBranch = needMergeEmptyBranch == TRUE || needMergeEmptyBranch == READY;

            if (!left.io.merge(prnt.io, prnt.buf, prntIdx, left.buf, right.buf, emptyBranch, pageSize()))
                return false;

            // Invalidate indexes after successful merge.
            prnt.idx = Short.MIN_VALUE;
            left.idx = Short.MIN_VALUE;

            // TODO GG-11640 log a correct merge record.
            left.walPlc = Boolean.TRUE;

            // Remove split key from parent. If we are merging empty branch then remove only on the top iteration.
            if (needMergeEmptyBranch != READY)
                doRemove(prnt.pageId, prnt.page, prnt.buf, prnt.walPlc, prnt.io, prntCnt, prntIdx);

            // Forward page is now empty and has no links, can free and release it right away.
            freePage(right.pageId, right.page, right.buf, right.walPlc, true);

            return true;
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param walPlc Full page WAL record policy.
         * @param release Release write lock and release page.
         * @throws IgniteCheckedException If failed.
         */
        private void freePage(long pageId, long page, long pageAddr, Boolean walPlc, boolean release)
            throws IgniteCheckedException {

            long effectivePageId = PageIdUtils.effectivePageId(pageId);

            long recycled = recyclePage(pageId, page, pageAddr, walPlc);

            if (effectivePageId != PageIdUtils.effectivePageId(pageId))
                throw new IllegalStateException("Effective page ID must stay the same.");

            if (release)
                writeUnlockAndClose(pageId, page, pageAddr, walPlc);

            addFreePage(recycled);
        }

        /**
         * @param lvl Expected root level.
         * @throws IgniteCheckedException If failed.
         */
        private void cutRoot(int lvl) throws IgniteCheckedException {
            Bool res = write(metaPageId, cutRoot, lvl, FALSE);

            assert res == TRUE : res;
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("unchecked")
        private void reuseFreePages() throws IgniteCheckedException {
            // If we have a bag, then it will be processed at the upper level.
            if (reuseList != null && freePages != null)
                reuseList.addForRecycle(this);
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void replaceInner() throws IgniteCheckedException {
            assert needReplaceInner == READY : needReplaceInner;

            int innerIdx;

            Tail<L> inner = tail;

            for (;;) { // Find inner key to replace.
                assert inner.type == Tail.EXACT : inner.type;
                assert inner.lvl > 0 : "leaf " + tail.lvl;

                innerIdx = insertionPoint(inner);

                if (innerIdx >= 0)
                    break; // Successfully found the inner key.

                // We did not find the inner key to replace.
                if (inner.lvl == 1)
                    return; // After leaf merge inner page lost inner key, nothing to do here.

                // Go level down.
                inner = inner.down;
            }

            Tail<L> leaf = getTail(inner, 0);

            int leafCnt = leaf.getCount();

            assert leafCnt > 0 : leafCnt; // Leaf must be merged at this point already if it was empty.

            int leafIdx = leafCnt - 1; // Last leaf item.

            // We increment remove ID in write lock on inner page, thus it is guaranteed that
            // any successor, who already passed the inner page, will get greater value at leaf
            // than he had read at the beginning of the operation and will retry operation from root.
            long rmvId = globalRmvId.incrementAndGet();

            // Update inner key with the new biggest key of left subtree.
            inner.io.store(inner.buf, innerIdx, leaf.io, leaf.buf, leafIdx);
            inner.io.setRemoveId(inner.buf, rmvId);

            // TODO GG-11640 log a correct inner replace record.
            inner.walPlc = Boolean.TRUE;

            // Update remove ID for the leaf page.
            leaf.io.setRemoveId(leaf.buf, rmvId);

            if (needWalDeltaRecord(leaf.pageId, leaf.page, leaf.walPlc))
                wal.log(new FixRemoveId(cacheId, leaf.pageId, rmvId));
        }

        /**
         * @param prnt Parent for merge.
         * @return {@code true} If merged, {@code false} if not (because of insufficient space or empty parent).
         * @throws IgniteCheckedException If failed.
         */
        private boolean merge(Tail<L> prnt) throws IgniteCheckedException {
            // If we are merging empty branch this is acceptable because even if we merge
            // two routing pages, one of them is effectively dropped in this merge, so just
            // keep a single routing page.
            if (prnt.getCount() == 0 && needMergeEmptyBranch != READY)
                return false; // Parent is an empty routing page, child forward page will have another parent.

            Tail<L> left = prnt.getLeftChild();
            Tail<L> right = prnt.getRightChild();

            if (!doMerge(prnt, left, right))
                return false;

            // left from BACK becomes EXACT.
            if (left.type == Tail.BACK) {
                assert left.sibling == null;

                left.down = right.down;
                left.type = Tail.EXACT;
                prnt.down = left;
            }
            else { // left is already EXACT.
                assert left.type == Tail.EXACT : left.type;
                assert left.sibling != null;

                left.sibling = null;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override boolean isFinished() {
            return row == null;
        }

        /**
         * Release pages for all locked levels at the tail.
         */
        private void releaseTail() {
            doReleaseTail(tail);

            tail = null;
        }

        /**
         * @param t Tail.
         */
        private void doReleaseTail(Tail<L> t) {
            while (t != null) {
                writeUnlockAndClose(t.pageId, t.page, t.buf, t.walPlc);

                Tail<L> s = t.sibling;

                if (s != null)
                    writeUnlockAndClose(s.pageId, s.page, s.buf, s.walPlc);

                t = t.down;
            }
        }

        /** {@inheritDoc} */
        @Override boolean canRelease(long pageId, int lvl) {
            return pageId != 0L && !isTail(pageId, lvl);
        }

        /**
         * @param pageId Page ID.
         * @param lvl Level.
         * @return {@code true} If the given page is in tail.
         */
        private boolean isTail(long pageId, int lvl) {
            Tail t = tail;

            while (t != null) {
                if (t.lvl < lvl)
                    return false;

                if (t.lvl == lvl) {
                    if (t.pageId == pageId)
                        return true;

                    t = t.sibling;

                    return t != null && t.pageId == pageId;
                }

                t = t.down;
            }

            return false;
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param io IO.
         * @param lvl Level.
         * @param type Type.
         * @return Added tail.
         */
        private Tail<L> addTail(long pageId, long page, long pageAddr, BPlusIO<L> io, int lvl, byte type) {
            final Tail<L> t = new Tail<>(pageId, page, pageAddr, io, type, lvl);

            if (tail == null)
                tail = t;
            else if (tail.lvl == lvl) { // Add on the same level.
                assert tail.sibling == null; // Only two siblings on a single level.

                if (type == Tail.EXACT) {
                    assert tail.type != Tail.EXACT;

                    if (tail.down != null) { // Take down from sibling, EXACT must own down link.
                        t.down = tail.down;
                        tail.down = null;
                    }

                    t.sibling = tail;
                    tail = t;
                }
                else {
                    assert tail.type == Tail.EXACT : tail.type;

                    tail.sibling = t;
                }
            }
            else if (tail.lvl == lvl - 1) { // Add on top of existing level.
                t.down = tail;
                tail = t;
            }
            else
                throw new IllegalStateException();

            return t;
        }

        /**
         * @param tail Tail to start with.
         * @param lvl Level.
         * @return Tail of {@link Tail#EXACT} type at the given level.
         */
        private Tail<L> getTail(Tail<L> tail, int lvl) {
            assert tail != null;
            assert lvl >= 0 && lvl <= tail.lvl : lvl;

            Tail<L> t = tail;

            while (t.lvl != lvl)
                t = t.down;

            assert t.type == Tail.EXACT : t.type; // All the down links must be of EXACT type.

            return t;
        }

        /**
         * @param keys If we have to show keys.
         * @return Tail as a String.
         * @throws IgniteCheckedException If failed.
         */
        private String printTail(boolean keys) throws IgniteCheckedException {
            SB sb = new SB("");

            Tail<L> t = tail;

            while (t != null) {
                sb.a(t.lvl).a(": ").a(printPage(t.io, t.buf, keys));

                Tail<L> d = t.down;

                t = t.sibling;

                if (t != null)
                    sb.a(" -> ").a(t.type == Tail.FORWARD ? "F" : "B").a(' ').a(printPage(t.io, t.buf, keys));

                sb.a('\n');

                t = d;
            }

            return sb.toString();
        }

        /**
         * @param rootLvl Actual root level.
         * @return {@code true} If tail level is correct.
         */
        private boolean checkTailLevel(int rootLvl) {
            return tail == null || tail.lvl < rootLvl;
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void releaseAll() throws IgniteCheckedException {
            releaseTail();
            reuseFreePages();
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param backId Back page ID.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteCheckedException If failed.
         */
        private Result finishOrLockTail(long pageId, long page, long backId, long fwdId, int lvl)
            throws IgniteCheckedException {
            Result res = finishTail();

            if (res == NOT_FOUND)
                res = lockTail(pageId, page, backId, fwdId, lvl);

            return res;
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param backId Back page ID.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteCheckedException If failed.
         */
        private Result tryRemoveFromLeaf(long pageId, long page, long backId, long fwdId, int lvl)
            throws IgniteCheckedException {
            // We must be at the bottom here, just need to remove row from the current page.
            assert lvl == 0 : lvl;

            Result res = removeFromLeaf(pageId, page, backId, fwdId);

            if (res == FOUND && tail == null) // Finish if we don't need to do any merges.
                finish();

            return res;
        }
    }

    /**
     * Tail for remove.
     */
    private static final class Tail<L> {
        /** */
        static final byte BACK = 0;

        /** */
        static final byte EXACT = 1;

        /** */
        static final byte FORWARD = 2;

        /** */
        private final long pageId;

        /** */
        private final long page;

        /** */
        private final long buf;

        /** */
        private Boolean walPlc;

        /** */
        private final BPlusIO<L> io;

        /** */
        private byte type;

        /** */
        private final int lvl;

        /** */
        private short idx = Short.MIN_VALUE;

        /** Only {@link #EXACT} tail can have either {@link #BACK} or {@link #FORWARD} sibling.*/
        private Tail<L> sibling;

        /** Only {@link #EXACT} tail can point to {@link #EXACT} tail of lower level. */
        private Tail<L> down;

        /**
         * @param pageId Page ID.
         * @param page Page absolute pointer.
         * @param buf Buffer.
         * @param io IO.
         * @param type Type.
         * @param lvl Level.
         */
        private Tail(long pageId, long page, long buf, BPlusIO<L> io, byte type, int lvl) {
            assert type == BACK || type == EXACT || type == FORWARD : type;
            assert lvl >= 0 && lvl <= Byte.MAX_VALUE : lvl;
            assert pageId != 0L;
            assert page != 0L;
            assert buf != 0L;

            this.pageId = pageId;
            this.page = page;
            this.buf = buf;
            this.io = io;
            this.type = type;
            this.lvl = (byte)lvl;
        }

        /**
         * @return Count.
         */
        private int getCount() {
            return io.getCount(buf);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return new SB("Tail[").a("pageId=").appendHex(pageId).a(", cnt= ").a(getCount())
                .a(", lvl=" + lvl).a(", sibling=").a(sibling).a("]").toString();
        }

        /**
         * @return Left child.
         */
        private Tail<L> getLeftChild() {
            Tail<L> s = down.sibling;

            return s.type == Tail.BACK ? s : down;
        }

        /**
         * @return Right child.
         */
        private Tail<L> getRightChild() {
            Tail<L> s = down.sibling;

            return s.type == Tail.FORWARD ? s : down;
        }
    }

    /**
     * @param io IO.
     * @param buf Buffer.
     * @param low Start index.
     * @param cnt Row count.
     * @param row Lookup row.
     * @param shift Shift if equal.
     * @return Insertion point as in {@link Arrays#binarySearch(Object[], Object, Comparator)}.
     * @throws IgniteCheckedException If failed.
     */
    private int findInsertionPoint(int lvl, BPlusIO<L> io, long buf, int low, int cnt, L row, int shift)
        throws IgniteCheckedException {
        assert row != null;

        int high = cnt - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            int cmp = compare(lvl, io, buf, mid, row);

            if (cmp == 0)
                cmp = -shift; // We need to fix the case when search row matches multiple data rows.

            //noinspection Duplicates
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // Found.
        }

        return -(low + 1);  // Not found.
    }

    /**
     * @param pageAddr Page address.
     * @return IO.
     */
    private BPlusIO<L> io(long pageAddr) {
        assert pageAddr != 0;

        int type = PageIO.getType(pageAddr);
        int ver = PageIO.getVersion(pageAddr);

        if (innerIos.getType() == type)
            return innerIos.forVersion(ver);

        if (leafIos.getType() == type)
            return leafIos.forVersion(ver);

        throw new IllegalStateException("Unknown page type: " + type + " pageId: " + U.hexLong(PageIO.getPageId(pageAddr)));
    }

    /**
     * @param io IO.
     * @return Inner page IO.
     */
    private static <L> BPlusInnerIO<L> inner(BPlusIO<L> io) {
        assert !io.isLeaf();

        return (BPlusInnerIO<L>)io;
    }

    /**
     * @return Latest version of inner page IO.
     */
    protected final BPlusInnerIO<L> latestInnerIO() {
        return innerIos.latest();
    }

    /**
     * @return Latest version of leaf page IO.
     */
    protected final BPlusLeafIO<L> latestLeafIO() {
        return leafIos.latest();
    }

    /**
     * @param io IO.
     * @param pageAddr Page address.
     * @param idx Index of row in the given buffer.
     * @param row Lookup row.
     * @return Comparison result as in {@link Comparator#compare(Object, Object)}.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract int compare(BPlusIO<L> io, long pageAddr, int idx, L row) throws IgniteCheckedException;

    /**
     * @param lvl Level.
     * @param io IO.
     * @param pageAddr Page address.
     * @param idx Index of row in the given buffer.
     * @param row Lookup row.
     * @return Comparison result as in {@link Comparator#compare(Object, Object)}.
     * @throws IgniteCheckedException If failed.
     */
    protected int compare(int lvl, BPlusIO<L> io, long pageAddr, int idx, L row) throws IgniteCheckedException {
        return compare(io, pageAddr, idx, row);
    }

    /**
     * Get a full detached data row.
     *
     * @param io IO.
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Full detached data row.
     * @throws IgniteCheckedException If failed.
     */
    protected final T getRow(BPlusIO<L> io, long pageAddr, int idx) throws IgniteCheckedException {
        return getRow(io, pageAddr, idx, null);
    }

    /**
     * Get data row. Can be called on inner page only if {@link #canGetRowFromInner} is {@code true}.
     *
     * @param io IO.
     * @param pageAddr Page address.
     * @param idx Index.
     * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
     * @return Data row.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract T getRow(BPlusIO<L> io, long pageAddr, int idx, Object x) throws IgniteCheckedException;

    /**
     * Forward cursor.
     */
    @SuppressWarnings("unchecked")
    private final class ForwardCursor implements GridCursor<T> {
        /** */
        private T[] rows = (T[])EMPTY;

        /** */
        private int row = -1;

        /** */
        private long nextPageId;

        /** */
        private L lowerBound;

        /** */
        private int lowerShift = -1; // Initially it is -1 to handle multiple equal rows.

        /** */
        private final L upperBound;

        /** */
        private final Object x;

        /**
         * @param lowerBound Lower bound.
         * @param upperBound Upper bound.
         */
        ForwardCursor(L lowerBound, L upperBound) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.x = null;
        }

        /**
         * @param lowerBound Lower bound.
         * @param upperBound Upper bound.
         * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
         */
        ForwardCursor(L lowerBound, L upperBound, Object x) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.x = x;
        }

        /**
         * @param pageAddr Page address.
         * @param io IO.
         * @param startIdx Start index.
         * @throws IgniteCheckedException If failed.
         */
        private void init(long pageAddr, BPlusIO<L> io, int startIdx) throws IgniteCheckedException {
            nextPageId = 0;
            row = -1;

            int cnt = io.getCount(pageAddr);

            // If we see an empty page here, it means that it is an empty tree.
            if (cnt == 0) {
                assert io.getForward(pageAddr) == 0L;

                rows = null;
            }
            else if (!fillFromBuffer(pageAddr, io, startIdx, cnt)) {
                if (rows != EMPTY) {
                    assert rows.length > 0; // Otherwise it makes no sense to create an array.

                    // Fake clear.
                    rows[0] = null;
                }
            }
        }

        /**
         * @param pageAddr Page address.
         * @param io IO.
         * @param cnt Count.
         * @return Adjusted to lower bound start index.
         * @throws IgniteCheckedException If failed.
         */
        private int findLowerBound(long pageAddr, BPlusIO<L> io, int cnt) throws IgniteCheckedException {
            assert io.isLeaf();

            // Compare with the first row on the page.
            int cmp = compare(0, io, pageAddr, 0, lowerBound);

            if (cmp < 0 || (cmp == 0 && lowerShift == 1)) {
                int idx = findInsertionPoint(0, io, pageAddr, 0, cnt, lowerBound, lowerShift);

                assert idx < 0;

                return fix(idx);
            }

            return 0;
        }

        /**
         * @param pageAddr Page address.
         * @param io IO.
         * @param low Start index.
         * @param cnt Number of rows in the buffer.
         * @return Corrected number of rows with respect to upper bound.
         * @throws IgniteCheckedException If failed.
         */
        private int findUpperBound(long pageAddr, BPlusIO<L> io, int low, int cnt) throws IgniteCheckedException {
            assert io.isLeaf();

            // Compare with the last row on the page.
            int cmp = compare(0, io, pageAddr, cnt - 1, upperBound);

            if (cmp > 0) {
                int idx = findInsertionPoint(0, io, pageAddr, low, cnt, upperBound, 1);

                assert idx < 0;

                cnt = fix(idx);

                nextPageId = 0; // The End.
            }

            return cnt;
        }

        /**
         * @param pageAddr Page address.
         * @param io IO.
         * @param startIdx Start index.
         * @param cnt Number of rows in the buffer.
         * @return {@code true} If we were able to fetch rows from this page.
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("unchecked")
        private boolean fillFromBuffer(long pageAddr, BPlusIO<L> io, int startIdx, int cnt)
            throws IgniteCheckedException {
            assert io.isLeaf() : io;
            assert cnt != 0 : cnt; // We can not see empty pages (empty tree handled in init).
            assert startIdx >= 0 : startIdx;
            assert cnt >= startIdx;

            checkDestroyed();

            nextPageId = io.getForward(pageAddr);

            if (lowerBound != null && startIdx == 0)
                startIdx = findLowerBound(pageAddr, io, cnt);

            if (upperBound != null && cnt != startIdx)
                cnt = findUpperBound(pageAddr, io, startIdx, cnt);

            cnt -= startIdx;

            if (cnt == 0)
                return false;

            if (rows == EMPTY)
                rows = (T[])new Object[cnt];

            for (int i = 0; i < cnt; i++) {
                T r = getRow(io, pageAddr, startIdx + i, x);

                rows = GridArrays.set(rows, i, r);
            }

            GridArrays.clearTail(rows, cnt);

            return true;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("SimplifiableIfStatement")
        @Override public boolean next() throws IgniteCheckedException {
            if (rows == null)
                return false;

            if (++row < rows.length && rows[row] != null) {
                clearLastRow(); // Allow to GC the last returned row.

                return true;
            }

            return nextPage();
        }

        /**
         * @return Cleared last row.
         */
        private T clearLastRow() {
            if (row == 0)
                return null;

            int last = row - 1;

            T r = rows[last];

            assert r != null;

            rows[last] = null;

            return r;
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void find() throws IgniteCheckedException {
            assert lowerBound != null;

            doFind(new GetCursor(lowerBound, lowerShift, this));
        }

        /**
         * @throws IgniteCheckedException If failed.
         * @return {@code True} If we have rows to return after reading the next page.
         */
        private boolean reinitialize() throws IgniteCheckedException {
            // If initially we had no lower bound, then we have to have non-null lastRow argument here
            // (if the tree is empty we never call this method), otherwise we always fallback
            // to the previous lower bound.
            find();

            return next();
        }

        /**
         * @return {@code true} If we have rows to return after reading the next page.
         * @throws IgniteCheckedException If failed.
         */
        private boolean nextPage() throws IgniteCheckedException {
            updateLowerBound(clearLastRow());

            row = 0;

            for (;;) {
                if (nextPageId == 0) {
                    rows = null;

                    return false; // Done.
                }

                long pageId = nextPageId;
                long page = acquirePage(pageId);
                try {
                    long pageAddr = readLock(pageId, page); // Doing explicit null check.

                    // If concurrent merge occurred we have to reinitialize cursor from the last returned row.
                    if (pageAddr == 0L)
                        break;

                    try {
                        BPlusIO<L> io = io(pageAddr);

                        if (fillFromBuffer(pageAddr, io, 0, io.getCount(pageAddr)))
                            return true;

                        // Continue fetching forward.
                    }
                    finally {
                        readUnlock(pageId, page, pageAddr);
                    }
                }
                finally {
                    releasePage(pageId, page);
                }
            }

            // Reinitialize when `next` is released.
            return reinitialize();
        }

        /**
         * @param lower New exact lower bound.
         */
        private void updateLowerBound(T lower) {
            if (lower != null) {
                lowerShift = 1; // Now we have the full row an need to avoid duplicates.
                lowerBound = lower; // Move the lower bound forward for further concurrent merge retries.
            }
        }

        /** {@inheritDoc} */
        @Override public T get() {
            T r = rows[row];

            assert r != null;

            return r;
        }
    }

    /**
     * Page handler for basic {@link Get} operation.
     */
    private abstract class GetPageHandler<G extends Get> extends PageHandler<G, Result> {
        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Result run(int cacheId, long pageId, long page, long pageAddr, PageIO iox, Boolean walPlc,
            G g, int lvl) throws IgniteCheckedException {
            assert PageIO.getPageId(pageAddr) == pageId;

            // If we've passed the check for correct page ID, we can safely cast.
            BPlusIO<L> io = (BPlusIO<L>)iox;

            // In case of intersection with inner replace in remove operation
            // we need to restart our operation from the tree root.
            if (lvl == 0 && g.rmvId < io.getRemoveId(pageAddr))
                return RETRY_ROOT;

            return run0(pageId, page, pageAddr, io, g, lvl);
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param io IO.
         * @param g Operation.
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        protected abstract Result run0(long pageId, long page, long pageAddr, BPlusIO<L> io, G g, int lvl)
            throws IgniteCheckedException;

        /** {@inheritDoc} */
        @Override public boolean releaseAfterWrite(int cacheId, long pageId, long page, long pageAddr, G g, int lvl) {
            return g.canRelease(pageId, lvl);
        }
    }

    /**
     * Reuse bag for destroy.
     */
    protected static final class DestroyBag extends GridLongList implements ReuseBag {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Default constructor for {@link Externalizable}.
         */
        public DestroyBag() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void addFreePage(long pageId) {
            add(pageId);
        }

        /** {@inheritDoc} */
        @Override public long pollFreePage() {
            return isEmpty() ? 0 : remove();
        }
    }

    /**
     *
     */
    private static class TreeMetaData {
        /** */
        final int rootLvl;

        /** */
        final long rootId;

        /**
         * @param rootLvl Root level.
         * @param rootId Root page ID.
         */
        TreeMetaData(int rootLvl, long rootId) {
            this.rootLvl = rootLvl;
            this.rootId = rootId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TreeMetaData.class, this);
        }
    }

    /**
     * Operation result.
     */
    enum Result {
        /** */
        GO_DOWN,

        /** */
        GO_DOWN_X,

        /** */
        FOUND,

        /** */
        NOT_FOUND,

        /** */
        RETRY,

        /** */
        RETRY_ROOT
    }

    /**
     * Four state boolean.
     */
    enum Bool {
        /** */
        FALSE,

        /** */
        TRUE,

        /** */
        READY,

        /** */
        DONE
    }
}
