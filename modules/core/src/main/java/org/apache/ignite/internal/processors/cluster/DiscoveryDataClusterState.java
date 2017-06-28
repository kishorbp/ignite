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

package org.apache.ignite.internal.processors.cluster;

import java.io.Serializable;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class DiscoveryDataClusterState implements Serializable {
    /** */
    private final boolean active;

    /** */
    private final AffinityTopologyVersion transitionTopVer;

    /** */
    private final Set<UUID> transitionNodes;

    public static DiscoveryDataClusterState createState(boolean active) {
        return new DiscoveryDataClusterState(active, null, null);
    }

    public static DiscoveryDataClusterState createTransitionState(boolean active,
        AffinityTopologyVersion transitionTopVer,
        Set<UUID> transitionNodes) {
        assert transitionTopVer != null;
        assert !F.isEmpty(transitionNodes) : transitionNodes;

        return new DiscoveryDataClusterState(active, transitionTopVer, transitionNodes);
    }

    /**
     * @param active
     * @param transitionTopVer
     * @param transitionNodes
     */
    private DiscoveryDataClusterState(boolean active,
        @Nullable AffinityTopologyVersion transitionTopVer,
        @Nullable Set<UUID> transitionNodes) {
        this.active = active;
        this.transitionTopVer = transitionTopVer;
        this.transitionNodes = transitionNodes;
    }

    public boolean transition() {
        return transitionTopVer != null;
    }

    public AffinityTopologyVersion transitionTopologyVersion() {
        return transitionTopVer;
    }

    public boolean active() {
        return active;
    }

    public Set<UUID> transitionNodes() {
        return transitionNodes;
    }
}
