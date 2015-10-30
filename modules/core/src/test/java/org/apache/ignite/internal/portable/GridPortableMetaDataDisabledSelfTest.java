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
package org.apache.ignite.internal.portable;

import java.util.Arrays;
import org.apache.ignite.IgniteObjects;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.igniteobject.IgniteObjectBuilder;
import org.apache.ignite.igniteobject.IgniteObjectException;
import org.apache.ignite.igniteobject.IgniteObjectMarshalAware;
import org.apache.ignite.igniteobject.IgniteObjectReader;
import org.apache.ignite.igniteobject.IgniteObjectConfiguration;
import org.apache.ignite.igniteobject.IgniteObjectWriter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for disabled meta data.
 */
public class GridPortableMetaDataDisabledSelfTest extends GridCommonAbstractTest {
    /** */
    private PortableMarshaller marsh;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(marsh);

        return cfg;
    }

    /**
     * @return Portables.
     */
    private IgniteObjects portables() {
        return grid().portables();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisableGlobal() throws Exception {
        marsh = new PortableMarshaller();

        marsh.setClassNames(Arrays.asList(
            TestObject1.class.getName(),
            TestObject2.class.getName()
        ));

        marsh.setMetaDataEnabled(false);

        try {
            startGrid();

            portables().toPortable(new TestObject1());
            portables().toPortable(new TestObject2());
            portables().toPortable(new TestObject3());

            assertEquals(0, portables().metadata(TestObject1.class).fields().size());
            assertEquals(0, portables().metadata(TestObject2.class).fields().size());

            IgniteObjectBuilder bldr = portables().builder("FakeType");

            bldr.setField("field1", 0).setField("field2", "value").build();

            assertNull(portables().metadata("FakeType"));
            assertNull(portables().metadata(TestObject3.class));
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisableGlobalSimpleClass() throws Exception {
        marsh = new PortableMarshaller();

        IgniteObjectConfiguration typeCfg = new IgniteObjectConfiguration(TestObject2.class.getName());

        typeCfg.setMetaDataEnabled(true);

        marsh.setTypeConfigurations(Arrays.asList(
            new IgniteObjectConfiguration(TestObject1.class.getName()),
            typeCfg
        ));

        marsh.setMetaDataEnabled(false);

        try {
            startGrid();

            portables().toPortable(new TestObject1());
            portables().toPortable(new TestObject2());

            assertEquals(0, portables().metadata(TestObject1.class).fields().size());
            assertEquals(1, portables().metadata(TestObject2.class).fields().size());
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisableGlobalMarshalAwareClass() throws Exception {
        marsh = new PortableMarshaller();

        IgniteObjectConfiguration typeCfg = new IgniteObjectConfiguration(TestObject1.class.getName());

        typeCfg.setMetaDataEnabled(true);

        marsh.setTypeConfigurations(Arrays.asList(
            new IgniteObjectConfiguration(TestObject2.class.getName()),
            typeCfg
        ));

        marsh.setMetaDataEnabled(false);

        try {
            startGrid();

            portables().toPortable(new TestObject1());
            portables().toPortable(new TestObject2());

            assertEquals(1, portables().metadata(TestObject1.class).fields().size());
            assertEquals(0, portables().metadata(TestObject2.class).fields().size());
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisableSimpleClass() throws Exception {
        marsh = new PortableMarshaller();

        IgniteObjectConfiguration typeCfg = new IgniteObjectConfiguration(TestObject1.class.getName());

        typeCfg.setMetaDataEnabled(false);

        marsh.setTypeConfigurations(Arrays.asList(
            new IgniteObjectConfiguration(TestObject2.class.getName()),
            typeCfg
        ));

        try {
            startGrid();

            portables().toPortable(new TestObject1());
            portables().toPortable(new TestObject2());

            assertEquals(0, portables().metadata(TestObject1.class).fields().size());
            assertEquals(1, portables().metadata(TestObject2.class).fields().size());
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisableMarshalAwareClass() throws Exception {
        marsh = new PortableMarshaller();

        IgniteObjectConfiguration typeCfg = new IgniteObjectConfiguration(TestObject2.class.getName());

        typeCfg.setMetaDataEnabled(false);

        marsh.setTypeConfigurations(Arrays.asList(
            new IgniteObjectConfiguration(TestObject1.class.getName()),
            typeCfg
        ));

        try {
            startGrid();

            portables().toPortable(new TestObject1());
            portables().toPortable(new TestObject2());

            assertEquals(1, portables().metadata(TestObject1.class).fields().size());
            assertEquals(0, portables().metadata(TestObject2.class).fields().size());
        }
        finally {
            stopGrid();
        }
    }

    /**
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class TestObject1 {
        /** */
        private int field;
    }

    /**
     */
    private static class TestObject2 implements IgniteObjectMarshalAware {
        /** {@inheritDoc} */
        @Override public void writePortable(IgniteObjectWriter writer) throws IgniteObjectException {
            writer.writeInt("field", 1);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(IgniteObjectReader reader) throws IgniteObjectException {
            // No-op.
        }
    }

    /**
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class TestObject3 {
        /** */
        private int field;
    }
}