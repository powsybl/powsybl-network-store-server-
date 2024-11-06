/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.tck;

import com.powsybl.iidm.network.Generator;
import com.powsybl.iidm.network.Network;
import com.powsybl.iidm.network.VariantManager;
import com.powsybl.iidm.network.VariantManagerConstants;
import com.powsybl.iidm.network.tck.AbstractMultiVariantNetworkTest;
import com.powsybl.iidm.network.test.EurostagTutorialExample1Factory;
import com.powsybl.network.store.server.NetworkStoreApplication;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {"spring.config.location=classpath:application.yaml"})
@ContextHierarchy({@ContextConfiguration(classes = {NetworkStoreApplication.class})})
class MultiVariantNetworkIT extends AbstractMultiVariantNetworkTest {

    private static final String SECOND_VARIANT = "SecondVariant";

    @Test
    public void singleThreadTest() {
        Network network = EurostagTutorialExample1Factory.create();
        final VariantManager manager = network.getVariantManager();
        manager.cloneVariant(VariantManagerConstants.INITIAL_VARIANT_ID, SECOND_VARIANT);
        manager.setWorkingVariant(SECOND_VARIANT);
        final Generator generator = network.getGenerator("GEN");
        generator.setVoltageRegulatorOn(false);
        assertFalse(generator.isVoltageRegulatorOn());
        manager.setWorkingVariant(VariantManagerConstants.INITIAL_VARIANT_ID);
        assertTrue(generator.isVoltageRegulatorOn());
    }

    @Test
    @Override
    public void multiThreadTest() throws InterruptedException {
        // FIXME delete this test when multi-thread access is supported
    }

    @Test
    @Override
    public void multiVariantTopologyTest() {
        // FIXME delete this test when multi-thread access is supported
    }

    @Test
    @Override
    public void variantNotSetTest() throws InterruptedException {
        // FIXME delete this test when multi-thread access is supported
    }

    @Test
    @Override
    public void variantSetTest() throws InterruptedException {
        // FIXME delete this test when multi-thread access is supported
    }
}
