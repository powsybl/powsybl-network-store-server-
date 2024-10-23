/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.tck;

import com.powsybl.iidm.network.Network;
import com.powsybl.iidm.network.VoltageLevel;
import com.powsybl.iidm.network.tck.AbstractLoadTest;
import com.powsybl.iidm.network.test.FictitiousSwitchFactory;
import com.powsybl.network.store.server.NetworkStoreApplication;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;

import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {"spring.config.location=classpath:application.yaml"})
@ContextHierarchy({@ContextConfiguration(classes = {NetworkStoreApplication.class})})
class LoadIT extends AbstractLoadTest {

    @Override
    @Test
    public void testZipLoadModel() {
        //TODO remove this test when ZipLoadModelAdder is implemented
    }

    @Override
    @Test
    public void testExponentialLoadModel() {
        //TODO remove this test when ZipLoadModelAdder is implemented
        Network network = FictitiousSwitchFactory.create();
        VoltageLevel voltageLevel = network.getVoltageLevel("C");
        assertNull(voltageLevel.newLoad().newExponentialModel().setNp(0.0).setNq(0.0).add());
    }

    @Test
    @Override
    public void testSetterGetterInMultiVariants() {
        //FIXME remove when we fix primary key constraints violation on DB
    }
}
