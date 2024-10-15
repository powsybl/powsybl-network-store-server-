/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.tck;

import com.powsybl.iidm.network.tck.AbstractVoltageLevelExportTest;
import com.powsybl.network.store.server.NetworkStoreApplication;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;

import java.io.IOException;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {"spring.config.location=classpath:application.yaml"})
@ContextHierarchy({@ContextConfiguration(classes = {NetworkStoreApplication.class})})
class VoltageLevelExportIT extends AbstractVoltageLevelExportTest {
    @Disabled("VoltageLevelImpl.exportTopology isn't implemented")
    @Test
    @Override
    public void nodeBreakerTest() throws IOException {
        // FIXME remove this test when VoltageLevelImpl.exportTopology is implemented
        super.nodeBreakerTest();
    }

    @Disabled("VoltageLevelImpl.exportTopology isn't implemented")
    @Test
    @Override
    public void busBreakerTest() throws IOException {
        // FIXME remove this test when VoltageLevelImpl.exportTopology is implemented
        super.busBreakerTest();
    }
}
