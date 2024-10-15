/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.tck.extensions;

import com.powsybl.iidm.network.tck.extensions.AbstractSlackTerminalTest;
import com.powsybl.network.store.server.NetworkStoreApplication;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {"spring.config.location=classpath:application.yaml"})
@ContextHierarchy({@ContextConfiguration(classes = {NetworkStoreApplication.class})})
class SlackTerminalIT extends AbstractSlackTerminalTest {
    @Disabled("primary key constraints violation on DB")
    @Override
    @Test
    public void variantsResetTest() {
        //FIXME remove when we fix primary key constraints violation on DB
        super.variantsResetTest();
    }

    @Disabled("primary key constraints violation on DB")
    @Override
    @Test
    public void variantsCloneTest() {
        //FIXME remove when we fix primary key constraints violation on DB
        super.variantsCloneTest();
    }
}
