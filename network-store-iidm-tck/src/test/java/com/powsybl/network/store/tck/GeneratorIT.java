/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.tck;

import com.powsybl.iidm.network.tck.AbstractGeneratorTest;
import com.powsybl.network.store.server.NetworkStoreApplication;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {"spring.config.location=classpath:application.yaml"})
@ContextHierarchy({@ContextConfiguration(classes = {NetworkStoreApplication.class})})
class GeneratorIT extends AbstractGeneratorTest {
    @Disabled("exception message aren't homogenized with the powsybl-core")
    @Override
    @Test
    public void testRemove() {
        // FIXME remove this test when exception msg are homogenized with the powsybl-core
        super.testRemove();
    }

    @Disabled("primary key constraints violation on DB")
    @Override
    @Test
    public void testSetterGetterInMultiVariants() {
        //FIXME remove when we fix primary key constraints violation on DB
        super.testSetterGetterInMultiVariants();
    }
}
