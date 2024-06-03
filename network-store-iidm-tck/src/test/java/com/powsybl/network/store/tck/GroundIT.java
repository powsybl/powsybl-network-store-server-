/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.tck;

import com.powsybl.iidm.network.*;
import com.powsybl.iidm.network.tck.AbstractGroundTest;
import com.powsybl.iidm.network.test.TwoVoltageLevelNetworkFactory;
import com.powsybl.network.store.server.NetworkStoreApplication;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Abdelsalem HEDHILI <abdelsalem.hedhili at rte-france.com>
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ContextHierarchy({
        @ContextConfiguration(classes = {NetworkStoreApplication.class})
})
@TestPropertySource(properties = { "spring.config.location=classpath:application.yaml" })
class GroundIT extends AbstractGroundTest {

    @Test
    void testOnSubnetwork() {
        // FIXME remove this test when subnetworks are implemented
    }
}
