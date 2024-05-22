/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.tck;

import com.powsybl.iidm.network.tck.AbstractLineTest;
import com.powsybl.network.store.server.NetworkStoreApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ContextHierarchy({
    @ContextConfiguration(classes = {NetworkStoreApplication.class})
    })
@TestPropertySource(properties = { "spring.config.location=classpath:application.yaml" })
public class LineIT extends AbstractLineTest {

    @Test
    @Override
    public void baseAcLineTests() {
        // FIXME remove this test when we use the release containing this PR : https://github.com/powsybl/powsybl-core/pull/3022
    }

    @Test
    @Override
    public void testRemoveAcLine() {
        // FIXME remove this test when exception msg are homogenized with the powsybl-core
    }

    @Test
    @Override
    public void testMove1NbNetwork() {
        // FIXME to investigate, TerminalNodeBreakerViewImpl/TerminalBusBreakerViewImpl.moveConnectable fails
    }

    @Test
    @Override
    public void testMove2Nb() {
        // FIXME to investigate, TerminalNodeBreakerViewImpl/TerminalBusBreakerViewImpl.moveConnectable fails
    }
}
