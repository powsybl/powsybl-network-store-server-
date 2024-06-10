/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.tck;

import com.powsybl.iidm.network.tck.AbstractNetworkTest;
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
public class NetworkIT extends AbstractNetworkTest {

    @Test
    @Override
    public void testWith() {
        // FIXME remove this when we fix the fact that we use the "Default" NetworkFactory in the base class
    }

    @Test
    @Override
    public void testScadaNetwork() {
        // FIXME test throws UnsupportedOperationException: Validation level below STEADY_STATE_HYPOTHESIS not supported
    }

    @Test
    @Override
    public void testStreams() {
        // FIXME remove this test when we use the release containing this PR : https://github.com/powsybl/powsybl-core/pull/3020
    }

    @Override
    public void testNetwork1() {
        // FIXME remove this test when getFictitiousP0 and getFictitiousQ0 of CalculatedBus are implemented
    }

    @Override
    public void testPermanentLimitViaAdder() {
        // FIXME remove this test when we add validation on CurrentLimitAdder
    }

    @Override
    public void testPermanentLimitOnUnselectedOperationalLimitsGroup() {
        // FIXME remove this test when we add validation on CurrentLimitAdder
    }

    @Override
    public void testPermanentLimitOnSelectedOperationalLimitsGroup() {
        // FIXME remove this test when we add validation on CurrentLimitAdder
    }
}
