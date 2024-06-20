/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.tck;

import com.powsybl.iidm.network.tck.AbstractMergeNetworkTest;
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
class MergeNetworkIT extends AbstractMergeNetworkTest {

    //FIXME remove all these tests when network merge is implemented
    @Test
    @Override
    public void checkMergingDifferentFormat() {
        // FIXME
    }

    @Test
    @Override
    public void testMerge() {
        // FIXME
    }

    @Test
    @Override
    public void failMergeIfMultiVariants() {
        // FIXME
    }

    @Test
    @Override
    public void failMergeWithSameObj() {
        // FIXME
    }

    @Test
    @Override
    public void multipleDanglingLinesInMergedNetwork() {
        // FIXME
    }

    @Test
    @Override
    public void test() {
        // FIXME
    }

    @Test
    @Override
    public void mergeThenCloneVariantBug() {
        // FIXME
    }

    @Test
    @Override
    public void multipleDanglingLinesInMergingNetwork() {
        // FIXME
    }

    @Test
    @Override
    public void checkMergingSameFormat() {
        // FIXME
    }

    @Test
    @Override
    public void testMergeAndDetach() {
        // FIXME
    }

    @Test
    @Override
    public void testMergeAndDetachWithExtensions() {
        // FIXME
    }

    @Test
    @Override
    public void failDetachWithALineBetween2Subnetworks() {
        //FIXME
    }

    @Test
    @Override
    public void failDetachIfMultiVariants() {
        //FIXME
    }

    @Test
    @Override
    public void testMerge3Networks() {
        // FIXME
    }

    @Test
    @Override
    public void failMergeDanglingLinesWithSameId() {
        // FIXME
    }

    @Test
    @Override
    public void testValidationLevelWhenMerging2Eq() {
        // FIXME
    }

    @Test
    @Override
    public void testValidationLevelWhenMergingEqAndSsh() {
        // FIXME
    }

    @Test
    @Override
    public void testValidationLevelWhenMerging2Ssh() {
        // FIXME
    }

    @Test
    @Override
    public void failMergeOnlyOneNetwork() {
        // FIXME
    }

    @Test
    @Override
    public void failMergeOnSubnetworks() {
        // FIXME
    }

    @Test
    @Override
    public void failMergeSubnetworks() {
        // FIXME
    }

    @Test
    @Override
    public void failMergeContainingSubnetworks() {
        // FIXME
    }

    @Test
    @Override
    public void testNoEmptyAdditionalSubnetworkIsCreated() {
        // FIXME
    }

    @Test
    @Override
    public void testListeners() {
        // FIXME
    }

    @Test
    @Override
    public void dontCreateATieLineWithAlreadyMergedDanglingLinesInMergedNetwork() {
        // FIXME
    }

    @Test
    @Override
    public void dontCreateATieLineWithAlreadyMergedDanglingLinesInMergingNetwork() {
        // FIXME
    }

    @Test
    @Override
    public void multipleConnectedDanglingLinesInMergedNetwork() {
        // FIXME
    }

    @Test
    @Override
    public void multipleConnectedDanglingLinesWithSamePairingKey() {
        // FIXME
    }

    @Test
    @Override
    public void invertDanglingLinesWhenCreatingATieLine() {
        // FIXME
    }

    @Test
    @Override
    public void testMergeAndDetachWithProperties() {
        // FIXME
    }

}
