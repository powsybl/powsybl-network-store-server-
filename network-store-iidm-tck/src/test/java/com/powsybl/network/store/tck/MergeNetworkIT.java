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
public class MergeNetworkIT extends AbstractMergeNetworkTest {

    //FIXME remove all these tests when network merge is implemented
    @Test
    @Override
    public void checkMergingDifferentFormat() {
        // FIXME
    }

    @Override
    public void testMerge() {
        // FIXME
    }

    @Override
    public void failMergeIfMultiVariants() {
        // FIXME
    }

    @Override
    public void failMergeWithSameObj() {
        // FIXME
    }

    @Override
    public void multipleDanglingLinesInMergedNetwork() {
        // FIXME
    }

    @Override
    public void test() {
        // FIXME
    }

    @Override
    public void mergeThenCloneVariantBug() {
        // FIXME
    }

    @Override
    public void multipleDanglingLinesInMergingNetwork() {
        // FIXME
    }

    @Override
    public void checkMergingSameFormat() {
        // FIXME
    }

    @Override
    public void testMergeAndDetach() {
        // FIXME
    }

    @Override
    public void testMergeAndDetachWithExtensions() {
        // FIXME
    }

    @Override
    public void failDetachWithALineBetween2Subnetworks() {
        //FIXME
    }

    @Override
    public void failDetachIfMultiVariants() {
        //FIXME
    }

    @Override
    public void testMerge3Networks() {
        // FIXME
    }

    @Override
    public void failMergeDanglingLinesWithSameId() {
        // FIXME
    }

    @Override
    public void testValidationLevelWhenMerging2Eq() {
        // FIXME
    }

    @Override
    public void testValidationLevelWhenMergingEqAndSsh() {
        // FIXME
    }

    @Override
    public void testValidationLevelWhenMerging2Ssh() {
        // FIXME
    }

    @Override
    public void failMergeOnlyOneNetwork() {
        // FIXME
    }

    @Override
    public void failMergeOnSubnetworks() {
        // FIXME
    }

    @Override
    public void failMergeSubnetworks() {
        // FIXME
    }

    @Override
    public void failMergeContainingSubnetworks() {
        // FIXME
    }

    @Override
    public void testNoEmptyAdditionalSubnetworkIsCreated() {
        // FIXME
    }

    @Override
    public void testListeners() {
        // FIXME
    }

    @Override
    public void dontCreateATieLineWithAlreadyMergedDanglingLinesInMergedNetwork() {
        // FIXME
    }

    @Override
    public void dontCreateATieLineWithAlreadyMergedDanglingLinesInMergingNetwork() {
        // FIXME
    }

    @Override
    public void multipleConnectedDanglingLinesInMergedNetwork() {
        // FIXME
    }

    @Override
    public void multipleConnectedDanglingLinesWithSamePairingKey() {
        // FIXME
    }

    @Override
    public void invertDanglingLinesWhenCreatingATieLine() {
        // FIXME
    }

    @Override
    public void testMergeAndDetachWithProperties() {
        // FIXME
    }

}
