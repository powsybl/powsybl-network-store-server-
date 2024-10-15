/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.tck;

import com.powsybl.iidm.network.tck.AbstractMergeNetworkTest;
import com.powsybl.network.store.server.NetworkStoreApplication;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {"spring.config.location=classpath:application.yaml"})
@ContextHierarchy({@ContextConfiguration(classes = {NetworkStoreApplication.class})})
class MergeNetworkIT extends AbstractMergeNetworkTest {
    /* FIXME remove all these tests when network merge is implemented */

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void checkMergingDifferentFormat() {
        // FIXME
        super.checkMergingDifferentFormat();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void testMerge() {
        // FIXME
        super.testMerge();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void failMergeIfMultiVariants() {
        // FIXME
        super.failMergeIfMultiVariants();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void failMergeWithSameObj() {
        // FIXME
        super.failMergeWithSameObj();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void multipleDanglingLinesInMergedNetwork() {
        // FIXME
        super.multipleDanglingLinesInMergedNetwork();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void test() {
        // FIXME
        super.test();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void mergeThenCloneVariantBug() {
        // FIXME
        super.mergeThenCloneVariantBug();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void multipleDanglingLinesInMergingNetwork() {
        // FIXME
        super.multipleDanglingLinesInMergingNetwork();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void checkMergingSameFormat() {
        // FIXME
        super.checkMergingSameFormat();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void testMergeAndDetach() {
        // FIXME
        super.testMergeAndDetach();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void testMergeAndDetachWithExtensions() {
        // FIXME
        super.testMergeAndDetachWithExtensions();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void failDetachWithALineBetween2Subnetworks() {
        //FIXME
        super.failDetachWithALineBetween2Subnetworks();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void failDetachIfMultiVariants() {
        //FIXME
        super.failDetachIfMultiVariants();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void testMerge3Networks() {
        // FIXME
        super.testMerge3Networks();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void failMergeDanglingLinesWithSameId() {
        // FIXME
        super.failMergeDanglingLinesWithSameId();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void testValidationLevelWhenMerging2Eq() {
        // FIXME
        super.testValidationLevelWhenMerging2Eq();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void testValidationLevelWhenMergingEqAndSsh() {
        // FIXME
        super.testValidationLevelWhenMergingEqAndSsh();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void testValidationLevelWhenMerging2Ssh() {
        // FIXME
        super.testValidationLevelWhenMerging2Ssh();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void failMergeOnlyOneNetwork() {
        // FIXME
        super.failMergeOnlyOneNetwork();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void failMergeOnSubnetworks() {
        // FIXME
        super.failMergeOnSubnetworks();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void failMergeSubnetworks() {
        // FIXME
        super.failMergeSubnetworks();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void failMergeContainingSubnetworks() {
        // FIXME
        super.failMergeContainingSubnetworks();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void testNoEmptyAdditionalSubnetworkIsCreated() {
        // FIXME
        super.testNoEmptyAdditionalSubnetworkIsCreated();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void testListeners() {
        // FIXME
        super.testListeners();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void dontCreateATieLineWithAlreadyMergedDanglingLinesInMergedNetwork() {
        // FIXME
        super.dontCreateATieLineWithAlreadyMergedDanglingLinesInMergedNetwork();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void dontCreateATieLineWithAlreadyMergedDanglingLinesInMergingNetwork() {
        // FIXME
        super.dontCreateATieLineWithAlreadyMergedDanglingLinesInMergingNetwork();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void multipleConnectedDanglingLinesInMergedNetwork() {
        // FIXME
        super.multipleConnectedDanglingLinesInMergedNetwork();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void multipleConnectedDanglingLinesWithSamePairingKey() {
        // FIXME
        super.multipleConnectedDanglingLinesWithSamePairingKey();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void invertDanglingLinesWhenCreatingATieLine() {
        // FIXME
        super.invertDanglingLinesWhenCreatingATieLine();
    }

    @Disabled("network merge is implemented")
    @Test
    @Override
    public void testMergeAndDetachWithProperties() {
        // FIXME
        super.testMergeAndDetachWithProperties();
    }
}
