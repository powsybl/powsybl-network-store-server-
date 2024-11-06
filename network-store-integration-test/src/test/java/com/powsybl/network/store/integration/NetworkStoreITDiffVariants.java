/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.integration;

import com.powsybl.iidm.network.Generator;
import com.powsybl.iidm.network.Network;
import com.powsybl.iidm.network.test.EurostagTutorialExample1Factory;
import com.powsybl.network.store.client.NetworkStoreService;
import com.powsybl.network.store.model.VariantMode;
import com.powsybl.network.store.server.NetworkStoreApplication;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;

import java.util.UUID;

import static com.powsybl.iidm.network.VariantManagerConstants.INITIAL_VARIANT_ID;
import static com.powsybl.network.store.integration.TestUtils.createNetworkStoreService;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ContextHierarchy({@ContextConfiguration(classes = {NetworkStoreApplication.class, NetworkStoreService.class})})
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
class NetworkStoreITDiffVariants {
//    @DynamicPropertySource
//    private static void makeTestDbSuffix(DynamicPropertyRegistry registry) {
//        UUID uuid = UUID.randomUUID();
//        registry.add("testDbSuffix", () -> uuid);
//    }

    @LocalServerPort
    private int randomServerPort;

    @Test
    void testVariantsModifyOnce() {
        // import network on initial variant
        UUID networkUuid;
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.createWithMoreGenerators(service.getNetworkFactory());
            networkUuid = service.getNetworkUuid(network);
            assertEquals(2, network.getGeneratorCount());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.PARTIAL);

            // clone initial variant to variant "v"
            network.getVariantManager().cloneVariant(INITIAL_VARIANT_ID, "v");
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            network.getVariantManager().setWorkingVariant("v");
            assertEquals(2, network.getGeneratorCount());
            Generator gen = network.getGenerator("GEN");
            gen.setMaxP(15.5);
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            network.getVariantManager().setWorkingVariant("v");
            assertEquals(2, network.getGeneratorCount());
            assertEquals(15.5, network.getGenerator("GEN").getMaxP());
        }
    }

    @Test
    void testVariantsModifyTwice() {
        // import network on initial variant
        UUID networkUuid;
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.createWithMoreGenerators(service.getNetworkFactory());
            networkUuid = service.getNetworkUuid(network);
            assertEquals(2, network.getGeneratorCount());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.PARTIAL);

            // clone initial variant to variant "v"
            network.getVariantManager().cloneVariant(INITIAL_VARIANT_ID, "v");
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            network.getVariantManager().setWorkingVariant("v");
            assertEquals(2, network.getGeneratorCount());
            Generator gen = network.getGenerator("GEN");
            gen.setMaxP(15.5);
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            network.getVariantManager().setWorkingVariant("v");
            assertEquals(2, network.getGeneratorCount());
            assertEquals(15.5, network.getGenerator("GEN").getMaxP());
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            network.getVariantManager().setWorkingVariant("v");
            assertEquals(2, network.getGeneratorCount());
            Generator gen = network.getGenerator("GEN");
            gen.setMaxP(16.0);
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            network.getVariantManager().setWorkingVariant("v");
            assertEquals(2, network.getGeneratorCount());
            assertEquals(16.0, network.getGenerator("GEN").getMaxP());
        }
    }

    // add a test with 2 variants 0 => 1 => 2 should retrieve infos from 0 apply 1 and 2
    @Test
    void test2PartialVariantsModifyOnce() {
        // import network on initial variant
        UUID networkUuid;
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.createWithMoreGenerators(service.getNetworkFactory());
            networkUuid = service.getNetworkUuid(network);
            assertEquals(2, network.getGeneratorCount());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.PARTIAL);

            assertEquals(2, network.getGeneratorCount());
            Generator gen = network.getGenerator("GEN");
            gen.setMaxP(15.0);
            Generator gen2 = network.getGenerator("GEN2");
            gen2.setMaxP(14.0);

            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.PARTIAL);

            // clone initial to variant "v"
            network.getVariantManager().cloneVariant(INITIAL_VARIANT_ID, "v");
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.PARTIAL);
            network.getVariantManager().setWorkingVariant("v");

            assertEquals(2, network.getGeneratorCount());
            assertEquals(15.0, network.getGenerator("GEN").getMaxP());
            assertEquals(14.0, network.getGenerator("GEN2").getMaxP());
            Generator gen = network.getGenerator("GEN");
            gen.setMaxP(16.0);
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.PARTIAL);
            // clone "v" to variant "v1"
            network.getVariantManager().cloneVariant("v", "v1");
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.PARTIAL);
            network.getVariantManager().setWorkingVariant("v1");

            assertEquals(2, network.getGeneratorCount());
            assertEquals(16.0, network.getGenerator("GEN").getMaxP());
            assertEquals(14.0, network.getGenerator("GEN2").getMaxP());
            Generator gen = network.getGenerator("GEN2");
            gen.setMaxP(17.0);
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            // Initial variant id
            network.getVariantManager().setWorkingVariant(INITIAL_VARIANT_ID);
            assertEquals(2, network.getGeneratorCount());
            assertEquals(15.0, network.getGenerator("GEN").getMaxP());
            assertEquals(14.0, network.getGenerator("GEN2").getMaxP());

            // first variant
            network.getVariantManager().setWorkingVariant("v");
            assertEquals(2, network.getGeneratorCount());
            assertEquals(16.0, network.getGenerator("GEN").getMaxP());
            assertEquals(14.0, network.getGenerator("GEN2").getMaxP());

            // second variant
            network.getVariantManager().setWorkingVariant("v1");
            assertEquals(2, network.getGeneratorCount());
            assertEquals(16.0, network.getGenerator("GEN").getMaxP());
            assertEquals(17.0, network.getGenerator("GEN2").getMaxP());
            network.getVariantManager().cloneVariant("v1", "v2");
            service.flush(network);
        }
    }

    // add a test with a removed identifiable
    @Test
    void testPartialVariantDeleteIdentifiable() {
        // import network on initial variant
        UUID networkUuid;
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.createWithMoreGenerators(service.getNetworkFactory());
            networkUuid = service.getNetworkUuid(network);
            assertEquals(2, network.getGeneratorCount());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.PARTIAL);

            // clone initial to variant "v"
            network.getVariantManager().cloneVariant(INITIAL_VARIANT_ID, "v");
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);

            network.getVariantManager().setWorkingVariant("v");
            assertEquals(2, network.getGeneratorCount());
            assertEquals(4, network.getVoltageLevelCount()); // force collection loading because not yet implemented not collection get

            network.getGenerator("GEN").remove();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            // Initial variant id
            network.getVariantManager().setWorkingVariant(INITIAL_VARIANT_ID);
            assertEquals(2, network.getGeneratorCount());

            // first variant
            network.getVariantManager().setWorkingVariant("v");
            assertEquals(1, network.getGeneratorCount());
        }
    }

    // next test is to recreate the identifiable that was deleted and delete again
    @Test
    void testPartialVariantDeleteAndCreateAndDeleteAgainIdentifiable() {
        // import network on initial variant
        UUID networkUuid;
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.createWithMoreGenerators(service.getNetworkFactory());
            networkUuid = service.getNetworkUuid(network);
            assertEquals(2, network.getGeneratorCount());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.PARTIAL);

            // clone initial to variant "v"
            network.getVariantManager().cloneVariant(INITIAL_VARIANT_ID, "v");
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);

            network.getVariantManager().setWorkingVariant("v");
            assertEquals(2, network.getGeneratorCount());
            assertEquals(4, network.getVoltageLevelCount()); // force collection loading because not yet implemented not collection get

            network.getGenerator("GEN").remove();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            // Initial variant id
            network.getVariantManager().setWorkingVariant(INITIAL_VARIANT_ID);
            assertEquals(2, network.getGeneratorCount());

            // first variant
            network.getVariantManager().setWorkingVariant("v");
            assertEquals(1, network.getGeneratorCount());
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            network.getVariantManager().setWorkingVariant("v");
            network.getVoltageLevel("VLGEN").newGenerator()
                    .setId("GEN")
                    .setConnectableBus("NGEN")
                    .setMinP(0)
                    .setMaxP(10)
                    .setMinP(-9999.99)
                    .setMaxP(9999.99)
                    .setVoltageRegulatorOn(true)
                    .setTargetV(24.5)
                    .setTargetP(607.0)
                    .setTargetQ(301.0)
                    .add();
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            network.getVariantManager().setWorkingVariant("v");
            assertEquals(2, network.getGeneratorCount());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);

            network.getVariantManager().setWorkingVariant("v");
            assertEquals(2, network.getGeneratorCount());
            assertEquals(4, network.getVoltageLevelCount()); // force collection loading because not yet implemented not collection get

            network.getGenerator("GEN").remove();
            service.flush(network);
        }
    }

    //get identifiable shoudl also work
    @Test
    void testVariantsModifyOnceGetIdentifiable() {
        // import network on initial variant
        UUID networkUuid;
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.createWithMoreGenerators(service.getNetworkFactory());
            networkUuid = service.getNetworkUuid(network);
            assertEquals(2, network.getGeneratorCount());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.PARTIAL);

            // clone initial variant to variant "v"
            network.getVariantManager().cloneVariant(INITIAL_VARIANT_ID, "v");
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            network.getVariantManager().setWorkingVariant("v");
            Generator gen = network.getGenerator("GEN");
            gen.setMaxP(15.5);
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            network.getVariantManager().setWorkingVariant("v");
            assertEquals(15.5, network.getGenerator("GEN").getMaxP());
        }
    }

    @Test
    void testVariantsModifyOnceGetIdentifiableFULL() {
        // import network on initial variant
        UUID networkUuid;
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.createWithMoreGenerators(service.getNetworkFactory());
            networkUuid = service.getNetworkUuid(network);
            assertEquals(2, network.getGeneratorCount());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.FULL);

            // clone initial variant to variant "v"
            network.getVariantManager().cloneVariant(INITIAL_VARIANT_ID, "v");
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            network.getVariantManager().setWorkingVariant("v");
            Generator gen = network.getGenerator("GEN");
            gen.setMaxP(15.5);
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            network.getVariantManager().setWorkingVariant("v");
            assertEquals(15.5, network.getGenerator("GEN").getMaxP());
        }
    }

    @Test
    void test2PartialVariantsModifyOnceWithFull() {
        // import network on initial variant
        UUID networkUuid;
        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = EurostagTutorialExample1Factory.createWithMoreGenerators(service.getNetworkFactory());
            networkUuid = service.getNetworkUuid(network);
            assertEquals(2, network.getGeneratorCount());
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.PARTIAL);

            assertEquals(2, network.getGeneratorCount());
            Generator gen = network.getGenerator("GEN");
            gen.setMaxP(15.0);
            Generator gen2 = network.getGenerator("GEN2");
            gen2.setMaxP(14.0);

            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
//            service.setVariantMode(network, VariantMode.PARTIAL);

            // clone initial to variant "v"
            network.getVariantManager().cloneVariant(INITIAL_VARIANT_ID, "v");
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.PARTIAL);
            network.getVariantManager().setWorkingVariant("v");

            assertEquals(2, network.getGeneratorCount());
            assertEquals(15.0, network.getGenerator("GEN").getMaxP());
            assertEquals(14.0, network.getGenerator("GEN2").getMaxP());
            Generator gen = network.getGenerator("GEN");
            gen.setMaxP(16.0);
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.PARTIAL);
            // clone "v" to variant "v1"
            network.getVariantManager().cloneVariant("v", "v1");
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            assertNotNull(network);
            service.setVariantMode(network, VariantMode.PARTIAL);
            network.getVariantManager().setWorkingVariant("v1");

            assertEquals(2, network.getGeneratorCount());
            assertEquals(16.0, network.getGenerator("GEN").getMaxP());
            assertEquals(14.0, network.getGenerator("GEN2").getMaxP());
            Generator gen = network.getGenerator("GEN2");
            gen.setMaxP(17.0);
            service.flush(network);
        }

        try (NetworkStoreService service = createNetworkStoreService(randomServerPort)) {
            Network network = service.getNetwork(networkUuid);
            // Initial variant id
            network.getVariantManager().setWorkingVariant(INITIAL_VARIANT_ID);
            assertEquals(2, network.getGeneratorCount());
            assertEquals(15.0, network.getGenerator("GEN").getMaxP());
            assertEquals(14.0, network.getGenerator("GEN2").getMaxP());

            // first variant
            network.getVariantManager().setWorkingVariant("v");
            assertEquals(2, network.getGeneratorCount());
            assertEquals(16.0, network.getGenerator("GEN").getMaxP());
            assertEquals(14.0, network.getGenerator("GEN2").getMaxP());

            // second variant
            network.getVariantManager().setWorkingVariant("v1");
            assertEquals(2, network.getGeneratorCount());
            assertEquals(16.0, network.getGenerator("GEN").getMaxP());
            assertEquals(17.0, network.getGenerator("GEN2").getMaxP());
            network.getVariantManager().cloneVariant("v1", "v2");
            service.flush(network);
        }
    }
}
