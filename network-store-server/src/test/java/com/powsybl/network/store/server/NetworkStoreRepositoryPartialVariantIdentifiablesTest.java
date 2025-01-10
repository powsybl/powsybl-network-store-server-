/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server;

import com.powsybl.commons.PowsyblException;
import com.powsybl.iidm.network.VariantManagerConstants;
import com.powsybl.network.store.model.*;
import com.powsybl.network.store.server.exceptions.UncheckedSqlException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiConsumer;

import static com.powsybl.network.store.server.Mappings.*;
import static com.powsybl.network.store.server.QueryCatalog.*;
import static com.powsybl.network.store.server.utils.PartialVariantTestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Antoine Bouhours <antoine.bouhours at rte-france.com>
 */
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
class NetworkStoreRepositoryPartialVariantIdentifiablesTest {

    @Autowired
    private Mappings mappings;

    @Autowired
    private NetworkStoreRepository networkStoreRepository;

    @Autowired
    private DataSource dataSource;

    @DynamicPropertySource
    static void makeTestDbSuffix(DynamicPropertyRegistry registry) {
        UUID uuid = UUID.randomUUID();
        registry.add("testDbSuffix", () -> uuid);
    }

    private static final UUID NETWORK_UUID = UUID.fromString("7928181c-7977-4592-ba19-88027e4254e4");

    @Test
    void getNetworkContainsCloneStrategyFullVariantNum() {
        String networkId = "network1";
        int variantNum = 12;
        String variantId = VariantManagerConstants.INITIAL_VARIANT_ID;
        CloneStrategy cloneStrategy = CloneStrategy.FULL;
        int fullVariantNum = 8;
        Resource<NetworkAttributes> network1 = Resource.networkBuilder()
                .id(networkId)
                .variantNum(variantNum)
                .attributes(NetworkAttributes.builder()
                        .uuid(NETWORK_UUID)
                        .variantId(variantId)
                        .cloneStrategy(cloneStrategy)
                        .fullVariantNum(fullVariantNum)
                        .build())
                .build();
        networkStoreRepository.createNetworks(List.of(network1));

        Optional<Resource<NetworkAttributes>> networkAttributesOpt = networkStoreRepository.getNetwork(NETWORK_UUID, variantNum);

        assertTrue(networkAttributesOpt.isPresent());
        Resource<NetworkAttributes> networkAttributes = networkAttributesOpt.get();
        assertEquals(networkId, networkAttributes.getId());
        assertEquals(variantNum, networkAttributes.getVariantNum());
        assertEquals(NETWORK_UUID, networkAttributes.getAttributes().getUuid());
        assertEquals(variantId, networkAttributes.getAttributes().getVariantId());
        assertEquals(cloneStrategy, networkAttributes.getAttributes().getCloneStrategy());
        assertEquals(fullVariantNum, networkAttributes.getAttributes().getFullVariantNum());
    }

    @Test
    void updateNetworkUpdatesCloneStrategyFullVariantNum() {
        String networkId = "network1";
        int variantNum = 3;
        String variantId = VariantManagerConstants.INITIAL_VARIANT_ID;
        Resource<NetworkAttributes> network1 = Resource.networkBuilder()
                .id(networkId)
                .variantNum(variantNum)
                .attributes(NetworkAttributes.builder()
                        .uuid(NETWORK_UUID)
                        .variantId(variantId)
                        .cloneStrategy(CloneStrategy.PARTIAL)
                        .fullVariantNum(-1)
                        .build())
                .build();
        networkStoreRepository.createNetworks(List.of(network1));

        CloneStrategy cloneStrategy = CloneStrategy.FULL;
        int fullVariantNum = 2;
        network1 = Resource.networkBuilder()
                .id(networkId)
                .variantNum(variantNum)
                .attributes(NetworkAttributes.builder()
                        .uuid(NETWORK_UUID)
                        .variantId(variantId)
                        .cloneStrategy(cloneStrategy)
                        .fullVariantNum(fullVariantNum)
                        .build())
                .build();
        networkStoreRepository.updateNetworks(List.of(network1));

        Optional<Resource<NetworkAttributes>> networkAttributesOpt = networkStoreRepository.getNetwork(NETWORK_UUID, variantNum);
        assertTrue(networkAttributesOpt.isPresent());
        Resource<NetworkAttributes> networkAttributes = networkAttributesOpt.get();
        assertEquals(networkId, networkAttributes.getId());
        assertEquals(variantNum, networkAttributes.getVariantNum());
        assertEquals(NETWORK_UUID, networkAttributes.getAttributes().getUuid());
        assertEquals(variantId, networkAttributes.getAttributes().getVariantId());
        assertEquals(cloneStrategy, networkAttributes.getAttributes().getCloneStrategy());
        assertEquals(fullVariantNum, networkAttributes.getAttributes().getFullVariantNum());
    }

    @Test
    void cloneAllVariantsOfNetwork() {
        String networkId = "network1";
        String loadId1 = "load1";
        String lineId1 = "line1";
        String loadId2 = "load2";
        String lineId2 = "line2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 0, loadId1, lineId1, "vl1", "vl2");
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 1, loadId2, lineId2, "vl1", "vl2");
        UUID targetNetworkUuid = UUID.fromString("0dd45074-009d-49b8-877f-8ae648a8e8b4");

        networkStoreRepository.cloneNetwork(targetNetworkUuid, NETWORK_UUID, List.of("variant0", "variant1"));

        // Check variant 0
        Optional<Resource<NetworkAttributes>> networkAttributesOptVariant0 = networkStoreRepository.getNetwork(targetNetworkUuid, 0);
        assertTrue(networkAttributesOptVariant0.isPresent());
        Resource<NetworkAttributes> networkAttributesVariant0 = networkAttributesOptVariant0.get();
        assertEquals(networkId, networkAttributesVariant0.getId());
        assertEquals(0, networkAttributesVariant0.getVariantNum());
        assertEquals(targetNetworkUuid, networkAttributesVariant0.getAttributes().getUuid());
        assertEquals("variant0", networkAttributesVariant0.getAttributes().getVariantId());
        assertEquals(CloneStrategy.PARTIAL, networkAttributesVariant0.getAttributes().getCloneStrategy());
        assertEquals(-1, networkAttributesVariant0.getAttributes().getFullVariantNum());
        assertEquals(List.of(loadId1, lineId1), getIdentifiableIdsForVariant(targetNetworkUuid, 0));

        // Check variant 1
        Optional<Resource<NetworkAttributes>> networkAttributesOptVariant1 = networkStoreRepository.getNetwork(targetNetworkUuid, 1);
        assertTrue(networkAttributesOptVariant1.isPresent());
        Resource<NetworkAttributes> networkAttributesVariant1 = networkAttributesOptVariant1.get();
        assertEquals(networkId, networkAttributesVariant1.getId());
        assertEquals(1, networkAttributesVariant1.getVariantNum());
        assertEquals(targetNetworkUuid, networkAttributesVariant1.getAttributes().getUuid());
        assertEquals("variant1", networkAttributesVariant1.getAttributes().getVariantId());
        assertEquals(CloneStrategy.PARTIAL, networkAttributesVariant1.getAttributes().getCloneStrategy());
        assertEquals(0, networkAttributesVariant1.getAttributes().getFullVariantNum());
        assertEquals(List.of(loadId2, lineId2), getIdentifiableIdsForVariant(targetNetworkUuid, 1));
    }

    @Test
    void cloneAllVariantsOfNetworkWithTombstonedIdentifiable() {
        String networkId = "network1";
        String loadId1 = "load1";
        String lineId1 = "line1";
        String loadId2 = "load2";
        String lineId2 = "line2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 0, loadId1, lineId1, "vl1", "vl2");
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 1, loadId2, lineId2, "vl1", "vl2");
        networkStoreRepository.deleteIdentifiables(NETWORK_UUID, 0, Collections.singletonList(lineId1), LINE_TABLE);
        networkStoreRepository.deleteIdentifiables(NETWORK_UUID, 1, Collections.singletonList(loadId1), LOAD_TABLE);
        networkStoreRepository.deleteIdentifiables(NETWORK_UUID, 1, Collections.singletonList(loadId2), LOAD_TABLE);
        UUID targetNetworkUuid = UUID.fromString("0dd45074-009d-49b8-877f-8ae648a8e8b4");

        networkStoreRepository.cloneNetwork(targetNetworkUuid, NETWORK_UUID, List.of("variant0", "variant1"));

        assertEquals(List.of(loadId1), getIdentifiableIdsForVariant(targetNetworkUuid, 0));
        assertEquals(List.of(lineId2), getIdentifiableIdsForVariant(targetNetworkUuid, 1));
        assertEquals(Set.of(loadId1, loadId2), getTombstonedIdentifiableIds(targetNetworkUuid, 1));
    }

    @Test
    void clonePartialVariantInPartialMode() {
        String networkId = "network1";
        String loadId2 = "load2";
        String lineId2 = "line2";
        createNetwork(networkStoreRepository, NETWORK_UUID, networkId, 1, "variant1", CloneStrategy.PARTIAL, 0);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 1, loadId2, lineId2, "vl1", "vl2");
        networkStoreRepository.deleteIdentifiables(NETWORK_UUID, 1, Collections.singletonList(loadId2), LOAD_TABLE);

        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 1, 2, "variant2", CloneStrategy.PARTIAL);

        assertEquals(List.of(lineId2), getIdentifiableIdsForVariant(NETWORK_UUID, 2));
        assertEquals(Set.of(loadId2), getTombstonedIdentifiableIds(NETWORK_UUID, 2));

        // Check that tombstoned identifiables are cleaned when removing the network
        networkStoreRepository.deleteNetwork(NETWORK_UUID, 2);
        assertEquals(List.of(), getIdentifiableIdsForVariant(NETWORK_UUID, 2));
        assertEquals(Set.of(), getTombstonedIdentifiableIds(NETWORK_UUID, 2));
    }

    @Test
    void cloneFullVariantInPartialMode() {
        String networkId = "network1";
        String loadId1 = "load1";
        String lineId1 = "line1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 0, loadId1, lineId1, "vl1", "vl2");
        networkStoreRepository.deleteIdentifiables(NETWORK_UUID, 0, Collections.singletonList(loadId1), LOAD_TABLE);

        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);

        assertTrue(getIdentifiableIdsForVariant(NETWORK_UUID, 1).isEmpty());
        assertTrue(getTombstonedIdentifiableIds(NETWORK_UUID, 1).isEmpty());
    }

    @Test
    void cloneFullVariantInFullMode() {
        String networkId = "network1";
        String loadId1 = "load1";
        String lineId1 = "line1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.FULL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 0, loadId1, lineId1, "vl1", "vl2");

        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.FULL);

        assertEquals(List.of(loadId1, lineId1), getIdentifiableIdsForVariant(NETWORK_UUID, 1));
    }

    @Test
    void clonePartialVariantInFullModeShouldThrow() {
        String networkId = "network1";
        String loadId1 = "load1";
        String lineId1 = "line1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 0, loadId1, lineId1, "vl1", "vl2");
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        Exception exception = assertThrows(PowsyblException.class, () -> networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 1, 2, "variant2", CloneStrategy.FULL));
        assertEquals("Not implemented", exception.getMessage());
    }

    @Test
    void getIdentifiableWithoutNetwork() {
        List<Runnable> getIdentifiableRunnables = List.of(
                () -> networkStoreRepository.getIdentifiablesIds(NETWORK_UUID, 0),
                () -> networkStoreRepository.getIdentifiable(NETWORK_UUID, 0, "unknownId"),
                () -> networkStoreRepository.getLoad(NETWORK_UUID, 0, "unknownId"),
                () -> networkStoreRepository.getVoltageLevelLoads(NETWORK_UUID, 0, "unknownId"),
                () -> networkStoreRepository.getLoads(NETWORK_UUID, 0)
        );

        getIdentifiableRunnables.forEach(getIdentifiableRunnable -> {
            PowsyblException exception = assertThrows(PowsyblException.class, getIdentifiableRunnable::run);
            assertTrue(exception.getMessage().contains("Cannot retrieve source network attributes"));
        });
    }

    @Test
    void getIdentifiableFromPartialCloneWithNoIdentifiableInPartialVariant() {
        String networkId = "network1";
        String loadId1 = "load1";
        String lineId1 = "line1";
        String loadId2 = "load2";
        String lineId2 = "line2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 0, loadId1, lineId1, "vl1", "vl2");
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 0, loadId2, lineId2, "vl3", "vl4");
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);

        List<String> identifiablesIds = networkStoreRepository.getIdentifiablesIds(NETWORK_UUID, 1);
        assertEquals(List.of(loadId1, loadId2, lineId1, lineId2), identifiablesIds);

        // Variant num of load1 must be 1
        Resource<LoadAttributes> expLoad1 = buildLoad(loadId1, 1, "vl1");
        assertTrue(networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, "unknown").isEmpty());
        assertEquals(Optional.of(expLoad1), networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, loadId1));
        assertTrue(networkStoreRepository.getLoad(NETWORK_UUID, 1, "unknown").isEmpty());
        assertEquals(Optional.of(expLoad1), networkStoreRepository.getLoad(NETWORK_UUID, 1, loadId1));

        assertTrue(networkStoreRepository.getVoltageLevelLoads(NETWORK_UUID, 1, "unknown").isEmpty());
        assertEquals(List.of(expLoad1), networkStoreRepository.getVoltageLevelLoads(NETWORK_UUID, 1, "vl1"));

        Resource<LoadAttributes> expLoad2 = buildLoad(loadId2, 1, "vl3");
        assertEquals(List.of(expLoad1, expLoad2), networkStoreRepository.getLoads(NETWORK_UUID, 1));
    }

    @Test
    void getIdentifiableFromPartialClone() {
        String networkId = "network1";
        String loadId1 = "load1";
        String lineId1 = "line1";
        String loadId2 = "load2";
        String lineId2 = "line2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 0, loadId1, lineId1, "vl1", "vl2");
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 1, loadId2, lineId2, "vl1", "vl2");

        List<String> identifiablesIds = networkStoreRepository.getIdentifiablesIds(NETWORK_UUID, 1);
        assertEquals(List.of(loadId1, lineId1, loadId2, lineId2), identifiablesIds);

        Resource<LoadAttributes> expLoad1 = buildLoad(loadId1, 1, "vl1");
        Resource<LoadAttributes> expLoad2 = buildLoad(loadId2, 1, "vl1");
        assertEquals(Optional.of(expLoad2), networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, loadId2));
        assertEquals(Optional.of(expLoad2), networkStoreRepository.getLoad(NETWORK_UUID, 1, loadId2));

        assertEquals(List.of(expLoad1, expLoad2), networkStoreRepository.getVoltageLevelLoads(NETWORK_UUID, 1, "vl1"));

        assertEquals(List.of(expLoad1, expLoad2), networkStoreRepository.getLoads(NETWORK_UUID, 1));
    }

    @Test
    void getIdentifiableFromPartialCloneWithUpdatedIdentifiables() {
        String networkId = "network1";
        String loadId1 = "load1";
        String lineId1 = "line1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 0, loadId1, lineId1, "vl1", "vl2");
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 1, loadId1, lineId1, "vl2", "vl3");

        List<String> identifiablesIds = networkStoreRepository.getIdentifiablesIds(NETWORK_UUID, 1);
        assertEquals(List.of(loadId1, lineId1), identifiablesIds);

        Resource<LoadAttributes> expLoad = buildLoad(loadId1, 1, "vl2");
        assertEquals(Optional.of(expLoad), networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, loadId1));
        assertEquals(Optional.of(expLoad), networkStoreRepository.getLoad(NETWORK_UUID, 1, loadId1));

        assertTrue(networkStoreRepository.getVoltageLevelLoads(NETWORK_UUID, 1, "vl1").isEmpty());
        assertEquals(List.of(expLoad), networkStoreRepository.getVoltageLevelLoads(NETWORK_UUID, 1, "vl2"));

        assertEquals(List.of(expLoad), networkStoreRepository.getLoads(NETWORK_UUID, 1));
    }

    @Test
    void getIdentifiableFromFullClone() {
        String networkId = "network1";
        String loadId1 = "load1";
        String lineId1 = "line1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 2, "variant2", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 2, loadId1, lineId1, "vl1", "vl2");

        List<String> identifiablesIds = networkStoreRepository.getIdentifiablesIds(NETWORK_UUID, 2);
        assertEquals(List.of(loadId1, lineId1), identifiablesIds);

        Resource<LoadAttributes> expLoad = buildLoad(loadId1, 2, "vl1");
        assertEquals(Optional.of(expLoad), networkStoreRepository.getIdentifiable(NETWORK_UUID, 2, loadId1));
        assertEquals(Optional.of(expLoad), networkStoreRepository.getLoad(NETWORK_UUID, 2, loadId1));

        assertEquals(List.of(expLoad), networkStoreRepository.getVoltageLevelLoads(NETWORK_UUID, 2, "vl1"));

        assertEquals(List.of(expLoad), networkStoreRepository.getLoads(NETWORK_UUID, 2));
    }

    @Test
    void getIdentifiableFromPartialCloneWithTombstonedIdentifiable() {
        String networkId = "network1";
        String loadId1 = "load1";
        String lineId1 = "line1";
        String loadId2 = "load2";
        String lineId2 = "line2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 0, loadId1, lineId1, "vl1", "vl2");
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 1, loadId2, lineId2, "vl1", "vl2");
        networkStoreRepository.deleteIdentifiables(NETWORK_UUID, 0, Collections.singletonList(lineId1), LINE_TABLE);
        networkStoreRepository.deleteIdentifiables(NETWORK_UUID, 1, Collections.singletonList(loadId1), LOAD_TABLE);
        networkStoreRepository.deleteIdentifiables(NETWORK_UUID, 1, Collections.singletonList(lineId2), LINE_TABLE);

        List<String> identifiablesIds = networkStoreRepository.getIdentifiablesIds(NETWORK_UUID, 1);
        assertEquals(List.of(loadId2), identifiablesIds);

        Resource<LoadAttributes> expLoad = buildLoad(loadId2, 1, "vl1");
        assertTrue(networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, loadId1).isEmpty());
        assertEquals(Optional.of(expLoad), networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, loadId2));
        assertTrue(networkStoreRepository.getLoad(NETWORK_UUID, 1, loadId1).isEmpty());
        assertEquals(Optional.of(expLoad), networkStoreRepository.getLoad(NETWORK_UUID, 1, loadId2));

        assertEquals(List.of(expLoad), networkStoreRepository.getVoltageLevelLoads(NETWORK_UUID, 1, "vl1"));

        assertEquals(List.of(expLoad), networkStoreRepository.getLoads(NETWORK_UUID, 1));
    }

    @Test
    void getIdentifiableFromPartialCloneWithExternalAttributes() {
        String networkId = "network1";
        String lineId1 = "line1";
        String lineId2 = "line2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        Resource<LineAttributes> line1 = Resource.lineBuilder()
                .id(lineId1)
                .variantNum(0)
                .attributes(LineAttributes.builder()
                        .voltageLevelId1("vl1")
                        .voltageLevelId2("vl2")
                        .operationalLimitsGroups1(Map.of("group1", OperationalLimitsGroupAttributes.builder()
                                .id("group1")
                                .currentLimits(LimitsAttributes.builder().permanentLimit(30.).build())
                                .build()))
                        .build())
                .build();
        networkStoreRepository.createLines(NETWORK_UUID, List.of(line1));
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        Resource<LineAttributes> line2 = Resource.lineBuilder()
                .id(lineId2)
                .variantNum(1)
                .attributes(LineAttributes.builder()
                        .voltageLevelId1("vl1")
                        .voltageLevelId2("vl3")
                        .operationalLimitsGroups1(Map.of("group1", OperationalLimitsGroupAttributes.builder()
                                .id("group1")
                                .currentLimits(LimitsAttributes.builder().permanentLimit(20.).build())
                                .build()))
                        .build())
                .build();
        networkStoreRepository.createLines(NETWORK_UUID, List.of(line2));

        // Line1 is retrieved from variant 1 so variantNum must be 1
        Resource<LineAttributes> expLine1 = Resource.lineBuilder()
                .id(lineId1)
                .variantNum(1)
                .attributes(LineAttributes.builder()
                        .voltageLevelId1("vl1")
                        .voltageLevelId2("vl2")
                        .operationalLimitsGroups1(Map.of("group1", OperationalLimitsGroupAttributes.builder()
                                .id("group1")
                                .currentLimits(LimitsAttributes.builder().permanentLimit(30.).build())
                                .build()))
                        .build())
                .build();

        assertEquals(Optional.of(expLine1), networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, lineId1));
        assertEquals(Optional.of(line2), networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, lineId2));

        assertEquals(Optional.of(expLine1), networkStoreRepository.getLine(NETWORK_UUID, 1, lineId1));
        assertEquals(Optional.of(line2), networkStoreRepository.getLine(NETWORK_UUID, 1, lineId2));

        assertEquals(List.of(expLine1, line2), networkStoreRepository.getVoltageLevelLines(NETWORK_UUID, 1, "vl1"));

        assertEquals(List.of(expLine1, line2), networkStoreRepository.getLines(NETWORK_UUID, 1));
    }

    @Test
    void deleteIdentifiableWithoutNetwork() {
        String loadId1 = "load1";
        String lineId1 = "line1";
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 0, loadId1, lineId1, "vl1", "vl2");
        PowsyblException exception = assertThrows(PowsyblException.class, () -> networkStoreRepository.deleteIdentifiables(NETWORK_UUID, 0, Collections.singletonList(loadId1), LOAD_TABLE));
        assertTrue(exception.getMessage().contains("Cannot retrieve source network attributes"));
    }

    @Test
    void deleteIdentifiableOnFullVariant() {
        String networkId = "network1";
        String loadId1 = "load1";
        String lineId1 = "line1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 0, loadId1, lineId1, "vl1", "vl2");
        networkStoreRepository.deleteIdentifiables(NETWORK_UUID, 0, Collections.singletonList(loadId1), LOAD_TABLE);

        assertEquals(List.of(lineId1), getIdentifiableIdsForVariant(NETWORK_UUID, 0));
        assertTrue(getTombstonedIdentifiableIds(NETWORK_UUID, 0).isEmpty());
    }

    @Test
    void deleteIdentifiableOnPartialVariant() {
        String networkId = "network1";
        String loadId1 = "load1";
        String lineId1 = "line1";
        createNetwork(networkStoreRepository, NETWORK_UUID, networkId, 1, "variant1", CloneStrategy.PARTIAL, 0);
        createLineAndLoad(networkStoreRepository, NETWORK_UUID, 1, loadId1, lineId1, "vl1", "vl2");
        networkStoreRepository.deleteIdentifiables(NETWORK_UUID, 1, Collections.singletonList(loadId1), LOAD_TABLE);

        assertEquals(List.of(lineId1), getIdentifiableIdsForVariant(NETWORK_UUID, 1));
        assertEquals(Set.of(loadId1), getTombstonedIdentifiableIds(NETWORK_UUID, 1));

        // Delete an identifiable already deleted should throw
        assertThrows(UncheckedSqlException.class, () -> networkStoreRepository.deleteIdentifiables(NETWORK_UUID, 1, Collections.singletonList(loadId1), LOAD_TABLE));
    }

    @Test
    void createIdentifiablesInPartialVariant() {
        String networkId = "network1";
        createNetwork(networkStoreRepository, NETWORK_UUID, networkId, 1, "variant1", CloneStrategy.PARTIAL, 0);
        Resource<LineAttributes> line2 = createLine(networkStoreRepository, NETWORK_UUID, 1, "line2", "vl2", "vl3");

        assertEquals(List.of(line2), getIdentifiablesForVariant(NETWORK_UUID, 1, mappings.getLineMappings()));
        assertEquals(List.of("line2"), getIdentifiableIdsForVariant(NETWORK_UUID, 1));
    }

    @Test
    void createIdentifiablesWithRecreatedTombstonedIdentifiable() {
        String networkId = "network1";
        // Variant 0
        createNetwork(networkStoreRepository, NETWORK_UUID, networkId, 1, "variant1", CloneStrategy.PARTIAL, 0);
        String lineId1 = "line1";
        createLine(networkStoreRepository, NETWORK_UUID, 1, lineId1, "vl1", "vl2");
        networkStoreRepository.deleteIdentifiables(NETWORK_UUID, 1, Collections.singletonList(lineId1), LINE_TABLE);
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 1, 2, "variant1", CloneStrategy.PARTIAL);
        // Variant 2
        Resource<LineAttributes> lineVariant2 = createLine(networkStoreRepository, NETWORK_UUID, 2, lineId1, "vl2", "vl3");

        // Variant 1 (removed line1)
        assertTrue(networkStoreRepository.getLines(NETWORK_UUID, 1).isEmpty());
        assertTrue(getIdentifiablesForVariant(NETWORK_UUID, 1, mappings.getLineMappings()).isEmpty());
        assertTrue(getIdentifiableIdsForVariant(NETWORK_UUID, 1).isEmpty());
        assertEquals(Set.of(lineId1), getTombstonedIdentifiableIds(NETWORK_UUID, 1));
        // Variant 2 (recreated line1 with different attributes)
        assertEquals(Optional.of(lineVariant2), networkStoreRepository.getLine(NETWORK_UUID, 2, lineId1));
        assertEquals(List.of(), networkStoreRepository.getVoltageLevelLines(NETWORK_UUID, 2, "vl1"));
        assertEquals(List.of(lineVariant2), networkStoreRepository.getVoltageLevelLines(NETWORK_UUID, 2, "vl2"));
        assertEquals(List.of(lineVariant2), networkStoreRepository.getLines(NETWORK_UUID, 2));
        assertEquals(List.of(lineVariant2), getIdentifiablesForVariant(NETWORK_UUID, 2, mappings.getLineMappings()));
        assertEquals(List.of(lineId1), getIdentifiableIdsForVariant(NETWORK_UUID, 2));
        assertEquals(Set.of(lineId1), getTombstonedIdentifiableIds(NETWORK_UUID, 1));
    }

    @Test
    void updateIdentifiablesWithWhereClauseNotExistingInPartialVariant() {
        testUpdateIdentifiablesNotExistingInPartialVariant((networkId, resources) ->
                networkStoreRepository.updateIdentifiables(
                        networkId,
                        resources,
                        mappings.getLoadMappings(),
                        VOLTAGE_LEVEL_ID_COLUMN
                )
        );
    }

    @Test
    void updateIdentifiablesNotExistingInPartialVariant() {
        testUpdateIdentifiablesNotExistingInPartialVariant((networkId, resources) ->
                networkStoreRepository.updateIdentifiables(
                        networkId,
                        resources,
                        mappings.getLoadMappings()
                )
        );
    }

    private void testUpdateIdentifiablesNotExistingInPartialVariant(BiConsumer<UUID, List<Resource<LoadAttributes>>> updateMethod) {
        String networkId = "network1";
        String loadId = "load";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        Resource<LoadAttributes> initialLoad = Resource.loadBuilder()
                .id(loadId)
                .variantNum(0)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(5.1)
                        .q(6.1)
                        .build())
                .build();
        networkStoreRepository.createLoads(NETWORK_UUID, List.of(initialLoad));
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);

        Resource<LoadAttributes> updatedLoad = Resource.loadBuilder()
                .id(loadId)
                .variantNum(1)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(5.6)
                        .q(6.6)
                        .build())
                .build();
        updateMethod.accept(NETWORK_UUID, List.of(updatedLoad));

        assertEquals(Optional.of(updatedLoad), networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, loadId));
    }

    @Test
    void updateIdentifiablesSvNotExistingInPartialVariant() {
        String networkId = "network1";
        String loadId = "load";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        Resource<LoadAttributes> initialLoad = Resource.loadBuilder()
                .id(loadId)
                .variantNum(0)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(5.1)
                        .q(6.1)
                        .build())
                .build();
        networkStoreRepository.createLoads(NETWORK_UUID, List.of(initialLoad));
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);

        InjectionSvAttributes injectionSvAttributes = InjectionSvAttributes.builder()
                .p(5.6)
                .q(6.6)
                .build();
        Resource<InjectionSvAttributes> updatedSvLoad = new Resource<>(ResourceType.LOAD, loadId, 1, AttributeFilter.SV, injectionSvAttributes);
        networkStoreRepository.updateLoadsSv(NETWORK_UUID, List.of(updatedSvLoad));

        Resource<LoadAttributes> expUpdatedLoad = Resource.loadBuilder()
                .id(loadId)
                .variantNum(1)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(5.6)
                        .q(6.6)
                        .build())
                .build();
        assertEquals(Optional.of(expUpdatedLoad), networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, loadId));
    }

    @Test
    void updateIdentifiablesWithWhereClauseAlreadyExistingInPartialVariant() {
        testUpdateIdentifiablesAlreadyExistingInPartialVariant((networkId, resources) ->
                networkStoreRepository.updateIdentifiables(
                        networkId,
                        resources,
                        mappings.getLoadMappings(),
                        VOLTAGE_LEVEL_ID_COLUMN
                )
        );
    }

    @Test
    void updateIdentifiablesAlreadyExistingInPartialVariant() {
        testUpdateIdentifiablesAlreadyExistingInPartialVariant((networkId, resources) ->
                networkStoreRepository.updateIdentifiables(
                        networkId,
                        resources,
                        mappings.getLoadMappings()
                )
        );
    }

    private void testUpdateIdentifiablesAlreadyExistingInPartialVariant(BiConsumer<UUID, List<Resource<LoadAttributes>>> updateMethod) {
        String networkId = "network1";
        String loadId = "load";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        Resource<LoadAttributes> initialLoad = Resource.loadBuilder()
                .id(loadId)
                .variantNum(0)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(5.1)
                        .q(6.1)
                        .build())
                .build();
        networkStoreRepository.createLoads(NETWORK_UUID, List.of(initialLoad));
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        Resource<LoadAttributes> updatedLoad = Resource.loadBuilder()
                .id(loadId)
                .variantNum(1)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(5.6)
                        .q(6.6)
                        .build())
                .build();
        updateMethod.accept(NETWORK_UUID, List.of(updatedLoad));

        updatedLoad = Resource.loadBuilder()
                .id(loadId)
                .variantNum(1)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(8.1)
                        .q(5.9)
                        .build())
                .build();
        updateMethod.accept(NETWORK_UUID, List.of(updatedLoad));

        assertEquals(Optional.of(updatedLoad), networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, loadId));
    }

    @Test
    void updateIdentifiablesSvAlreadyExistingInPartialVariant() {
        String networkId = "network1";
        String loadId = "load";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        Resource<LoadAttributes> initialLoad = Resource.loadBuilder()
                .id(loadId)
                .variantNum(0)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(5.1)
                        .q(6.1)
                        .build())
                .build();
        networkStoreRepository.createLoads(NETWORK_UUID, List.of(initialLoad));
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        InjectionSvAttributes injectionSvAttributes = InjectionSvAttributes.builder()
                .p(5.6)
                .q(6.6)
                .build();
        Resource<InjectionSvAttributes> updatedSvLoad = new Resource<>(ResourceType.LOAD, loadId, 1, AttributeFilter.SV, injectionSvAttributes);
        networkStoreRepository.updateLoadsSv(NETWORK_UUID, List.of(updatedSvLoad));

        injectionSvAttributes = InjectionSvAttributes.builder()
                .p(8.1)
                .q(5.9)
                .build();
        updatedSvLoad = new Resource<>(ResourceType.LOAD, loadId, 1, AttributeFilter.SV, injectionSvAttributes);
        networkStoreRepository.updateLoadsSv(NETWORK_UUID, List.of(updatedSvLoad));

        Resource<LoadAttributes> expUpdatedSvLoad = Resource.loadBuilder()
                .id(loadId)
                .variantNum(1)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(8.1)
                        .q(5.9)
                        .build())
                .build();
        assertEquals(Optional.of(expUpdatedSvLoad), networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, loadId));
    }

    @Test
    void updateIdentifiablesWithWhereClauseNotExistingAndExistingInPartialVariant() {
        testUpdateIdentifiablesNotExistingAndExistingInPartialVariant((networkId, resources) ->
                networkStoreRepository.updateIdentifiables(
                        networkId,
                        resources,
                        mappings.getLoadMappings(),
                        VOLTAGE_LEVEL_ID_COLUMN
                )
        );
    }

    @Test
    void updateIdentifiablesNotExistingAndExistingInPartialVariant() {
        testUpdateIdentifiablesNotExistingAndExistingInPartialVariant((networkId, resources) ->
                networkStoreRepository.updateIdentifiables(
                        networkId,
                        resources,
                        mappings.getLoadMappings()
                )
        );
    }

    private void testUpdateIdentifiablesNotExistingAndExistingInPartialVariant(BiConsumer<UUID, List<Resource<LoadAttributes>>> updateMethod) {
        String networkId = "network1";
        String loadId1 = "load1";
        String loadId2 = "load2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        Resource<LoadAttributes> initialLoad1 = Resource.loadBuilder()
                .id(loadId1)
                .variantNum(0)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(5.1)
                        .q(6.1)
                        .build())
                .build();
        Resource<LoadAttributes> initialLoad2 = Resource.loadBuilder()
                .id(loadId2)
                .variantNum(0)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(7.1)
                        .q(3.1)
                        .build())
                .build();
        networkStoreRepository.createLoads(NETWORK_UUID, List.of(initialLoad1, initialLoad2));
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        Resource<LoadAttributes> updatedLoad2 = Resource.loadBuilder()
                .id(loadId2)
                .variantNum(1)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(5.4)
                        .q(6.6)
                        .build())
                .build();
        updateMethod.accept(NETWORK_UUID, List.of(updatedLoad2));

        Resource<LoadAttributes> updatedLoad1 = Resource.loadBuilder()
                .id(loadId1)
                .variantNum(1)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(5.9)
                        .q(6.4)
                        .build())
                .build();
        updatedLoad2 = Resource.loadBuilder()
                .id(loadId2)
                .variantNum(1)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(8.1)
                        .q(6.6)
                        .build())
                .build();
        updateMethod.accept(NETWORK_UUID, List.of(updatedLoad1, updatedLoad2));

        assertEquals(Optional.of(updatedLoad1), networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, loadId1));
        assertEquals(Optional.of(updatedLoad2), networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, loadId2));
    }

    @Test
    void updateIdentifiablesSvNotExistingAndExistingInPartialVariant() {
        String networkId = "network1";
        String loadId1 = "load1";
        String loadId2 = "load2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        Resource<LoadAttributes> initialLoad1 = Resource.loadBuilder()
                .id(loadId1)
                .variantNum(0)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(5.1)
                        .q(6.1)
                        .build())
                .build();
        Resource<LoadAttributes> initialLoad2 = Resource.loadBuilder()
                .id(loadId2)
                .variantNum(0)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(7.1)
                        .q(3.1)
                        .build())
                .build();
        networkStoreRepository.createLoads(NETWORK_UUID, List.of(initialLoad1, initialLoad2));
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        InjectionSvAttributes injectionSvAttributes = InjectionSvAttributes.builder()
                .p(5.6)
                .q(6.6)
                .build();
        Resource<InjectionSvAttributes> updatedSvLoad2 = new Resource<>(ResourceType.LOAD, loadId2, 1, AttributeFilter.SV, injectionSvAttributes);
        networkStoreRepository.updateLoadsSv(NETWORK_UUID, List.of(updatedSvLoad2));

        injectionSvAttributes = InjectionSvAttributes.builder()
                .p(2.1)
                .q(3.3)
                .build();
        Resource<InjectionSvAttributes> updatedSvLoad1 = new Resource<>(ResourceType.LOAD, loadId1, 1, AttributeFilter.SV, injectionSvAttributes);
        injectionSvAttributes = InjectionSvAttributes.builder()
                .p(8.1)
                .q(6.6)
                .build();
        updatedSvLoad2 = new Resource<>(ResourceType.LOAD, loadId2, 1, AttributeFilter.SV, injectionSvAttributes);
        networkStoreRepository.updateLoadsSv(NETWORK_UUID, List.of(updatedSvLoad1, updatedSvLoad2));

        Resource<LoadAttributes> expLoad1 = Resource.loadBuilder()
                .id(loadId1)
                .variantNum(1)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(2.1)
                        .q(3.3)
                        .build())
                .build();
        Resource<LoadAttributes> expLoad2 = Resource.loadBuilder()
                .id(loadId2)
                .variantNum(1)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId("vl1")
                        .p(8.1)
                        .q(6.6)
                        .build())
                .build();
        assertEquals(Optional.of(expLoad1), networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, loadId1));
        assertEquals(Optional.of(expLoad2), networkStoreRepository.getIdentifiable(NETWORK_UUID, 1, loadId2));
    }

    @Test
    void emptyCreateIdentifiablesDoesNotThrow() {
        assertDoesNotThrow(() -> networkStoreRepository.createIdentifiables(NETWORK_UUID, List.of(), mappings.getLoadMappings()));
    }

    private List<String> getIdentifiableIdsForVariant(UUID networkUuid, int variantNum) {
        try (var connection = dataSource.getConnection()) {
            return NetworkStoreRepository.getIdentifiablesIdsForVariant(connection, networkUuid, variantNum);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private List<Resource<IdentifiableAttributes>> getIdentifiablesForVariant(UUID networkUuid, int variantNum, TableMapping tableMapping) {
        try (var connection = dataSource.getConnection()) {
            return networkStoreRepository.getIdentifiablesForVariant(connection, networkUuid, variantNum, tableMapping, variantNum);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private Set<String> getTombstonedIdentifiableIds(UUID networkUuid, int variantNum) {
        try (var connection = dataSource.getConnection()) {
            return networkStoreRepository.getTombstonedIdentifiableIds(connection, networkUuid, variantNum);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }
}
