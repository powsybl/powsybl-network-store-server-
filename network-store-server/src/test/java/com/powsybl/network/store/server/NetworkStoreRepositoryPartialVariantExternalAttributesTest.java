/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server;

import com.powsybl.commons.PowsyblException;
import com.powsybl.iidm.network.LimitType;
import com.powsybl.iidm.network.extensions.ActivePowerControl;
import com.powsybl.iidm.network.extensions.OperatingStatus;
import com.powsybl.network.store.model.*;
import com.powsybl.network.store.server.dto.LimitsInfos;
import com.powsybl.network.store.server.dto.OwnerInfo;
import com.powsybl.network.store.server.dto.PermanentLimitAttributes;
import com.powsybl.network.store.server.exceptions.UncheckedSqlException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static com.powsybl.network.store.server.Mappings.*;
import static com.powsybl.network.store.server.QueryCatalog.EQUIPMENT_ID_COLUMN;
import static com.powsybl.network.store.server.QueryCatalog.REGULATING_EQUIPMENT_ID;
import static com.powsybl.network.store.server.utils.PartialVariantTestUtils.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Antoine Bouhours <antoine.bouhours at rte-france.com>
 */
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
class NetworkStoreRepositoryPartialVariantExternalAttributesTest {

    @Autowired
    private ExtensionHandler extensionHandler;

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
    void cloneAllVariantsOfNetworkWithExternalAttributes() {
        String networkId = "network1";
        String lineId1 = "line1";
        String genId1 = "gen1";
        String twoWTId1 = "twoWT1";
        String loadId1 = "load1";
        String lineId2 = "line2";
        String genId2 = "gen2";
        String twoWTId2 = "twoWT2";
        String loadId2 = "load2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createEquipmentsWithExternalAttributes(0, lineId1, genId1, twoWTId1, loadId1);
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        createEquipmentsWithExternalAttributes(1, lineId2, genId2, twoWTId2, loadId2);
        UUID targetNetworkUuid = UUID.fromString("0dd45074-009d-49b8-877f-8ae648a8e8b4");

        networkStoreRepository.cloneNetwork(targetNetworkUuid, NETWORK_UUID, List.of("variant0", "variant1"));

        // Check variant 0
        verifyExternalAttributes(lineId1, genId1, twoWTId1, 0, targetNetworkUuid);

        // Check variant 1
        verifyExternalAttributes(lineId2, genId2, twoWTId2, 1, targetNetworkUuid);
    }

    private void createEquipmentsWithExternalAttributes(int variantNum, String lineId, String generatorId, String twoWTId, String loadId) {
        createLine(networkStoreRepository, NETWORK_UUID, variantNum, lineId, "vl1", "vl2");
        createGeneratorAndLoadWithRegulatingAttributes(variantNum, generatorId, loadId, "vl1");
        createTwoWindingTransformer(variantNum, twoWTId, "vl1", "vl2");
        createExternalAttributes(variantNum, lineId, generatorId, twoWTId);
    }

    private void createExternalAttributes(int variantNum, String lineId, String generatorId, String twoWTId) {
        // Tap changer steps
        OwnerInfo ownerInfoLine = new OwnerInfo(lineId, ResourceType.LINE, NETWORK_UUID, variantNum);
        OwnerInfo ownerInfoGen = new OwnerInfo(generatorId, ResourceType.GENERATOR, NETWORK_UUID, variantNum);
        OwnerInfo ownerInfoTwoWT = new OwnerInfo(twoWTId, ResourceType.TWO_WINDINGS_TRANSFORMER, NETWORK_UUID, variantNum);
        TapChangerStepAttributes ratioStepA1 = buildTapChangerStepAttributes(1., 0);
        TapChangerStepAttributes ratioStepA2 = buildTapChangerStepAttributes(2., 1);
        networkStoreRepository.insertTapChangerSteps(Map.of(ownerInfoTwoWT, List.of(ratioStepA1, ratioStepA2)));
        // Temporary limits
        TemporaryLimitAttributes templimitA = TemporaryLimitAttributes.builder()
                .side(2)
                .acceptableDuration(100)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .build();
        TemporaryLimitAttributes templimitB = TemporaryLimitAttributes.builder()
                .side(2)
                .acceptableDuration(200)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .build();
        List<TemporaryLimitAttributes> temporaryLimitAttributes = List.of(templimitA, templimitB);
        // Permanent limits
        PermanentLimitAttributes permlimitA1 = PermanentLimitAttributes.builder()
                .side(1)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .value(2.5)
                .build();
        PermanentLimitAttributes permlimitA2 = PermanentLimitAttributes.builder()
                .side(2)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .value(2.6)
                .build();
        List<PermanentLimitAttributes> permanentLimitAttributes = List.of(permlimitA1, permlimitA2);
        LimitsInfos limitsInfos = new LimitsInfos();
        limitsInfos.setTemporaryLimits(temporaryLimitAttributes);
        limitsInfos.setPermanentLimits(permanentLimitAttributes);
        networkStoreRepository.insertTemporaryLimits(Map.of(ownerInfoLine, limitsInfos));
        networkStoreRepository.insertPermanentLimits(Map.of(ownerInfoLine, limitsInfos));
        // Reactive capability curve points
        ReactiveCapabilityCurvePointAttributes curvePointA = ReactiveCapabilityCurvePointAttributes.builder()
                .minQ(-100.)
                .maxQ(100.)
                .p(0.)
                .build();
        ReactiveCapabilityCurvePointAttributes curvePointB = ReactiveCapabilityCurvePointAttributes.builder()
                .minQ(10.)
                .maxQ(30.)
                .p(20.)
                .build();
        networkStoreRepository.insertReactiveCapabilityCurvePoints(Map.of(ownerInfoGen, List.of(curvePointA, curvePointB)));
        // Extensions
        Map<String, ExtensionAttributes> extensionAttributes = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(6.0).participate(true).participationFactor(1.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test12").build());
        insertExtensions(Map.of(ownerInfoLine, extensionAttributes));
    }

    private static TapChangerStepAttributes buildTapChangerStepAttributes(double value, int index) {
        return TapChangerStepAttributes.builder()
                .rho(value)
                .r(value)
                .g(value)
                .b(value)
                .x(value)
                .side(0)
                .index(index)
                .type(TapChangerType.RATIO)
                .build();
    }

    private Resource<TwoWindingsTransformerAttributes> createTwoWindingTransformer(int variantNum, String generatorId, String voltageLevel1, String voltageLevel2) {
        Resource<TwoWindingsTransformerAttributes> twoWT = Resource.twoWindingsTransformerBuilder()
                .id(generatorId)
                .variantNum(variantNum)
                .attributes(TwoWindingsTransformerAttributes.builder()
                        .voltageLevelId1(voltageLevel1)
                        .voltageLevelId1(voltageLevel2)
                        .build())
                .build();
        networkStoreRepository.createTwoWindingsTransformers(NETWORK_UUID, List.of(twoWT));
        return twoWT;
    }

    private void createGeneratorAndLoadWithRegulatingAttributes(int variantNum, String generatorId, String loadId, String voltageLevel) {
        Resource<GeneratorAttributes> gen = Resource.generatorBuilder()
                .id(generatorId)
                .variantNum(variantNum)
                .attributes(GeneratorAttributes.builder()
                        .voltageLevelId(voltageLevel)
                        .name(generatorId)
                        .regulatingPoint(RegulatingPointAttributes.builder()
                                .localTerminal(TerminalRefAttributes.builder().connectableId(generatorId).build())
                                .regulatedResourceType(ResourceType.GENERATOR)
                                .regulatingEquipmentId(generatorId)
                                .regulatingTerminal(TerminalRefAttributes.builder().connectableId(generatorId).build())
                                .build())
                        .build())
                .build();
        networkStoreRepository.createGenerators(NETWORK_UUID, List.of(gen));
        Resource<LoadAttributes> load1 = Resource.loadBuilder()
                .id(loadId)
                .variantNum(variantNum)
                .attributes(LoadAttributes.builder()
                        .voltageLevelId(voltageLevel)
                        .build())
                .build();
        networkStoreRepository.createLoads(NETWORK_UUID, List.of(load1));
    }

    private void verifyExternalAttributes(String lineId, String generatorId, String twoWTId, int variantNum, UUID networkUuid) {
        OwnerInfo ownerInfoLine = new OwnerInfo(lineId, ResourceType.LINE, networkUuid, variantNum);
        OwnerInfo ownerInfoGen = new OwnerInfo(generatorId, ResourceType.GENERATOR, networkUuid, variantNum);
        OwnerInfo ownerInfoTwoWT = new OwnerInfo(twoWTId, ResourceType.TWO_WINDINGS_TRANSFORMER, networkUuid, variantNum);

        // Tap Changer Steps
        List<TapChangerStepAttributes> tapChangerSteps = networkStoreRepository.getTapChangerSteps(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, twoWTId).get(ownerInfoTwoWT);
        assertEquals(2, tapChangerSteps.size());
        assertEquals(1.0, tapChangerSteps.get(0).getRho());
        assertEquals(2.0, tapChangerSteps.get(1).getRho());

        tapChangerSteps = networkStoreRepository.getTapChangerStepsWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, List.of(twoWTId)).get(ownerInfoTwoWT);
        assertEquals(2, tapChangerSteps.size());
        assertEquals(1.0, tapChangerSteps.get(0).getRho());
        assertEquals(2.0, tapChangerSteps.get(1).getRho());

        // Temporary Limits
        List<TemporaryLimitAttributes> temporaryLimits = networkStoreRepository.getTemporaryLimits(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, lineId).get(ownerInfoLine);
        assertEquals(2, temporaryLimits.size());
        assertEquals(100, temporaryLimits.get(0).getAcceptableDuration());
        assertEquals(200, temporaryLimits.get(1).getAcceptableDuration());

        temporaryLimits = networkStoreRepository.getTemporaryLimitsWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, List.of(lineId)).get(ownerInfoLine);
        assertEquals(2, temporaryLimits.size());
        assertEquals(100, temporaryLimits.get(0).getAcceptableDuration());
        assertEquals(200, temporaryLimits.get(1).getAcceptableDuration());

        // Permanent Limits
        List<PermanentLimitAttributes> permanentLimits = networkStoreRepository.getPermanentLimits(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, lineId).get(ownerInfoLine);
        assertEquals(2, permanentLimits.size());
        assertEquals(2.5, permanentLimits.get(0).getValue());
        assertEquals(2.6, permanentLimits.get(1).getValue());

        permanentLimits = networkStoreRepository.getPermanentLimitsWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, List.of(lineId)).get(ownerInfoLine);
        assertEquals(2, permanentLimits.size());
        assertEquals(2.5, permanentLimits.get(0).getValue());
        assertEquals(2.6, permanentLimits.get(1).getValue());

        // Reactive Capability Curve Points
        List<ReactiveCapabilityCurvePointAttributes> curvePoints = networkStoreRepository.getReactiveCapabilityCurvePoints(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, generatorId).get(ownerInfoGen);
        assertEquals(2, curvePoints.size());
        assertEquals(-100.0, curvePoints.get(0).getMinQ());
        assertEquals(30.0, curvePoints.get(1).getMaxQ());

        curvePoints = networkStoreRepository.getReactiveCapabilityCurvePointsWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, List.of(generatorId)).get(ownerInfoGen);
        assertEquals(2, curvePoints.size());
        assertEquals(-100.0, curvePoints.get(0).getMinQ());
        assertEquals(30.0, curvePoints.get(1).getMaxQ());

        // Regulating Points
        RegulatingPointAttributes regulatingPoint = networkStoreRepository.getRegulatingPoints(networkUuid, variantNum, ResourceType.GENERATOR).get(ownerInfoGen);
        assertNotNull(regulatingPoint);
        assertEquals(generatorId, regulatingPoint.getRegulatingEquipmentId());
        assertEquals(generatorId, regulatingPoint.getRegulatingTerminal().getConnectableId());
        assertEquals(generatorId, regulatingPoint.getLocalTerminal().getConnectableId());
        Map<String, ResourceType> regulatingEquipments = networkStoreRepository.getRegulatingEquipments(networkUuid, variantNum, ResourceType.GENERATOR).get(ownerInfoGen);
        assertEquals(1, regulatingEquipments.size());
        assertTrue(regulatingEquipments.containsKey(generatorId));
        assertEquals(ResourceType.GENERATOR, regulatingEquipments.get(generatorId));

        regulatingPoint = networkStoreRepository.getRegulatingPointsWithInClause(networkUuid, variantNum, REGULATING_EQUIPMENT_ID, List.of(generatorId), ResourceType.GENERATOR).get(ownerInfoGen);
        assertNotNull(regulatingPoint);
        assertEquals(generatorId, regulatingPoint.getRegulatingEquipmentId());
        assertEquals(generatorId, regulatingPoint.getRegulatingTerminal().getConnectableId());
        assertEquals(generatorId, regulatingPoint.getLocalTerminal().getConnectableId());
        regulatingEquipments = networkStoreRepository.getRegulatingEquipmentsWithInClause(networkUuid, variantNum, "regulatingterminalconnectableid", List.of(generatorId), ResourceType.GENERATOR).get(ownerInfoGen);
        assertEquals(1, regulatingEquipments.size());
        assertTrue(regulatingEquipments.containsKey(generatorId));
        assertEquals(ResourceType.GENERATOR, regulatingEquipments.get(generatorId));

        regulatingEquipments = networkStoreRepository.getRegulatingEquipmentsForIdentifiable(networkUuid, variantNum, generatorId, ResourceType.GENERATOR);
        assertEquals(1, regulatingEquipments.size());
        assertTrue(regulatingEquipments.containsKey(generatorId));
        assertEquals(ResourceType.GENERATOR, regulatingEquipments.get(generatorId));

        // Extensions
        Map<String, ExtensionAttributes> extensions = networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(networkUuid, variantNum, lineId);
        assertEquals(2, extensions.size());
        assertTrue(extensions.containsKey("activePowerControl"));
        assertTrue(extensions.containsKey("operatingStatus"));
        ActivePowerControlAttributes activePowerControl = (ActivePowerControlAttributes) extensions.get("activePowerControl");
        assertEquals(6.0, activePowerControl.getDroop());
        OperatingStatusAttributes operatingStatus = (OperatingStatusAttributes) extensions.get("operatingStatus");
        assertEquals("test12", operatingStatus.getOperatingStatus());
    }

    @Test
    void cloneAllVariantsOfNetworkWithTombstonedExtension() {
        String networkId = "network1";
        String lineId1 = "line1";
        String lineId2 = "line2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        OwnerInfo ownerInfo1 = new OwnerInfo(lineId1, ResourceType.LINE, NETWORK_UUID, 0);
        Map<String, ExtensionAttributes> extensionAttributesMap1 = buildExtensionAttributesMap(5.6, "status1");
        insertExtensions(Map.of(ownerInfo1, extensionAttributesMap1));
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        OwnerInfo ownerInfo2 = new OwnerInfo(lineId2, ResourceType.LINE, NETWORK_UUID, 1);
        Map<String, ExtensionAttributes> extensionAttributesMap2 = buildExtensionAttributesMap(8.9, "status2");
        insertExtensions(Map.of(ownerInfo2, extensionAttributesMap2));
        networkStoreRepository.removeExtensionAttributes(NETWORK_UUID, 0, lineId1, ActivePowerControl.NAME);
        networkStoreRepository.removeExtensionAttributes(NETWORK_UUID, 1, lineId1, OperatingStatus.NAME);
        networkStoreRepository.removeExtensionAttributes(NETWORK_UUID, 1, lineId2, ActivePowerControl.NAME);
        UUID targetNetworkUuid = UUID.fromString("0dd45074-009d-49b8-877f-8ae648a8e8b4");

        networkStoreRepository.cloneNetwork(targetNetworkUuid, NETWORK_UUID, List.of("variant0", "variant1"));

        assertEquals(Map.of(lineId1, Set.of(OperatingStatus.NAME), lineId2, Set.of(ActivePowerControl.NAME)), getTombstonedExtensions(targetNetworkUuid, 1));
    }

    private static Map<String, ExtensionAttributes> buildExtensionAttributesMap(double droop, String operatingStatus) {
        ExtensionAttributes activePowerControlAttributes = buildActivePowerControlAttributes(droop);
        ExtensionAttributes operatingStatusAttributes = buildOperatingStatusAttributes(operatingStatus);
        return Map.of(ActivePowerControl.NAME, activePowerControlAttributes, OperatingStatus.NAME, operatingStatusAttributes);
    }

    private static ExtensionAttributes buildOperatingStatusAttributes(String operatingStatus) {
        return OperatingStatusAttributes.builder()
                .operatingStatus(operatingStatus)
                .build();
    }

    private static ExtensionAttributes buildActivePowerControlAttributes(double droop) {
        return ActivePowerControlAttributes.builder()
                .droop(droop)
                .participate(false)
                .build();
    }

    @Test
    void clonePartialVariantInPartialModeWithExternalAttributes() {
        String networkId = "network1";
        String lineId1 = "line1";
        String genId1 = "gen1";
        String twoWTId1 = "twoWT1";
        String loadId1 = "load1";
        createNetwork(networkStoreRepository, NETWORK_UUID, networkId, 1, "variant1", CloneStrategy.PARTIAL, 0);
        createEquipmentsWithExternalAttributes(1, lineId1, genId1, twoWTId1, loadId1);

        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 1, 2, "variant2", CloneStrategy.PARTIAL);

        verifyExternalAttributes(lineId1, genId1, twoWTId1, 2, NETWORK_UUID);
    }

    @Test
    void cloneFullVariantInPartialModeWithExternalAttributes() {
        String networkId = "network1";
        String lineId1 = "line1";
        String genId1 = "gen1";
        String twoWTId1 = "twoWT1";
        String loadId1 = "load1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createEquipmentsWithExternalAttributes(0, lineId1, genId1, twoWTId1, loadId1);

        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);

        verifyEmptyExternalAttributesForVariant(lineId1, genId1, twoWTId1, 1, NETWORK_UUID);
    }

    private void verifyEmptyExternalAttributesForVariant(String lineId, String generatorId, String twoWTId, int variantNum, UUID networkUuid) {
        try (Connection connection = dataSource.getConnection()) {
            // Tap Changer Steps
            assertTrue(networkStoreRepository.getTapChangerStepsForVariant(connection, networkUuid, variantNum, EQUIPMENT_ID_COLUMN, twoWTId, variantNum).isEmpty());

            // Temporary Limits
            assertTrue(networkStoreRepository.getTemporaryLimitsForVariant(connection, networkUuid, variantNum, EQUIPMENT_ID_COLUMN, lineId, variantNum).isEmpty());

            // Permanent Limits
            assertTrue(networkStoreRepository.getPermanentLimitsForVariant(connection, networkUuid, variantNum, EQUIPMENT_ID_COLUMN, lineId, variantNum).isEmpty());

            // Reactive Capability Curve Points
            assertTrue(networkStoreRepository.getReactiveCapabilityCurvePointsForVariant(connection, networkUuid, variantNum, EQUIPMENT_ID_COLUMN, generatorId, variantNum).isEmpty());

            // Regulating Points
            OwnerInfo ownerInfo = new OwnerInfo(generatorId, ResourceType.GENERATOR, networkUuid, variantNum);
            assertNull(networkStoreRepository.getRegulatingPointsForVariant(connection, networkUuid, variantNum, ResourceType.GENERATOR, variantNum).get(ownerInfo));

            // Extensions
            assertTrue(extensionHandler.getAllExtensionsAttributesByIdentifiableIdForVariant(connection, networkUuid, variantNum, lineId).isEmpty());
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    @Test
    void cloneFullVariantInFullModeWithExternalAttributes() {
        String networkId = "network1";
        String lineId1 = "line1";
        String genId1 = "gen1";
        String twoWTId1 = "twoWT1";
        String loadId1 = "load1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.FULL);
        createEquipmentsWithExternalAttributes(1, lineId1, genId1, twoWTId1, loadId1);

        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.FULL);

        verifyExternalAttributes(lineId1, genId1, twoWTId1, 1, NETWORK_UUID);
    }

    @Test
    void getExternalAttributesWithoutNetwork() {
        List<Runnable> getExternalAttributesRunnables = List.of(
                () -> networkStoreRepository.getTapChangerSteps(NETWORK_UUID, 0, EQUIPMENT_ID_COLUMN, "unknownId"),
                () -> networkStoreRepository.getTapChangerStepsWithInClause(NETWORK_UUID, 0, EQUIPMENT_ID_COLUMN, List.of("unknownId")),
                () -> networkStoreRepository.getTemporaryLimits(NETWORK_UUID, 0, EQUIPMENT_ID_COLUMN, "unknownId"),
                () -> networkStoreRepository.getTemporaryLimitsWithInClause(NETWORK_UUID, 0, EQUIPMENT_ID_COLUMN, List.of("unknownId")),
                () -> networkStoreRepository.getPermanentLimits(NETWORK_UUID, 0, EQUIPMENT_ID_COLUMN, "unknownId"),
                () -> networkStoreRepository.getPermanentLimitsWithInClause(NETWORK_UUID, 0, EQUIPMENT_ID_COLUMN, List.of("unknownId")),
                () -> networkStoreRepository.getRegulatingPoints(NETWORK_UUID, 0, ResourceType.LINE),
                () -> networkStoreRepository.getRegulatingPointsWithInClause(NETWORK_UUID, 0, REGULATING_EQUIPMENT_ID, List.of("unknownId"), ResourceType.LINE),
                () -> networkStoreRepository.getRegulatingEquipments(NETWORK_UUID, 0, ResourceType.LINE),
                () -> networkStoreRepository.getRegulatingEquipmentsWithInClause(NETWORK_UUID, 0, REGULATING_EQUIPMENT_ID, List.of("unknownId"), ResourceType.LINE),
                () -> networkStoreRepository.getRegulatingEquipmentsForIdentifiable(NETWORK_UUID, 0, "unknownId", ResourceType.LINE),
                () -> networkStoreRepository.getReactiveCapabilityCurvePoints(NETWORK_UUID, 0, EQUIPMENT_ID_COLUMN, "unknownId"),
                () -> networkStoreRepository.getReactiveCapabilityCurvePointsWithInClause(NETWORK_UUID, 0, EQUIPMENT_ID_COLUMN, List.of("unknownId"))
        );

        getExternalAttributesRunnables.forEach(getExternalAttributesRunnable -> {
            PowsyblException exception = assertThrows(PowsyblException.class, getExternalAttributesRunnable::run);
            assertTrue(exception.getMessage().contains("Cannot retrieve source network attributes"));
        });
    }

    @Test
    void getExternalAttributesFromPartialCloneWithNoExternalAttributesInPartialVariant() {
        String networkId = "network1";
        String lineId = "line1";
        String genId = "gen1";
        String twoWTId = "twoWT1";
        String loadId = "load1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createEquipmentsWithExternalAttributes(0, lineId, genId, twoWTId, loadId);
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);

        verifyExternalAttributes(lineId, genId, twoWTId, 1, NETWORK_UUID);
    }

    @Test
    void getExternalAttributesFromPartialClone() {
        String networkId = "network1";
        String lineId = "line1";
        String genId = "gen1";
        String twoWTId = "twoWT1";
        String loadId = "load1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        createEquipmentsWithExternalAttributes(1, lineId, genId, twoWTId, loadId);

        verifyExternalAttributes(lineId, genId, twoWTId, 1, NETWORK_UUID);
    }

    @Test
    void getExternalAttributesFromPartialCloneWithUpdatedExternalAttributes() {
        String networkId = "network1";
        String lineId = "line";
        String genId = "gen1";
        String twoWTId = "twoWT1";
        String loadId = "load1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createEquipmentsWithExternalAttributes(0, lineId, genId, twoWTId, loadId);
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        updateExternalAttributes(1, lineId, genId, twoWTId, loadId);

        verifyUpdatedExternalAttributes(lineId, genId, twoWTId, loadId, 1, NETWORK_UUID);
    }

    private void updateExternalAttributes(int variantNum, String lineId, String generatorId, String twoWTId, String loadId) {
        // Equipements are updated too
        createLine(networkStoreRepository, NETWORK_UUID, variantNum, lineId, "vl1", "vl2");
        Resource<GeneratorAttributes> updatedGen = Resource.generatorBuilder()
                .id(generatorId)
                .variantNum(variantNum)
                .attributes(GeneratorAttributes.builder()
                        .voltageLevelId("vl1")
                        .name(generatorId)
                        .regulatingPoint(RegulatingPointAttributes.builder()
                                .localTerminal(TerminalRefAttributes.builder().connectableId(generatorId).build())
                                .regulatingEquipmentId(generatorId)
                                .regulatingTerminal(TerminalRefAttributes.builder().connectableId(loadId).build())
                                .regulatedResourceType(ResourceType.LOAD)
                                .build())
                        .build())
                .build();
        networkStoreRepository.updateGenerators(NETWORK_UUID, List.of(updatedGen));
        createTwoWindingTransformer(variantNum, twoWTId, "vl1", "vl2");
        // Tap changer steps
        OwnerInfo ownerInfoLine = new OwnerInfo(lineId, ResourceType.LINE, NETWORK_UUID, variantNum);
        OwnerInfo ownerInfoGen = new OwnerInfo(generatorId, ResourceType.GENERATOR, NETWORK_UUID, variantNum);
        OwnerInfo ownerInfoTwoWT = new OwnerInfo(twoWTId, ResourceType.TWO_WINDINGS_TRANSFORMER, NETWORK_UUID, variantNum);
        TapChangerStepAttributes ratioStepA1 = buildTapChangerStepAttributes(3., 0);
        networkStoreRepository.insertTapChangerSteps(Map.of(ownerInfoTwoWT, List.of(ratioStepA1)));
        // Temporary limits
        TemporaryLimitAttributes templimitA = TemporaryLimitAttributes.builder()
                .side(2)
                .acceptableDuration(101)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .build();
        List<TemporaryLimitAttributes> temporaryLimitAttributes = List.of(templimitA);
        // Permanent limits
        PermanentLimitAttributes permlimitA1 = PermanentLimitAttributes.builder()
                .side(1)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .value(2.8)
                .build();
        List<PermanentLimitAttributes> permanentLimitAttributes = List.of(permlimitA1);
        LimitsInfos limitsInfos = new LimitsInfos();
        limitsInfos.setTemporaryLimits(temporaryLimitAttributes);
        limitsInfos.setPermanentLimits(permanentLimitAttributes);
        networkStoreRepository.insertTemporaryLimits(Map.of(ownerInfoLine, limitsInfos));
        networkStoreRepository.insertPermanentLimits(Map.of(ownerInfoLine, limitsInfos));
        // Reactive capability curve points
        ReactiveCapabilityCurvePointAttributes curvePointA = ReactiveCapabilityCurvePointAttributes.builder()
                .minQ(-120.)
                .maxQ(100.)
                .p(0.)
                .build();
        networkStoreRepository.insertReactiveCapabilityCurvePoints(Map.of(ownerInfoGen, List.of(curvePointA)));
        // Extensions
        Map<String, ExtensionAttributes> extensionAttributes = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(6.5).participate(true).participationFactor(1.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test123").build());
        insertExtensions(Map.of(ownerInfoLine, extensionAttributes));
    }

    private void verifyUpdatedExternalAttributes(String lineId, String generatorId, String twoWTId, String loadId, int variantNum, UUID networkUuid) {
        OwnerInfo ownerInfoLine = new OwnerInfo(lineId, ResourceType.LINE, NETWORK_UUID, variantNum);
        OwnerInfo ownerInfoGen = new OwnerInfo(generatorId, ResourceType.GENERATOR, NETWORK_UUID, variantNum);
        OwnerInfo ownerInfoTwoWT = new OwnerInfo(twoWTId, ResourceType.TWO_WINDINGS_TRANSFORMER, NETWORK_UUID, variantNum);
        OwnerInfo ownerInfoLoad = new OwnerInfo(loadId, ResourceType.LOAD, NETWORK_UUID, variantNum);

        // Tap Changer Steps
        List<TapChangerStepAttributes> tapChangerSteps = networkStoreRepository.getTapChangerSteps(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, twoWTId).get(ownerInfoTwoWT);
        assertEquals(1, tapChangerSteps.size());
        assertEquals(3.0, tapChangerSteps.get(0).getRho());

        tapChangerSteps = networkStoreRepository.getTapChangerStepsWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, List.of(twoWTId)).get(ownerInfoTwoWT);
        assertEquals(1, tapChangerSteps.size());
        assertEquals(3.0, tapChangerSteps.get(0).getRho());

        // Temporary Limits
        List<TemporaryLimitAttributes> temporaryLimits = networkStoreRepository.getTemporaryLimits(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, lineId).get(ownerInfoLine);
        assertEquals(1, temporaryLimits.size());
        assertEquals(101, temporaryLimits.get(0).getAcceptableDuration());

        temporaryLimits = networkStoreRepository.getTemporaryLimitsWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, List.of(lineId)).get(ownerInfoLine);
        assertEquals(1, temporaryLimits.size());
        assertEquals(101, temporaryLimits.get(0).getAcceptableDuration());

        // Permanent Limits
        List<PermanentLimitAttributes> permanentLimits = networkStoreRepository.getPermanentLimits(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, lineId).get(ownerInfoLine);
        assertEquals(1, permanentLimits.size());
        assertEquals(2.8, permanentLimits.get(0).getValue());

        permanentLimits = networkStoreRepository.getPermanentLimitsWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, List.of(lineId)).get(ownerInfoLine);
        assertEquals(1, permanentLimits.size());
        assertEquals(2.8, permanentLimits.get(0).getValue());

        // Reactive Capability Curve Points
        List<ReactiveCapabilityCurvePointAttributes> curvePoints = networkStoreRepository.getReactiveCapabilityCurvePoints(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, generatorId).get(ownerInfoGen);
        assertEquals(1, curvePoints.size());
        assertEquals(-120.0, curvePoints.get(0).getMinQ());

        curvePoints = networkStoreRepository.getReactiveCapabilityCurvePointsWithInClause(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, List.of(generatorId)).get(ownerInfoGen);
        assertEquals(1, curvePoints.size());
        assertEquals(-120.0, curvePoints.get(0).getMinQ());

        // Regulating Points
        RegulatingPointAttributes regulatingPoint = networkStoreRepository.getRegulatingPoints(networkUuid, variantNum, ResourceType.GENERATOR).get(ownerInfoGen);
        assertNotNull(regulatingPoint);
        assertEquals(generatorId, regulatingPoint.getRegulatingEquipmentId());
        assertEquals(generatorId, regulatingPoint.getLocalTerminal().getConnectableId());
        assertEquals(loadId, regulatingPoint.getRegulatingTerminal().getConnectableId());
        Map<OwnerInfo, Map<String, ResourceType>> regulatingEquipmentsGen = networkStoreRepository.getRegulatingEquipments(networkUuid, variantNum, ResourceType.GENERATOR);
        assertTrue(regulatingEquipmentsGen.isEmpty());
        Map<String, ResourceType> regulatingEquipmentsLoad = networkStoreRepository.getRegulatingEquipments(networkUuid, variantNum, ResourceType.LOAD).get(ownerInfoLoad);
        assertEquals(1, regulatingEquipmentsLoad.size());
        assertTrue(regulatingEquipmentsLoad.containsKey(generatorId));
        assertEquals(ResourceType.GENERATOR, regulatingEquipmentsLoad.get(generatorId));

        regulatingPoint = networkStoreRepository.getRegulatingPointsWithInClause(networkUuid, variantNum, REGULATING_EQUIPMENT_ID, List.of(generatorId), ResourceType.GENERATOR).get(ownerInfoGen);
        assertNotNull(regulatingPoint);
        assertEquals(generatorId, regulatingPoint.getRegulatingEquipmentId());
        assertEquals(generatorId, regulatingPoint.getLocalTerminal().getConnectableId());
        assertEquals(loadId, regulatingPoint.getRegulatingTerminal().getConnectableId());
        regulatingEquipmentsGen = networkStoreRepository.getRegulatingEquipmentsWithInClause(networkUuid, variantNum, "regulatingterminalconnectableid", List.of(generatorId), ResourceType.GENERATOR);
        assertTrue(regulatingEquipmentsGen.isEmpty());
        regulatingEquipmentsLoad = networkStoreRepository.getRegulatingEquipmentsWithInClause(networkUuid, variantNum, "regulatingterminalconnectableid", List.of(loadId), ResourceType.LOAD).get(ownerInfoLoad);
        assertEquals(1, regulatingEquipmentsLoad.size());
        assertTrue(regulatingEquipmentsLoad.containsKey(generatorId));
        assertEquals(ResourceType.GENERATOR, regulatingEquipmentsLoad.get(generatorId));

        assertTrue(networkStoreRepository.getRegulatingEquipmentsForIdentifiable(networkUuid, variantNum, generatorId, ResourceType.GENERATOR).isEmpty());
        regulatingEquipmentsLoad = networkStoreRepository.getRegulatingEquipmentsForIdentifiable(networkUuid, variantNum, loadId, ResourceType.LOAD);
        assertEquals(1, regulatingEquipmentsLoad.size());
        assertTrue(regulatingEquipmentsLoad.containsKey(generatorId));
        assertEquals(ResourceType.GENERATOR, regulatingEquipmentsLoad.get(generatorId));

        // Extensions
        Map<String, ExtensionAttributes> extensions = networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(networkUuid, variantNum, lineId);
        assertEquals(2, extensions.size());
        assertTrue(extensions.containsKey("activePowerControl"));
        assertTrue(extensions.containsKey("operatingStatus"));
        ActivePowerControlAttributes activePowerControl = (ActivePowerControlAttributes) extensions.get("activePowerControl");
        assertEquals(6.5, activePowerControl.getDroop());
        OperatingStatusAttributes operatingStatus = (OperatingStatusAttributes) extensions.get("operatingStatus");
        assertEquals("test123", operatingStatus.getOperatingStatus());
    }

    @Test
    void getExternalAttributesFromPartialCloneWithUpdatedSv() {
        String networkId = "network1";
        String lineId = "line";
        String genId = "gen1";
        String twoWTId = "twoWT1";
        String loadId = "load1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createEquipmentsWithExternalAttributes(0, lineId, genId, twoWTId, loadId);
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        createLine(networkStoreRepository, NETWORK_UUID, 1, lineId, "vl1", "vl2");

        verifyExternalAttributes(lineId, genId, twoWTId, 1, NETWORK_UUID);
    }

    @Test
    void getExternalAttributesFromPartialCloneWithUpdatedIdentifiableWithoutExternalAttributes() {
        String networkId = "network1";
        String lineId = "line";
        String genId = "gen1";
        String twoWTId = "twoWT1";
        String loadId = "load1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createEquipmentsWithExternalAttributes(0, lineId, genId, twoWTId, loadId);
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);

        verifyExternalAttributes(lineId, genId, twoWTId, 1, NETWORK_UUID);
        updateExternalAttributesWithTombstone(1, lineId, genId, twoWTId);
        OwnerInfo ownerInfoLine = new OwnerInfo(lineId, ResourceType.LINE, NETWORK_UUID, 1);
        OwnerInfo ownerInfoGen = new OwnerInfo(genId, ResourceType.GENERATOR, NETWORK_UUID, 1);
        OwnerInfo ownerInfoTwoWT = new OwnerInfo(twoWTId, ResourceType.TWO_WINDINGS_TRANSFORMER, NETWORK_UUID, 1);
        assertNull(networkStoreRepository.getTapChangerSteps(NETWORK_UUID, 1, EQUIPMENT_ID_COLUMN, twoWTId).get(ownerInfoTwoWT));
        assertNull(networkStoreRepository.getTemporaryLimits(NETWORK_UUID, 1, EQUIPMENT_ID_COLUMN, lineId).get(ownerInfoLine));
        assertNull(networkStoreRepository.getPermanentLimits(NETWORK_UUID, 1, EQUIPMENT_ID_COLUMN, lineId).get(ownerInfoLine));
        assertNull(networkStoreRepository.getReactiveCapabilityCurvePoints(NETWORK_UUID, 1, EQUIPMENT_ID_COLUMN, genId).get(ownerInfoGen));
        updateExternalAttributesWithTombstone(1, lineId, genId, twoWTId);
        assertNull(networkStoreRepository.getTapChangerSteps(NETWORK_UUID, 1, EQUIPMENT_ID_COLUMN, twoWTId).get(ownerInfoTwoWT));
        assertNull(networkStoreRepository.getTemporaryLimits(NETWORK_UUID, 1, EQUIPMENT_ID_COLUMN, lineId).get(ownerInfoLine));
        assertNull(networkStoreRepository.getPermanentLimits(NETWORK_UUID, 1, EQUIPMENT_ID_COLUMN, lineId).get(ownerInfoLine));
        assertNull(networkStoreRepository.getReactiveCapabilityCurvePoints(NETWORK_UUID, 1, EQUIPMENT_ID_COLUMN, genId).get(ownerInfoGen));
    }

    private void updateExternalAttributesWithTombstone(int variantNum, String lineId, String generatorId, String twoWTId) {
        Resource<GeneratorAttributes> generator = new Resource<>(ResourceType.GENERATOR, generatorId, variantNum, null, new GeneratorAttributes());
        Resource<TwoWindingsTransformerAttributes> twoWT = new Resource<>(ResourceType.TWO_WINDINGS_TRANSFORMER, twoWTId, variantNum, null, new TwoWindingsTransformerAttributes());
        Resource<LineAttributes> line = new Resource<>(ResourceType.LINE, lineId, variantNum, null, new LineAttributes());
        networkStoreRepository.updateTapChangerSteps(NETWORK_UUID, List.of(twoWT));
        networkStoreRepository.updateTemporaryLimits(NETWORK_UUID, List.of(line), networkStoreRepository.getLimitsInfosFromEquipments(NETWORK_UUID, List.of(line)));
        networkStoreRepository.updatePermanentLimits(NETWORK_UUID, List.of(line), networkStoreRepository.getLimitsInfosFromEquipments(NETWORK_UUID, List.of(line)));
        networkStoreRepository.updateReactiveCapabilityCurvePoints(NETWORK_UUID, List.of(generator));
        // Regulating points can't be tombstoned for now so they're not tested
    }

    @Test
    void getExternalAttributesFromPartialCloneWithUpdatedIdentifiableSv() {
        String networkId = "network1";
        String lineId = "line";
        String genId = "gen1";
        String twoWTId = "twoWT1";
        String loadId = "load1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createEquipmentsWithExternalAttributes(0, lineId, genId, twoWTId, loadId);
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);

        BranchSvAttributes branchSvAttributes = BranchSvAttributes.builder()
                .p1(5.6)
                .q1(6.6)
                .build();
        Resource<BranchSvAttributes> updatedSvLine = new Resource<>(ResourceType.LINE, lineId, 1, AttributeFilter.SV, branchSvAttributes);
        Resource<BranchSvAttributes> updatedSvTwoWT = new Resource<>(ResourceType.TWO_WINDINGS_TRANSFORMER, twoWTId, 1, AttributeFilter.SV, branchSvAttributes);
        InjectionSvAttributes injectionSvAttributes = InjectionSvAttributes.builder()
                .p(5.6)
                .q(6.6)
                .build();
        Resource<InjectionSvAttributes> updatedSvGen = new Resource<>(ResourceType.GENERATOR, genId, 1, AttributeFilter.SV, injectionSvAttributes);
        networkStoreRepository.updateLinesSv(NETWORK_UUID, List.of(updatedSvLine));
        networkStoreRepository.updateGeneratorsSv(NETWORK_UUID, List.of(updatedSvGen));
        networkStoreRepository.updateTwoWindingsTransformersSv(NETWORK_UUID, List.of(updatedSvTwoWT));
        verifyExternalAttributes(lineId, genId, twoWTId, 1, NETWORK_UUID);
    }

    @Test
    void getExternalAttributesFromFullClone() {
        String networkId = "network1";
        String lineId = "line";
        String genId = "gen1";
        String twoWTId = "twoWT1";
        String loadId = "load1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 2, "variant2", CloneStrategy.PARTIAL);
        createEquipmentsWithExternalAttributes(2, lineId, genId, twoWTId, loadId);

        verifyExternalAttributes(lineId, genId, twoWTId, 2, NETWORK_UUID);
    }

    @Test
    void getExternalAttributesFromPartialCloneWithTombstonedIdentifiable() {
        String networkId = "network1";
        String lineId = "line1";
        String genId = "gen1";
        String twoWTId = "twoWT1";
        String loadId = "load1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createEquipmentsWithExternalAttributes(0, lineId, genId, twoWTId, loadId);
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);

        verifyExternalAttributes(lineId, genId, twoWTId, 0, NETWORK_UUID);
        networkStoreRepository.deleteIdentifiable(NETWORK_UUID, 1, lineId, LINE_TABLE);
        networkStoreRepository.deleteIdentifiable(NETWORK_UUID, 1, twoWTId, TWO_WINDINGS_TRANSFORMER_TABLE);
        networkStoreRepository.deleteIdentifiable(NETWORK_UUID, 1, genId, GENERATOR_TABLE);
        verifyEmptyExternalAttributes(lineId, genId, twoWTId, 1, NETWORK_UUID);
    }

    private void verifyEmptyExternalAttributes(String lineId, String generatorId, String twoWTId, int variantNum, UUID networkUuid) {
        OwnerInfo ownerInfoLine = new OwnerInfo(lineId, ResourceType.LINE, networkUuid, variantNum);
        OwnerInfo ownerInfoGen = new OwnerInfo(generatorId, ResourceType.GENERATOR, networkUuid, variantNum);
        OwnerInfo ownerInfoTwoWT = new OwnerInfo(twoWTId, ResourceType.TWO_WINDINGS_TRANSFORMER, networkUuid, variantNum);

        // Tap Changer Steps
        assertNull(networkStoreRepository.getTapChangerSteps(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, twoWTId).get(ownerInfoTwoWT));

        // Temporary Limits
        assertNull(networkStoreRepository.getTemporaryLimits(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, lineId).get(ownerInfoLine));

        // Permanent Limits
        assertNull(networkStoreRepository.getPermanentLimits(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, lineId).get(ownerInfoLine));

        // Reactive Capability Curve Points
        assertNull(networkStoreRepository.getReactiveCapabilityCurvePoints(networkUuid, variantNum, EQUIPMENT_ID_COLUMN, generatorId).get(ownerInfoGen));

        // Regulating Points
        assertNull(networkStoreRepository.getRegulatingPoints(networkUuid, variantNum, ResourceType.GENERATOR).get(ownerInfoGen));
    }

    @Test
    void getExtensionWithoutNetwork() {
        List<Runnable> getExtensionRunnables = List.of(
                () -> networkStoreRepository.getExtensionAttributes(NETWORK_UUID, 0, EQUIPMENT_ID_COLUMN, "unknownExtension"),
                () -> networkStoreRepository.getAllExtensionsAttributesByResourceType(NETWORK_UUID, 0, ResourceType.LINE),
                () -> networkStoreRepository.getAllExtensionsAttributesByResourceTypeAndExtensionName(NETWORK_UUID, 0, ResourceType.LINE, "unknownExtension"),
                () -> networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 0, "unknownId")
        );

        getExtensionRunnables.forEach(getExtensionRunnable -> {
            PowsyblException exception = assertThrows(PowsyblException.class, getExtensionRunnable::run);
            assertTrue(exception.getMessage().contains("Cannot retrieve source network attributes"));
        });
    }

    @Test
    void getExtensionFromPartialCloneWithNoExtensionInPartialVariant() {
        String networkId = "network1";
        String lineId1 = "line1";
        String lineId2 = "line2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        OwnerInfo ownerInfo1 = new OwnerInfo(lineId1, ResourceType.LINE, NETWORK_UUID, 0);
        Map<String, ExtensionAttributes> extensionAttributesMap1 = buildExtensionAttributesMap(5.6, "status1");
        OwnerInfo ownerInfo2 = new OwnerInfo(lineId2, ResourceType.LINE, NETWORK_UUID, 0);
        Map<String, ExtensionAttributes> extensionAttributesMap2 = buildExtensionAttributesMap(8.9, "status2");
        insertExtensions(Map.of(ownerInfo1, extensionAttributesMap1, ownerInfo2, extensionAttributesMap2));
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);

        Assertions.assertEquals(Optional.of(extensionAttributesMap1.get(ActivePowerControl.NAME)), networkStoreRepository.getExtensionAttributes(NETWORK_UUID, 1, lineId1, ActivePowerControl.NAME));
        Assertions.assertEquals(extensionAttributesMap1, networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 1, lineId1));
        Map<String, ExtensionAttributes> expExtensionAttributesApcLine = Map.of(lineId1, buildActivePowerControlAttributes(5.6), lineId2, buildActivePowerControlAttributes(8.9));
        Assertions.assertEquals(expExtensionAttributesApcLine, networkStoreRepository.getAllExtensionsAttributesByResourceTypeAndExtensionName(NETWORK_UUID, 1, ResourceType.LINE, ActivePowerControl.NAME));
        Map<String, Map<String, ExtensionAttributes>> expExtensionAttributesLine = Map.of(lineId1, extensionAttributesMap1, lineId2, extensionAttributesMap2);
        Assertions.assertEquals(expExtensionAttributesLine, networkStoreRepository.getAllExtensionsAttributesByResourceType(NETWORK_UUID, 1, ResourceType.LINE));
    }

    @Test
    void getExtensionFromPartialClone() {
        String networkId = "network1";
        String lineId1 = "line1";
        String lineId2 = "line2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);
        OwnerInfo ownerInfo1 = new OwnerInfo(lineId1, ResourceType.LINE, NETWORK_UUID, 1);
        Map<String, ExtensionAttributes> extensionAttributesMap1 = buildExtensionAttributesMap(5.6, "status1");
        OwnerInfo ownerInfo2 = new OwnerInfo(lineId2, ResourceType.LINE, NETWORK_UUID, 1);
        Map<String, ExtensionAttributes> extensionAttributesMap2 = buildExtensionAttributesMap(8.9, "status2");
        insertExtensions(Map.of(ownerInfo1, extensionAttributesMap1, ownerInfo2, extensionAttributesMap2));

        Assertions.assertEquals(Optional.of(extensionAttributesMap1.get(ActivePowerControl.NAME)), networkStoreRepository.getExtensionAttributes(NETWORK_UUID, 1, lineId1, ActivePowerControl.NAME));
        Assertions.assertEquals(extensionAttributesMap1, networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 1, lineId1));
        Map<String, ExtensionAttributes> expExtensionAttributesApcLine = Map.of(lineId1, buildActivePowerControlAttributes(5.6), lineId2, buildActivePowerControlAttributes(8.9));
        Assertions.assertEquals(expExtensionAttributesApcLine, networkStoreRepository.getAllExtensionsAttributesByResourceTypeAndExtensionName(NETWORK_UUID, 1, ResourceType.LINE, ActivePowerControl.NAME));
        Map<String, Map<String, ExtensionAttributes>> expExtensionAttributesLine = Map.of(lineId1, extensionAttributesMap1, lineId2, extensionAttributesMap2);
        Assertions.assertEquals(expExtensionAttributesLine, networkStoreRepository.getAllExtensionsAttributesByResourceType(NETWORK_UUID, 1, ResourceType.LINE));
    }

    @Test
    void getExtensionFromPartialCloneWithUpdatedExtension() {
        String networkId = "network1";
        String lineId1 = "line1";
        String lineId2 = "line2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        OwnerInfo ownerInfo1 = new OwnerInfo(lineId1, ResourceType.LINE, NETWORK_UUID, 0);
        Map<String, ExtensionAttributes> extensionAttributesMap1 = buildExtensionAttributesMap(5.6, "status1");
        OwnerInfo ownerInfo2 = new OwnerInfo(lineId2, ResourceType.LINE, NETWORK_UUID, 0);
        Map<String, ExtensionAttributes> extensionAttributesMap2 = buildExtensionAttributesMap(8.9, "status2");
        insertExtensions(Map.of(ownerInfo1, extensionAttributesMap1, ownerInfo2, extensionAttributesMap2));
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);

        ownerInfo1 = new OwnerInfo(lineId1, ResourceType.LINE, NETWORK_UUID, 1);
        extensionAttributesMap1 = buildExtensionAttributesMap(5.2, "statusUpdated1");
        insertExtensions(Map.of(ownerInfo1, extensionAttributesMap1));

        Assertions.assertEquals(Optional.of(extensionAttributesMap1.get(ActivePowerControl.NAME)), networkStoreRepository.getExtensionAttributes(NETWORK_UUID, 1, lineId1, ActivePowerControl.NAME));
        Assertions.assertEquals(extensionAttributesMap1, networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 1, lineId1));
        Map<String, ExtensionAttributes> expExtensionAttributesApcLine = Map.of(lineId1, buildActivePowerControlAttributes(5.2), lineId2, buildActivePowerControlAttributes(8.9));
        Assertions.assertEquals(expExtensionAttributesApcLine, networkStoreRepository.getAllExtensionsAttributesByResourceTypeAndExtensionName(NETWORK_UUID, 1, ResourceType.LINE, ActivePowerControl.NAME));
        Map<String, Map<String, ExtensionAttributes>> expExtensionAttributesLine = Map.of(lineId1, extensionAttributesMap1, lineId2, extensionAttributesMap2);
        Assertions.assertEquals(expExtensionAttributesLine, networkStoreRepository.getAllExtensionsAttributesByResourceType(NETWORK_UUID, 1, ResourceType.LINE));
    }

    @Test
    void getExtensionFromFullClone() {
        String networkId = "network1";
        String lineId1 = "line1";
        String lineId2 = "line2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 2, "variant2", CloneStrategy.PARTIAL);
        OwnerInfo ownerInfo1 = new OwnerInfo(lineId1, ResourceType.LINE, NETWORK_UUID, 2);
        Map<String, ExtensionAttributes> extensionAttributesMap1 = buildExtensionAttributesMap(5.6, "status1");
        OwnerInfo ownerInfo2 = new OwnerInfo(lineId2, ResourceType.LINE, NETWORK_UUID, 2);
        Map<String, ExtensionAttributes> extensionAttributesMap2 = buildExtensionAttributesMap(8.9, "status2");
        insertExtensions(Map.of(ownerInfo1, extensionAttributesMap1, ownerInfo2, extensionAttributesMap2));

        Assertions.assertEquals(Optional.of(extensionAttributesMap1.get(ActivePowerControl.NAME)), networkStoreRepository.getExtensionAttributes(NETWORK_UUID, 2, lineId1, ActivePowerControl.NAME));
        Assertions.assertEquals(extensionAttributesMap1, networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 2, lineId1));
        Map<String, ExtensionAttributes> expExtensionAttributesApcLine = Map.of(lineId1, buildActivePowerControlAttributes(5.6), lineId2, buildActivePowerControlAttributes(8.9));
        Assertions.assertEquals(expExtensionAttributesApcLine, networkStoreRepository.getAllExtensionsAttributesByResourceTypeAndExtensionName(NETWORK_UUID, 2, ResourceType.LINE, ActivePowerControl.NAME));
        Map<String, Map<String, ExtensionAttributes>> expExtensionAttributesLine = Map.of(lineId1, extensionAttributesMap1, lineId2, extensionAttributesMap2);
        Assertions.assertEquals(expExtensionAttributesLine, networkStoreRepository.getAllExtensionsAttributesByResourceType(NETWORK_UUID, 2, ResourceType.LINE));
    }

    @Test
    void getExtensionFromPartialCloneWithTombstonedIdentifiable() {
        String networkId = "network1";
        String lineId1 = "line1";
        String lineId2 = "line2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        OwnerInfo ownerInfo1 = new OwnerInfo(lineId1, ResourceType.LINE, NETWORK_UUID, 0);
        Map<String, ExtensionAttributes> extensionAttributesMap1 = buildExtensionAttributesMap(5.6, "status1");
        OwnerInfo ownerInfo2 = new OwnerInfo(lineId2, ResourceType.LINE, NETWORK_UUID, 0);
        Map<String, ExtensionAttributes> extensionAttributesMap2 = buildExtensionAttributesMap(8.9, "status2");
        insertExtensions(Map.of(ownerInfo1, extensionAttributesMap1, ownerInfo2, extensionAttributesMap2));
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);

        Assertions.assertEquals(Optional.of(extensionAttributesMap1.get(ActivePowerControl.NAME)), networkStoreRepository.getExtensionAttributes(NETWORK_UUID, 1, lineId1, ActivePowerControl.NAME));
        Assertions.assertEquals(extensionAttributesMap1, networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 1, lineId1));
        Map<String, ExtensionAttributes> expExtensionAttributesApcLine = Map.of(lineId1, buildActivePowerControlAttributes(5.6), lineId2, buildActivePowerControlAttributes(8.9));
        Assertions.assertEquals(expExtensionAttributesApcLine, networkStoreRepository.getAllExtensionsAttributesByResourceTypeAndExtensionName(NETWORK_UUID, 1, ResourceType.LINE, ActivePowerControl.NAME));
        Map<String, Map<String, ExtensionAttributes>> expExtensionAttributesLine = Map.of(lineId1, extensionAttributesMap1, lineId2, extensionAttributesMap2);
        Assertions.assertEquals(expExtensionAttributesLine, networkStoreRepository.getAllExtensionsAttributesByResourceType(NETWORK_UUID, 1, ResourceType.LINE));

        networkStoreRepository.deleteIdentifiable(NETWORK_UUID, 1, lineId1, LINE_TABLE);

        Assertions.assertEquals(Optional.empty(), networkStoreRepository.getExtensionAttributes(NETWORK_UUID, 1, lineId1, ActivePowerControl.NAME));
        Assertions.assertEquals(Map.of(), networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 1, lineId1));
        expExtensionAttributesApcLine = Map.of(lineId2, buildActivePowerControlAttributes(8.9));
        Assertions.assertEquals(expExtensionAttributesApcLine, networkStoreRepository.getAllExtensionsAttributesByResourceTypeAndExtensionName(NETWORK_UUID, 1, ResourceType.LINE, ActivePowerControl.NAME));
        expExtensionAttributesLine = Map.of(lineId2, extensionAttributesMap2);
        Assertions.assertEquals(expExtensionAttributesLine, networkStoreRepository.getAllExtensionsAttributesByResourceType(NETWORK_UUID, 1, ResourceType.LINE));
    }

    @Test
    void getExtensionFromPartialCloneWithTombstonedExtension() {
        String networkId = "network1";
        String lineId1 = "line1";
        String lineId2 = "line2";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        OwnerInfo ownerInfo1 = new OwnerInfo(lineId1, ResourceType.LINE, NETWORK_UUID, 0);
        Map<String, ExtensionAttributes> extensionAttributesMap1 = buildExtensionAttributesMap(5.6, "status1");
        OwnerInfo ownerInfo2 = new OwnerInfo(lineId2, ResourceType.LINE, NETWORK_UUID, 0);
        Map<String, ExtensionAttributes> extensionAttributesMap2 = buildExtensionAttributesMap(8.9, "status2");
        insertExtensions(Map.of(ownerInfo1, extensionAttributesMap1, ownerInfo2, extensionAttributesMap2));
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);

        Assertions.assertEquals(Optional.of(extensionAttributesMap1.get(ActivePowerControl.NAME)), networkStoreRepository.getExtensionAttributes(NETWORK_UUID, 1, lineId1, ActivePowerControl.NAME));
        Assertions.assertEquals(extensionAttributesMap1, networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 1, lineId1));
        Map<String, ExtensionAttributes> expExtensionAttributesApcLine = Map.of(lineId1, buildActivePowerControlAttributes(5.6), lineId2, buildActivePowerControlAttributes(8.9));
        Assertions.assertEquals(expExtensionAttributesApcLine, networkStoreRepository.getAllExtensionsAttributesByResourceTypeAndExtensionName(NETWORK_UUID, 1, ResourceType.LINE, ActivePowerControl.NAME));
        Map<String, Map<String, ExtensionAttributes>> expExtensionAttributesLine = Map.of(lineId1, extensionAttributesMap1, lineId2, extensionAttributesMap2);
        Assertions.assertEquals(expExtensionAttributesLine, networkStoreRepository.getAllExtensionsAttributesByResourceType(NETWORK_UUID, 1, ResourceType.LINE));

        networkStoreRepository.removeExtensionAttributes(NETWORK_UUID, 1, lineId1, ActivePowerControl.NAME);
        networkStoreRepository.removeExtensionAttributes(NETWORK_UUID, 1, lineId1, OperatingStatus.NAME);
        networkStoreRepository.removeExtensionAttributes(NETWORK_UUID, 1, lineId2, ActivePowerControl.NAME);

        Assertions.assertEquals(Optional.empty(), networkStoreRepository.getExtensionAttributes(NETWORK_UUID, 1, lineId1, ActivePowerControl.NAME));
        Assertions.assertEquals(Optional.of(extensionAttributesMap2.get(OperatingStatus.NAME)), networkStoreRepository.getExtensionAttributes(NETWORK_UUID, 1, lineId2, OperatingStatus.NAME));
        Assertions.assertEquals(Map.of(), networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 1, lineId1));
        Assertions.assertEquals(Map.of(OperatingStatus.NAME, buildOperatingStatusAttributes("status2")), networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 1, lineId2));
        Map<String, ExtensionAttributes> expExtensionAttributesOsLine = Map.of(lineId2, buildOperatingStatusAttributes("status2"));
        Assertions.assertEquals(Map.of(), networkStoreRepository.getAllExtensionsAttributesByResourceTypeAndExtensionName(NETWORK_UUID, 1, ResourceType.LINE, ActivePowerControl.NAME));
        Assertions.assertEquals(expExtensionAttributesOsLine, networkStoreRepository.getAllExtensionsAttributesByResourceTypeAndExtensionName(NETWORK_UUID, 1, ResourceType.LINE, OperatingStatus.NAME));
        expExtensionAttributesLine = Map.of(lineId2, Map.of(OperatingStatus.NAME, buildOperatingStatusAttributes("status2")));
        Assertions.assertEquals(expExtensionAttributesLine, networkStoreRepository.getAllExtensionsAttributesByResourceType(NETWORK_UUID, 1, ResourceType.LINE));
    }

    @Test
    void getExtensionFromPartialCloneWithRecreatedIdentifiable() {
        String networkId = "network1";
        String lineId1 = "line1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        createLine(networkStoreRepository, NETWORK_UUID, 0, lineId1, "vl1", "vl2");
        OwnerInfo ownerInfo1 = new OwnerInfo(lineId1, ResourceType.LINE, NETWORK_UUID, 0);
        Map<String, ExtensionAttributes> extensionAttributesMap1 = buildExtensionAttributesMap(5.6, "status1");
        insertExtensions(Map.of(ownerInfo1, extensionAttributesMap1));
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 0, 1, "variant1", CloneStrategy.PARTIAL);

        Assertions.assertEquals(Optional.of(extensionAttributesMap1.get(ActivePowerControl.NAME)), networkStoreRepository.getExtensionAttributes(NETWORK_UUID, 1, lineId1, ActivePowerControl.NAME));
        Assertions.assertEquals(extensionAttributesMap1, networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 1, lineId1));

        // Recreate identifiable without extensions
        networkStoreRepository.deleteIdentifiable(NETWORK_UUID, 1, lineId1, LINE_TABLE);
        createLine(networkStoreRepository, NETWORK_UUID, 1, lineId1, "vl1", "vl2");

        Assertions.assertEquals(Optional.empty(), networkStoreRepository.getExtensionAttributes(NETWORK_UUID, 1, lineId1, ActivePowerControl.NAME));
        Assertions.assertEquals(Map.of(), networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 1, lineId1));
    }

    @Test
    void removeExtensionWithoutNetwork() {
        PowsyblException exception = assertThrows(PowsyblException.class, () -> networkStoreRepository.removeExtensionAttributes(NETWORK_UUID, 0, "unknownId", "unknownExtension"));
        assertTrue(exception.getMessage().contains("Cannot retrieve source network attributes"));
    }

    @Test
    void removeExtensionOnFullVariant() {
        String networkId = "network1";
        String lineId = "line1";
        createFullVariantNetwork(networkStoreRepository, NETWORK_UUID, networkId, 0, "variant0", CloneStrategy.PARTIAL);
        OwnerInfo ownerInfo = new OwnerInfo(lineId, ResourceType.LINE, NETWORK_UUID, 0);
        Map<String, ExtensionAttributes> extensionAttributesMap = buildExtensionAttributesMap(5.6, "status1");
        insertExtensions(Map.of(ownerInfo, extensionAttributesMap));

        networkStoreRepository.removeExtensionAttributes(NETWORK_UUID, 0, lineId, ActivePowerControl.NAME);

        Assertions.assertEquals(Map.of(OperatingStatus.NAME, buildOperatingStatusAttributes("status1")), networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 0, lineId));
        Assertions.assertTrue(getTombstonedExtensions(NETWORK_UUID, 0).isEmpty());
    }

    @Test
    void removeExtensionOnPartialVariant() {
        String networkId = "network1";
        String lineId = "line1";
        createNetwork(networkStoreRepository, NETWORK_UUID, networkId, 1, "variant1", CloneStrategy.PARTIAL, 0);
        OwnerInfo ownerInfo = new OwnerInfo(lineId, ResourceType.LINE, NETWORK_UUID, 1);
        Map<String, ExtensionAttributes> extensionAttributesMap = buildExtensionAttributesMap(5.6, "status1");
        insertExtensions(Map.of(ownerInfo, extensionAttributesMap));

        networkStoreRepository.removeExtensionAttributes(NETWORK_UUID, 1, lineId, ActivePowerControl.NAME);
        networkStoreRepository.removeExtensionAttributes(NETWORK_UUID, 1, lineId, OperatingStatus.NAME);

        Assertions.assertEquals(Map.of(), networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 1, lineId));
        Assertions.assertEquals(Map.of(lineId, Set.of(ActivePowerControl.NAME, OperatingStatus.NAME)), getTombstonedExtensions(NETWORK_UUID, 1));
    }

    @Test
    void createExtensionWithRecreatedTombstonedExtension() {
        String networkId = "network1";
        String lineId = "line1";
        // Variant 0
        createNetwork(networkStoreRepository, NETWORK_UUID, networkId, 1, "variant1", CloneStrategy.PARTIAL, 0);
        OwnerInfo ownerInfo1 = new OwnerInfo(lineId, ResourceType.LINE, NETWORK_UUID, 1);
        Map<String, ExtensionAttributes> extensionAttributesMap = buildExtensionAttributesMap(5.6, "status1");
        insertExtensions(Map.of(ownerInfo1, extensionAttributesMap));
        networkStoreRepository.removeExtensionAttributes(NETWORK_UUID, 1, lineId, ActivePowerControl.NAME);
        networkStoreRepository.cloneNetworkVariant(NETWORK_UUID, 1, 2, "variant1", CloneStrategy.PARTIAL);
        // Variant 2
        OwnerInfo ownerInfo2 = new OwnerInfo(lineId, ResourceType.LINE, NETWORK_UUID, 2);
        extensionAttributesMap = Map.of(ActivePowerControl.NAME, buildActivePowerControlAttributes(8.4));
        insertExtensions(Map.of(ownerInfo2, extensionAttributesMap));

        // Variant 1 (removed line1)
        Assertions.assertEquals(Map.of(OperatingStatus.NAME, buildOperatingStatusAttributes("status1")), networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 1, lineId));
        Assertions.assertEquals(Map.of(lineId, Set.of(ActivePowerControl.NAME)), getTombstonedExtensions(NETWORK_UUID, 1));
        // Variant 2 (recreated line1 with different attributes)
        Assertions.assertEquals(Map.of(OperatingStatus.NAME, buildOperatingStatusAttributes("status1"), ActivePowerControl.NAME, buildActivePowerControlAttributes(8.4)), networkStoreRepository.getAllExtensionsAttributesByIdentifiableId(NETWORK_UUID, 2, lineId));
        Assertions.assertEquals(Map.of(lineId, Set.of(ActivePowerControl.NAME)), getTombstonedExtensions(NETWORK_UUID, 2));
    }

    @Test
    void emptyCreateExtensionsDoesNotThrow() {
        assertDoesNotThrow(() -> insertExtensions(Map.of()));
        assertDoesNotThrow(() -> insertExtensions(Map.of(new OwnerInfo("id", ResourceType.LINE, NETWORK_UUID, 0), Map.of())));
    }

    private Map<String, Set<String>> getTombstonedExtensions(UUID networkUuid, int variantNum) {
        try (var connection = dataSource.getConnection()) {
            return extensionHandler.getTombstonedExtensions(connection, networkUuid, variantNum);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private void insertExtensions(Map<OwnerInfo, Map<String, ExtensionAttributes>> extensions) {
        try (var connection = dataSource.getConnection()) {
            extensionHandler.insertExtensions(connection, extensions);
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }
}
