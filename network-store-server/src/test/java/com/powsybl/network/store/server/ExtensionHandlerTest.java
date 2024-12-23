/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server;

import com.powsybl.network.store.model.*;
import com.powsybl.network.store.server.dto.OwnerInfo;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Antoine Bouhours <antoine.bouhours at rte-france.com>
 */
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
class ExtensionHandlerTest {

    @Autowired
    private DataSource dataSource;

    @DynamicPropertySource
    static void makeTestDbSuffix(DynamicPropertyRegistry registry) {
        UUID uuid = UUID.randomUUID();
        registry.add("testDbSuffix", () -> uuid);
    }

    @Autowired
    private ExtensionHandler extensionHandler;

    private Connection connection;

    private static final UUID NETWORK_UUID = UUID.fromString("7928181c-7977-4592-ba19-88027e4254e4");

    @BeforeEach
    void setUp() throws SQLException {
        connection = dataSource.getConnection();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    void createExtensionsTest() throws SQLException {
        String equipmentId1 = "idBatteryA";

        OwnerInfo infoBatteryA = new OwnerInfo(
                equipmentId1,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        Map<String, ExtensionAttributes> extensionAttributesMap1 = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(6.0).participate(true).participationFactor(1.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test23").build());

        Map<OwnerInfo, Map<String, ExtensionAttributes>> map1 = new HashMap<>();
        map1.put(infoBatteryA, extensionAttributesMap1);

        extensionHandler.insertExtensions(connection, map1);

        Map<String, ExtensionAttributes> extensionAttributesResults = extensionHandler.getAllExtensionsAttributesByIdentifiableIdForVariant(connection, NETWORK_UUID, 0, "idBatteryA");
        assertEquals(2, extensionAttributesResults.size());
        assertNotNull(extensionAttributesResults.get("activePowerControl"));
        ActivePowerControlAttributes activePowerControl = (ActivePowerControlAttributes) extensionAttributesResults.get("activePowerControl");
        assertTrue(activePowerControl.isParticipate());
        assertEquals(6.0, activePowerControl.getDroop(), 0.1);
        assertEquals(1.5, activePowerControl.getParticipationFactor(), 0.1);
        assertNotNull(extensionAttributesResults.get("operatingStatus"));
        assertEquals(ActivePowerControlAttributes.class, activePowerControl.getClass());
        assertEquals(OperatingStatusAttributes.class, extensionAttributesResults.get("operatingStatus").getClass());
    }

    @Test
    void getExtensionsTest() throws SQLException {
        String batteryId1 = "idBattery1";
        String batteryId2 = "idBattery2";
        String generatorId1 = "idGenerator1";

        OwnerInfo infoBattery1 = new OwnerInfo(
                batteryId1,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        Map<String, ExtensionAttributes> extensionAttributesBattery1 = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(6.0).participate(true).participationFactor(1.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test12").build());
        extensionHandler.insertExtensions(connection, Map.of(infoBattery1, extensionAttributesBattery1));

        OwnerInfo infoBattery2 = new OwnerInfo(
                batteryId2,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        Map<String, ExtensionAttributes> extensionAttributesBattery2 = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(5.0).participate(false).participationFactor(0.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test23").build());
        extensionHandler.insertExtensions(connection, Map.of(infoBattery2, extensionAttributesBattery2));

        OwnerInfo infoGenerator1 = new OwnerInfo(
                generatorId1,
                ResourceType.GENERATOR,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        Map<String, ExtensionAttributes> extensionAttributesGenerator1 = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(7.0).participate(true).participationFactor(0.2).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test45").build());
        extensionHandler.insertExtensions(connection, Map.of(infoGenerator1, extensionAttributesGenerator1));

        // Get one extension attributes
        Optional<ExtensionAttributes> apcAttributesOpt = extensionHandler.getExtensionAttributesForVariant(connection, NETWORK_UUID, 0, batteryId1, "activePowerControl");
        assertTrue(apcAttributesOpt.isPresent());
        ActivePowerControlAttributes apcAttributes = (ActivePowerControlAttributes) apcAttributesOpt.get();
        assertTrue(apcAttributes.isParticipate());
        assertEquals(6.0, apcAttributes.getDroop(), 0.1);
        assertEquals(1.5, apcAttributes.getParticipationFactor(), 0.1);
        Optional<ExtensionAttributes> notFoundAttributesOpt = extensionHandler.getExtensionAttributesForVariant(connection, NETWORK_UUID, 0, batteryId1, "notFound");
        assertFalse(notFoundAttributesOpt.isPresent());

        // Get all extensions attributes by resource type and extensionName
        Map<String, ExtensionAttributes> extensionAttributesById = extensionHandler.getAllExtensionsAttributesByResourceTypeAndExtensionNameForVariant(connection, NETWORK_UUID, 0, ResourceType.BATTERY.toString(), "activePowerControl");
        assertEquals(2, extensionAttributesById.size());
        assertTrue(extensionAttributesById.containsKey(batteryId1));
        assertTrue(extensionAttributesById.containsKey(batteryId2));
        Map<String, ExtensionAttributes> notFoundAttributesById = extensionHandler.getAllExtensionsAttributesByResourceTypeAndExtensionNameForVariant(connection, NETWORK_UUID, 0, ResourceType.BATTERY.toString(), "notFound");
        assertTrue(notFoundAttributesById.isEmpty());

        // Get all extensions attributes by identifiable id
        Map<String, ExtensionAttributes> extensionAttributesByExtensionName = extensionHandler.getAllExtensionsAttributesByIdentifiableIdForVariant(connection, NETWORK_UUID, 0, batteryId1);
        assertEquals(2, extensionAttributesByExtensionName.size());
        assertTrue(extensionAttributesByExtensionName.containsKey("activePowerControl"));
        assertTrue(extensionAttributesByExtensionName.containsKey("operatingStatus"));
        assertTrue(extensionAttributesById.containsKey(batteryId2));
        Map<String, ExtensionAttributes> notFoundByExtensionName = extensionHandler.getAllExtensionsAttributesByIdentifiableIdForVariant(connection, NETWORK_UUID, 0, "notFound");
        assertTrue(notFoundByExtensionName.isEmpty());

        // Get all extensions attributes by resource type
        Map<String, Map<String, ExtensionAttributes>> extensionAttributesMap = extensionHandler.getAllExtensionsAttributesByResourceTypeForVariant(connection, NETWORK_UUID, 0, ResourceType.BATTERY.toString());
        assertEquals(2, extensionAttributesMap.size());
        assertTrue(extensionAttributesMap.containsKey(batteryId1));
        assertTrue(extensionAttributesMap.get(batteryId1).containsKey("activePowerControl"));
        assertTrue(extensionAttributesMap.get(batteryId1).containsKey("operatingStatus"));
        assertTrue(extensionAttributesMap.containsKey(batteryId2));
        assertTrue(extensionAttributesMap.get(batteryId2).containsKey("activePowerControl"));
        assertTrue(extensionAttributesMap.get(batteryId2).containsKey("operatingStatus"));
        Map<String, Map<String, ExtensionAttributes>> notExtensionAttributes = extensionHandler.getAllExtensionsAttributesByResourceTypeForVariant(connection, NETWORK_UUID, 0, ResourceType.LINE.toString());
        assertTrue(notExtensionAttributes.isEmpty());
    }

    @Test
    void deleteExtensionsTest() throws SQLException {
        String batteryId1 = "idBattery1";
        String batteryId2 = "idBattery2";

        OwnerInfo infoBattery1 = new OwnerInfo(
                batteryId1,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        Map<String, ExtensionAttributes> extensionAttributesBattery1 = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(6.0).participate(true).participationFactor(1.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test12").build());
        extensionHandler.insertExtensions(connection, Map.of(infoBattery1, extensionAttributesBattery1));

        OwnerInfo infoBattery2 = new OwnerInfo(
                batteryId2,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        Map<String, ExtensionAttributes> extensionAttributesBattery2 = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(5.0).participate(false).participationFactor(0.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test23").build());
        extensionHandler.insertExtensions(connection, Map.of(infoBattery2, extensionAttributesBattery2));

        Map<String, ExtensionAttributes> extensions = extensionHandler.getAllExtensionsAttributesByIdentifiableIdForVariant(connection, NETWORK_UUID, 0, batteryId1);
        assertEquals(2, extensions.size());
        extensionHandler.deleteExtensionsFromIdentifiables(connection, NETWORK_UUID, 0, Map.of(batteryId1, Set.of("activePowerControl")));
        extensions = extensionHandler.getAllExtensionsAttributesByIdentifiableIdForVariant(connection, NETWORK_UUID, 0, batteryId1);
        assertEquals(1, extensions.size());
        assertFalse(extensions.containsKey("activePowerControl"));
        assertTrue(extensions.containsKey("operatingStatus"));

        extensionHandler.deleteExtensionsFromIdentifiable(connection, NETWORK_UUID, 0, batteryId1);
        extensions = extensionHandler.getAllExtensionsAttributesByIdentifiableIdForVariant(connection, NETWORK_UUID, 0, batteryId1);
        assertEquals(0, extensions.size());

        extensionHandler.deleteExtensionsFromIdentifiables(connection, NETWORK_UUID, 0, Map.of(batteryId2, Set.of("activePowerControl", "operatingStatus")));
        extensions = extensionHandler.getAllExtensionsAttributesByIdentifiableIdForVariant(connection, NETWORK_UUID, 0, batteryId2);
        assertEquals(0, extensions.size());
    }

    @Test
    void updateExtensionsFromEquipmentsTest() throws SQLException {

        String batteryId1 = "idBattery1";

        OwnerInfo infoBattery1 = new OwnerInfo(
                batteryId1,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        Map<String, ExtensionAttributes> extensionAttributesBattery1 = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(6.0).participate(true).participationFactor(1.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test12").build());
        extensionHandler.insertExtensions(connection, Map.of(infoBattery1, extensionAttributesBattery1));

        Map<String, ExtensionAttributes> extensionAttributes = extensionHandler.getAllExtensionsAttributesByIdentifiableIdForVariant(connection, NETWORK_UUID, 0, batteryId1);
        assertEquals(2, extensionAttributes.size());
        assertNotNull(extensionAttributes.get("activePowerControl"));
        ActivePowerControlAttributes activePowerControl = (ActivePowerControlAttributes) extensionAttributes.get("activePowerControl");
        assertTrue(activePowerControl.isParticipate());
        assertEquals(6.0, activePowerControl.getDroop(), 0.1);
        assertEquals(1.5, activePowerControl.getParticipationFactor(), 0.1);
        OperatingStatusAttributes operatingStatus = (OperatingStatusAttributes) extensionAttributes.get("operatingStatus");
        assertEquals("test12", operatingStatus.getOperatingStatus());

        // Update one of the two extension attributes
        Map<String, ExtensionAttributes> updatedExtensionAttributes = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(10.0).participate(false).participationFactor(2.0).build());
        BatteryAttributes batteryAttributes = new BatteryAttributes();
        batteryAttributes.setExtensionAttributes(updatedExtensionAttributes);
        Resource<BatteryAttributes> battery1 = Resource.batteryBuilder().id(batteryId1).attributes(batteryAttributes).build();
        extensionHandler.updateExtensionsFromEquipments(connection, NETWORK_UUID, List.of(battery1));
        extensionAttributes = extensionHandler.getAllExtensionsAttributesByIdentifiableIdForVariant(connection, NETWORK_UUID, 0, batteryId1);
        assertEquals(2, extensionAttributes.size());
        assertNotNull(extensionAttributes.get("activePowerControl"));
        activePowerControl = (ActivePowerControlAttributes) extensionAttributes.get("activePowerControl");
        assertFalse(activePowerControl.isParticipate());
        assertEquals(10.0, activePowerControl.getDroop(), 0.1);
        assertEquals(2.0, activePowerControl.getParticipationFactor(), 0.1);
        assertEquals("test12", operatingStatus.getOperatingStatus());
    }

    @Test
    void updateExtensionsFromNetworksTest() throws SQLException {
        String networkId1 = "idBattery1";

        OwnerInfo infoNetwork1 = new OwnerInfo(
                networkId1,
                ResourceType.NETWORK,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        Map<String, ExtensionAttributes> extensionAttributesNetwork1 = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(6.0).participate(true).participationFactor(1.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test12").build());
        extensionHandler.insertExtensions(connection, Map.of(infoNetwork1, extensionAttributesNetwork1));

        Map<String, ExtensionAttributes> extensionAttributes = extensionHandler.getAllExtensionsAttributesByIdentifiableIdForVariant(connection, NETWORK_UUID, 0, networkId1);
        assertEquals(2, extensionAttributes.size());
        assertNotNull(extensionAttributes.get("activePowerControl"));
        ActivePowerControlAttributes activePowerControl = (ActivePowerControlAttributes) extensionAttributes.get("activePowerControl");
        assertTrue(activePowerControl.isParticipate());
        assertEquals(6.0, activePowerControl.getDroop(), 0.1);
        assertEquals(1.5, activePowerControl.getParticipationFactor(), 0.1);
        OperatingStatusAttributes operatingStatus = (OperatingStatusAttributes) extensionAttributes.get("operatingStatus");
        assertEquals("test12", operatingStatus.getOperatingStatus());

        // Update one of the two extension attributes
        Map<String, ExtensionAttributes> updatedExtensionAttributes = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(10.0).participate(false).participationFactor(2.0).build());
        NetworkAttributes networkAttributes = new NetworkAttributes();
        networkAttributes.setExtensionAttributes(updatedExtensionAttributes);
        networkAttributes.setUuid(NETWORK_UUID);
        Resource<NetworkAttributes> network1 = Resource.networkBuilder().id(networkId1).attributes(networkAttributes).build();
        extensionHandler.updateExtensionsFromNetworks(connection, List.of(network1));
        extensionAttributes = extensionHandler.getAllExtensionsAttributesByIdentifiableIdForVariant(connection, NETWORK_UUID, 0, networkId1);
        assertEquals(2, extensionAttributes.size());
        assertNotNull(extensionAttributes.get("activePowerControl"));
        activePowerControl = (ActivePowerControlAttributes) extensionAttributes.get("activePowerControl");
        assertFalse(activePowerControl.isParticipate());
        assertEquals(10.0, activePowerControl.getDroop(), 0.1);
        assertEquals(2.0, activePowerControl.getParticipationFactor(), 0.1);
        assertEquals("test12", operatingStatus.getOperatingStatus());
    }

    @Test
    void insertNonPersistentExtensionTest() throws SQLException {
        String equipmentId = "idBattery";

        OwnerInfo infoBattery = new OwnerInfo(
                equipmentId,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        Map<String, ExtensionAttributes> extensionAttributes = Map.of(
                "notPersistent", new NonPersistentExtensionAttributes(),
                "activePowerControl", ActivePowerControlAttributes.builder().droop(6.0).participate(true).participationFactor(1.5).build()
        );
        extensionHandler.insertExtensions(connection, Map.of(infoBattery, extensionAttributes));

        extensionAttributes = extensionHandler.getAllExtensionsAttributesByIdentifiableIdForVariant(connection, NETWORK_UUID, 0, equipmentId);
        assertEquals(1, extensionAttributes.size());
        assertFalse(extensionAttributes.containsKey("notPersistent"));
        assertTrue(extensionAttributes.containsKey("activePowerControl"));
    }

    @NoArgsConstructor
    private static class NonPersistentExtensionAttributes implements ExtensionAttributes {
        @Override
        public boolean isPersistent() {
            return false;
        }
    }
}
