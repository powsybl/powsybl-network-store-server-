/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server;

import com.powsybl.network.store.model.*;
import com.powsybl.network.store.server.dto.OwnerInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

import static org.junit.Assert.*;

/**
 * @author Antoine Bouhours <antoine.bouhours at rte-france.com>
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class ExtensionHandlerTest {

    @DynamicPropertySource
    static void makeTestDbSuffix(DynamicPropertyRegistry registry) {
        UUID uuid = UUID.randomUUID();
        registry.add("testDbSuffix", () -> uuid);
    }

    @Autowired
    private ExtensionHandler extensionHandler;

    private static final UUID NETWORK_UUID = UUID.fromString("7928181c-7977-4592-ba19-88027e4254e4");

    @Test
    public void insertExtensionsInBatteryTest() {

        String equipmentIdA = "idBatteryA";
        String equipmentIdB = "idBatteryB";

        OwnerInfo infoBatteryA = new OwnerInfo(
                equipmentIdA,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        OwnerInfo infoBatteryB = new OwnerInfo(
                equipmentIdB,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        OwnerInfo infoBatteryX = new OwnerInfo(
                "badID",
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );

        Resource<BatteryAttributes> resBatteryA = Resource.batteryBuilder()
                .id(equipmentIdA)
                .attributes(BatteryAttributes.builder()
                        .voltageLevelId("vl1")
                        .name("batteryA")
                        .targetP(250)
                        .targetQ(100)
                        .maxP(500)
                        .minP(100)
                        .reactiveLimits(MinMaxReactiveLimitsAttributes.builder().maxQ(10).minQ(11).build())
                        .build())
                .build();

        Resource<BatteryAttributes> resBatteryB = Resource.batteryBuilder()
                .id(equipmentIdB)
                .attributes(BatteryAttributes.builder()
                        .voltageLevelId("vl1")
                        .name("batteryB")
                        .targetP(250)
                        .targetQ(100)
                        .maxP(500)
                        .minP(100)
                        .reactiveLimits(MinMaxReactiveLimitsAttributes.builder().maxQ(5).minQ(6).build())
                        .build())
                .build();

        List<Resource<BatteryAttributes>> batteries = new ArrayList<>();
        batteries.add(resBatteryA);
        batteries.add(resBatteryB);

        assertEquals(resBatteryA.getId(), infoBatteryA.getEquipmentId());
        assertEquals(resBatteryB.getId(), infoBatteryB.getEquipmentId());
        assertNotEquals(resBatteryA.getId(), infoBatteryX.getEquipmentId());

        Map<String, ExtensionAttributes> extensionAttributesMapA = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(6.0).participate(true).participationFactor(1.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test12").build());
        Map<String, ExtensionAttributes> extensionAttributesMapB = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(5.0).participate(false).participationFactor(0.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test23").build());

        Map<OwnerInfo, Map<String, ExtensionAttributes>> mapA = new HashMap<>();
        mapA.put(infoBatteryA, extensionAttributesMapA);
        Map<OwnerInfo, Map<String, ExtensionAttributes>> mapB = new HashMap<>();
        mapB.put(infoBatteryB, extensionAttributesMapB);

        assertEquals(resBatteryA.getAttributes().getExtensionAttributes(), new HashMap<>());
        assertNull(resBatteryA.getAttributes().getExtensionAttributes().get("activePowerControl"));
        assertNull(resBatteryA.getAttributes().getExtensionAttributes().get("operatingStatus"));
        assertEquals(resBatteryB.getAttributes().getExtensionAttributes(), new HashMap<>());
        assertNull(resBatteryB.getAttributes().getExtensionAttributes().get("activePowerControl"));
        assertNull(resBatteryB.getAttributes().getExtensionAttributes().get("operatingStatus"));

        extensionHandler.insertExtensionsInEquipments(NETWORK_UUID, batteries, new HashMap<>());

        assertEquals(resBatteryA.getAttributes().getExtensionAttributes(), new HashMap<>());
        assertNull(resBatteryA.getAttributes().getExtensionAttributes().get("activePowerControl"));
        assertNull(resBatteryA.getAttributes().getExtensionAttributes().get("operatingStatus"));
        assertEquals(resBatteryB.getAttributes().getExtensionAttributes(), new HashMap<>());
        assertNull(resBatteryB.getAttributes().getExtensionAttributes().get("activePowerControl"));
        assertNull(resBatteryB.getAttributes().getExtensionAttributes().get("operatingStatus"));

        extensionHandler.insertExtensionsInEquipments(NETWORK_UUID, batteries, mapA);
        assertNotNull(resBatteryA.getAttributes().getExtensionAttributes().get("activePowerControl"));
        assertNotNull(resBatteryA.getAttributes().getExtensionAttributes().get("operatingStatus"));
        assertEquals(resBatteryB.getAttributes().getExtensionAttributes(), new HashMap<>());
        assertNull(resBatteryB.getAttributes().getExtensionAttributes().get("activePowerControl"));
        assertNull(resBatteryB.getAttributes().getExtensionAttributes().get("operatingStatus"));

        extensionHandler.insertExtensionsInEquipments(NETWORK_UUID, batteries, mapB);
        assertNotNull(resBatteryA.getAttributes().getExtensionAttributes().get("activePowerControl"));
        assertNotNull(resBatteryA.getAttributes().getExtensionAttributes().get("operatingStatus"));
        assertNotNull(resBatteryB.getAttributes().getExtensionAttributes().get("activePowerControl"));
        assertNotNull(resBatteryB.getAttributes().getExtensionAttributes().get("operatingStatus"));
    }

    @Test
    public void insertExtensionsTest() {
        String equipmentIdA = "idBatteryA";

        OwnerInfo infoBatteryA = new OwnerInfo(
                equipmentIdA,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        Map<String, ExtensionAttributes> extensionAttributesMapA = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(6.0).participate(true).participationFactor(1.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test23").build());

        Map<OwnerInfo, Map<String, ExtensionAttributes>> mapA = new HashMap<>();
        mapA.put(infoBatteryA, extensionAttributesMapA);

        extensionHandler.insertExtensions(mapA);

        Map<OwnerInfo, Map<String, ExtensionAttributes>> extensions = extensionHandler.getExtensions(NETWORK_UUID, 0, "equipmentId", "idBatteryA");
        Map<String, ExtensionAttributes> extensionAttributes = extensions.get(infoBatteryA);
        assertEquals(2, extensionAttributes.size());
        assertNotNull(extensionAttributes.get("activePowerControl"));
        ActivePowerControlAttributes activePowerControl = (ActivePowerControlAttributes) extensionAttributes.get("activePowerControl");
        assertTrue(activePowerControl.isParticipate());
        assertEquals(6.0, activePowerControl.getDroop(), 0.1);
        assertEquals(1.5, activePowerControl.getParticipationFactor(), 0.1);
        assertNotNull(extensionAttributes.get("operatingStatus"));
        assertEquals(ActivePowerControlAttributes.class, activePowerControl.getClass());
        assertEquals(OperatingStatusAttributes.class, extensionAttributes.get("operatingStatus").getClass());
    }

    @Test
    public void getExtensionsWithInClauseTest() {
        String equipmentIdA = "idBatteryA";
        String equipmentIdB = "idBatteryB";

        OwnerInfo infoBatteryA = new OwnerInfo(
                equipmentIdA,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        OwnerInfo infoBatteryB = new OwnerInfo(
                equipmentIdB,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        Map<String, ExtensionAttributes> extensionAttributesMapA = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(6.0).participate(true).participationFactor(1.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test12").build());
        Map<String, ExtensionAttributes> extensionAttributesMapB = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(5.0).participate(false).participationFactor(0.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test23").build());

        Map<OwnerInfo, Map<String, ExtensionAttributes>> mapA = new HashMap<>();
        mapA.put(infoBatteryA, extensionAttributesMapA);
        Map<OwnerInfo, Map<String, ExtensionAttributes>> mapB = new HashMap<>();
        mapB.put(infoBatteryB, extensionAttributesMapB);

        extensionHandler.insertExtensions(mapA);
        extensionHandler.insertExtensions(mapB);

        Map<OwnerInfo, Map<String, ExtensionAttributes>> extensions = extensionHandler.getExtensionsWithInClause(NETWORK_UUID, 0, "equipmentId", List.of("idBatteryA", "idBatteryB"));
        assertEquals(2, extensions.size());
        assertNotNull(extensions.get(infoBatteryA));
        Map<String, ExtensionAttributes> expBatteryA = extensionHandler.getExtensions(NETWORK_UUID, 0, "equipmentId", equipmentIdA).get(infoBatteryA);
        assertEquals(expBatteryA, extensions.get(infoBatteryA));
        assertNotNull(extensions.get(infoBatteryB));
        Map<String, ExtensionAttributes> expBatteryB = extensionHandler.getExtensions(NETWORK_UUID, 0, "equipmentId", equipmentIdB).get(infoBatteryB);
        assertEquals(expBatteryB, extensions.get(infoBatteryB));
    }

    @Test
    public void deleteExtensionsTest() {
        String equipmentIdA = "idBatteryA";
        String equipmentIdB = "idBatteryB";

        OwnerInfo infoBatteryA = new OwnerInfo(
                equipmentIdA,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        OwnerInfo infoBatteryB = new OwnerInfo(
                equipmentIdB,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        Map<String, ExtensionAttributes> extensionAttributesMapA = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(6.0).participate(true).participationFactor(1.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test12").build());
        Map<String, ExtensionAttributes> extensionAttributesMapB = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(5.0).participate(false).participationFactor(0.5).build(),
                "operatingStatus", OperatingStatusAttributes.builder().operatingStatus("test23").build());

        Map<OwnerInfo, Map<String, ExtensionAttributes>> mapA = new HashMap<>();
        mapA.put(infoBatteryA, extensionAttributesMapA);
        Map<OwnerInfo, Map<String, ExtensionAttributes>> mapB = new HashMap<>();
        mapB.put(infoBatteryB, extensionAttributesMapB);

        extensionHandler.insertExtensions(mapA);
        extensionHandler.insertExtensions(mapB);

        extensionHandler.deleteExtensions(NETWORK_UUID, 0, "idBatteryA");
        Map<OwnerInfo, Map<String, ExtensionAttributes>> extensions = extensionHandler.getExtensionsWithInClause(NETWORK_UUID, 0, "equipmentId", List.of("idBatteryA", "idBatteryB"));
        assertEquals(1, extensions.size());
        Resource<BatteryAttributes> batteryB = Resource.batteryBuilder().id("idBatteryB").attributes(new BatteryAttributes()).build();
        extensionHandler.deleteExtensions(NETWORK_UUID, List.of(batteryB));
        extensions = extensionHandler.getExtensionsWithInClause(NETWORK_UUID, 0, "equipmentId", List.of("idBatteryA", "idBatteryB"));
        assertEquals(0, extensions.size());
    }

    @Test
    public void updateExtensionsTest() {
        String equipmentIdA = "idBatteryA";

        OwnerInfo infoBatteryA = new OwnerInfo(
                equipmentIdA,
                ResourceType.BATTERY,
                NETWORK_UUID,
                Resource.INITIAL_VARIANT_NUM
        );
        Map<String, ExtensionAttributes> extensionAttributesMapA = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(6.0).participate(true).participationFactor(1.5).build());

        Map<OwnerInfo, Map<String, ExtensionAttributes>> mapA = new HashMap<>();
        mapA.put(infoBatteryA, extensionAttributesMapA);

        extensionHandler.insertExtensions(mapA);

        Map<OwnerInfo, Map<String, ExtensionAttributes>> extensions = extensionHandler.getExtensions(NETWORK_UUID, 0, "equipmentId", "idBatteryA");
        Map<String, ExtensionAttributes> extensionAttributes = extensions.get(infoBatteryA);
        assertEquals(1, extensionAttributes.size());
        assertNotNull(extensionAttributes.get("activePowerControl"));
        ActivePowerControlAttributes activePowerControl = (ActivePowerControlAttributes) extensionAttributes.get("activePowerControl");
        assertTrue(activePowerControl.isParticipate());
        assertEquals(6.0, activePowerControl.getDroop(), 0.1);
        assertEquals(1.5, activePowerControl.getParticipationFactor(), 0.1);

        extensionAttributesMapA = Map.of("activePowerControl", ActivePowerControlAttributes.builder().droop(10.0).participate(false).participationFactor(2.0).build());
        BatteryAttributes batteryAttributes = new BatteryAttributes();
        batteryAttributes.setExtensionAttributes(extensionAttributesMapA);
        Resource<BatteryAttributes> batteryA = Resource.batteryBuilder().id("idBatteryA").attributes(batteryAttributes).build();
        extensionHandler.updateExtensions(NETWORK_UUID, List.of(batteryA));
        extensions = extensionHandler.getExtensions(NETWORK_UUID, 0, "equipmentId", "idBatteryA");
        extensionAttributes = extensions.get(infoBatteryA);
        activePowerControl = (ActivePowerControlAttributes) extensionAttributes.get("activePowerControl");
        assertFalse(activePowerControl.isParticipate());
        assertEquals(10.0, activePowerControl.getDroop(), 0.1);
        assertEquals(2.0, activePowerControl.getParticipationFactor(), 0.1);
    }
}
