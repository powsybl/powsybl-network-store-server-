/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.powsybl.iidm.network.*;
import com.powsybl.iidm.network.extensions.ActivePowerControl;
import com.powsybl.iidm.network.extensions.ConnectablePosition;
import com.powsybl.iidm.network.extensions.GeneratorStartup;
import com.powsybl.network.store.model.*;
import jakarta.servlet.ServletException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.time.ZonedDateTime;
import java.util.*;

import static com.powsybl.network.store.model.NetworkStoreApi.VERSION;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class NetworkStoreControllerIT {

    @DynamicPropertySource
    static void makeTestDbSuffix(DynamicPropertyRegistry registry) {
        UUID uuid = UUID.randomUUID();
        registry.add("testDbSuffix", () -> uuid);
    }

    private static final UUID NETWORK_UUID = UUID.fromString("7928181c-7977-4592-ba19-88027e4254e4");

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    private MockMvc mvc;

    @Before
    public void setup() {
        this.objectMapper.registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                .configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false);
    }

    @Test
    public void test() throws Exception {
        mvc.perform(get("/" + VERSION + "/networks")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(content().json("[]"));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM)
                .contentType(APPLICATION_JSON))
                .andExpect(status().isNotFound());

        Resource<NetworkAttributes> foo = Resource.networkBuilder()
                .id("foo")
                .attributes(NetworkAttributes.builder()
                                             .uuid(NETWORK_UUID)
                                             .variantId("v")
                                             .caseDate(ZonedDateTime.parse("2015-01-01T00:00:00.000Z"))
                                             .build())
                .build();
        mvc.perform(post("/" + VERSION + "/networks")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(foo))))
                .andExpect(status().isCreated());

        //Do it again, it should error
        assertThrows(ServletException.class, () -> {
            mvc.perform(post("/" + VERSION + "/networks")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(foo))))
                .andReturn();
        });

        mvc.perform(get("/" + VERSION + "/networks")
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().json("[{\"uuid\":\"" + NETWORK_UUID + "\",\"id\":\"foo\"}]"));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID)
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().json("[{\"id\":\"v\",\"num\":0}]"));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM)
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("data[0].id").value("foo"));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/substations")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(content().json("{data: []}"));

        Resource<SubstationAttributes> bar = Resource.substationBuilder().id("bar")
                .attributes(SubstationAttributes.builder()
                        .country(Country.FR)
                        .tso("RTE")
                        .entsoeArea(EntsoeAreaAttributes.builder().code("D7").build())
                        .build())
                .build();
        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/substations")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(bar))))
                .andExpect(status().isCreated());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/substations/bar")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("data[0].id").value("bar"))
                .andExpect(jsonPath("data[0].attributes.country").value("FR"))
                .andExpect(jsonPath("data[0].attributes.tso").value("RTE"))
                .andExpect(jsonPath("data[0].attributes.entsoeArea.code").value("D7"));

        Resource<SubstationAttributes> bar2 = Resource.substationBuilder()
                .id("bar2")
                .attributes(SubstationAttributes.builder()
                        .country(Country.BE)
                        .tso("ELIA")
                        .build())
                .build();
        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/substations")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(bar2))))
                .andExpect(status().isCreated());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/substations")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("meta.totalCount").value("2"))
                .andExpect(jsonPath("data", hasSize(2)))
                .andExpect(jsonPath("data[0].id").value("bar"))
                .andExpect(jsonPath("data[0].attributes.country").value("FR"))
                .andExpect(jsonPath("data[0].attributes.tso").value("RTE"))
                .andExpect(jsonPath("data[1].id").value("bar2"))
                .andExpect(jsonPath("data[1].attributes.country").value("BE"))
                .andExpect(jsonPath("data[1].attributes.tso").value("ELIA"));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/substations?limit=1")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("meta.totalCount").value("2"))
                .andExpect(jsonPath("data", hasSize(1)));

        List<InternalConnectionAttributes> ics1 = new ArrayList<>();
        ics1.add(InternalConnectionAttributes.builder()
                .node1(10)
                .node2(20)
                .build());

        List<CalculatedBusAttributes> cbs1 = new ArrayList<>();
        cbs1.add(CalculatedBusAttributes.builder()
            .connectedComponentNumber(7)
            .synchronousComponentNumber(3)
            .v(13.7)
            .angle(1.5)
            .vertices(Set.of(Vertex.builder().id("vId1").bus("vBus1").node(13).side("TWO").build()))
            .build());

        Resource<VoltageLevelAttributes> baz = Resource.voltageLevelBuilder()
                .id("baz")
                .attributes(VoltageLevelAttributes.builder()
                        .substationId("bar")
                        .nominalV(380)
                        .lowVoltageLimit(360)
                        .highVoltageLimit(400)
                        .topologyKind(TopologyKind.NODE_BREAKER)
                        .internalConnections(ics1)
                        .calculatedBusesForBusView(cbs1)
                        .build())
                .build();
        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/voltage-levels")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(baz))))
                .andExpect(status().isCreated());

        List<InternalConnectionAttributes> ics2 = new ArrayList<>();
        ics2.add(InternalConnectionAttributes.builder()
                .node1(12)
                .node2(22)
                .build());

        Resource<VoltageLevelAttributes> baz2 = Resource.voltageLevelBuilder()
                .id("baz2")
                .attributes(VoltageLevelAttributes.builder()
                        .substationId("bar2")
                        .nominalV(382)
                        .lowVoltageLimit(362)
                        .highVoltageLimit(402)
                        .topologyKind(TopologyKind.NODE_BREAKER)
                        .internalConnections(ics2)
                        .build())
                .build();
        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/voltage-levels")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(baz2))))
                .andExpect(status().isCreated());

        //no substation for this voltage level
        Resource<VoltageLevelAttributes> baz3 = Resource.voltageLevelBuilder()
                .id("baz3")
                .attributes(VoltageLevelAttributes.builder()
                        .nominalV(382)
                        .lowVoltageLimit(362)
                        .highVoltageLimit(402)
                        .topologyKind(TopologyKind.NODE_BREAKER)
                        .internalConnections(Collections.emptyList())
                        .build())
                .build();
        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/voltage-levels")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(baz3))))
                .andExpect(status().isCreated());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/substations/bar/voltage-levels")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("meta.totalCount").value("1"))
                .andExpect(jsonPath("data", hasSize(1)));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/voltage-levels")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data", hasSize(3)));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/voltage-levels/baz")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data", hasSize(1)))
                .andExpect(jsonPath("data[0].attributes.internalConnections[0].node1").value(10))
                .andExpect(jsonPath("data[0].attributes.internalConnections[0].node2").value(20))
                .andExpect(jsonPath("data[0].attributes.calculatedBusesForBusView[0].connectedComponentNumber").value(7))
                .andExpect(jsonPath("data[0].attributes.calculatedBusesForBusView[0].synchronousComponentNumber").value(3))
                .andExpect(jsonPath("data[0].attributes.calculatedBusesForBusView[0].v").value(13.7))
                .andExpect(jsonPath("data[0].attributes.calculatedBusesForBusView[0].angle").value(1.5))
                .andExpect(jsonPath("data[0].attributes.calculatedBusesForBusView[0].vertices[0].id").value("vId1"))
                .andExpect(jsonPath("data[0].attributes.calculatedBusesForBusView[0].vertices[0].bus").value("vBus1"))
                .andExpect(jsonPath("data[0].attributes.calculatedBusesForBusView[0].vertices[0].node").value(13))
                .andExpect(jsonPath("data[0].attributes.calculatedBusesForBusView[0].vertices[0].side").value("TWO"));

        mvc.perform(delete("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/switches/b1")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk());

        // switch creation and update
        Resource<SwitchAttributes> resBreaker = Resource.switchBuilder()
                .id("b1")
                .attributes(SwitchAttributes.builder()
                        .voltageLevelId("baz")
                        .kind(SwitchKind.BREAKER)
                        .node1(1)
                        .node2(2)
                        .open(false)
                        .retained(false)
                        .fictitious(false)
                        .build())
                .build();
        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/switches")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(resBreaker))))
                .andExpect(status().isCreated());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/switches/b1")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.open").value("false"));

        resBreaker.getAttributes().setOpen(true);  // opening the breaker switch
        mvc.perform(put("/" + VERSION + "/networks/" + NETWORK_UUID + "/switches")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(resBreaker))))
                .andExpect(status().isOk());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/switches/b1")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.open").value("true"));

        // line creation and update
        Resource<LineAttributes> resLine = Resource.lineBuilder()
            .id("idLine")
            .attributes(LineAttributes.builder()
                .voltageLevelId1("vl1")
                .voltageLevelId2("vl2")
                .name("idLine")
                .node1(1)
                .node2(1)
                .bus1("bus1")
                .bus2("bus2")
                .connectableBus1("bus1")
                .connectableBus2("bus2")
                .r(1)
                .x(1)
                .g1(1)
                .b1(1)
                .g2(1)
                .b2(1)
                .p1(0)
                .q1(0)
                .p2(0)
                .q2(0)
                .fictitious(true)
                .properties(new HashMap<>(Map.of("property1", "value1", "property2", "value2")))
                .aliasesWithoutType(new HashSet<>(Set.of("alias1")))
                .aliasByType(new HashMap<>(Map.of("aliasInt", "valueAliasInt", "aliasDouble", "valueAliasDouble")))
                .position1(ConnectablePositionAttributes.builder().label("labPosition1").order(1).direction(ConnectablePosition.Direction.BOTTOM).build())
                .position2(ConnectablePositionAttributes.builder().label("labPosition2").order(2).direction(ConnectablePosition.Direction.TOP).build())
                .mergedXnode(MergedXnodeAttributes.builder().rdp(50.).build())
                .selectedOperationalLimitsGroupId1("group1")
                .operationalLimitsGroups1(Map.of("group1", OperationalLimitsGroupAttributes.builder()
                        .id("group1")
                        .currentLimits(LimitsAttributes.builder().permanentLimit(20.).temporaryLimits(new TreeMap<>(Map.of(1200, TemporaryLimitAttributes.builder().value(30.).acceptableDuration(1200).build()))).build())
                        .build()))
                .build())
            .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/lines")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(resLine))))
                .andExpect(status().isCreated());
        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/lines/idLine")
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].id").value("idLine"))
                .andExpect(jsonPath("data[0].attributes.p1").value(0.))
                .andExpect(jsonPath("data[0].attributes.bus2").value("bus2"))
                .andExpect(jsonPath("data[0].attributes.node1").value(1))
                .andExpect(jsonPath("data[0].attributes.fictitious").value(true))
                .andExpect(jsonPath("data[0].attributes.properties[\"property1\"]").value("value1"))
                .andExpect(jsonPath("data[0].attributes.aliasByType[\"aliasDouble\"]").value("valueAliasDouble"))
                .andExpect(jsonPath("data[0].attributes.aliasesWithoutType").value("alias1"))
                .andExpect(jsonPath("data[0].attributes.position1.label").value("labPosition1"))
                .andExpect(jsonPath("data[0].attributes.position1.direction").value("BOTTOM"))
                .andExpect(jsonPath("data[0].attributes.position2.label").value("labPosition2"))
                .andExpect(jsonPath("data[0].attributes.position2.direction").value("TOP"))
                .andExpect(jsonPath("data[0].attributes.mergedXnode.rdp").value(50.0))
                .andExpect(jsonPath("data[0].attributes.operationalLimitsGroups1[\"group1\"].currentLimits.permanentLimit").value(20.));

        resLine.getAttributes().setP1(100.);  // changing p1 value
        resLine.getAttributes().getProperties().put("property1", "newValue1");  // changing property value
        mvc.perform(put("/" + VERSION + "/networks/" + NETWORK_UUID + "/lines")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(resLine))))
                .andExpect(status().isOk());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/lines/idLine")
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].id").value("idLine"))
                .andExpect(jsonPath("data[0].attributes.p1").value(100.))
                .andExpect(jsonPath("data[0].attributes.properties[\"property1\"]").value("newValue1"));

        // line creation Without Positions
        Resource<LineAttributes> lineWithoutFirstPositions = Resource.lineBuilder()
                .id("idLineWithoutFirstPosition")
                .attributes(LineAttributes.builder()
                        .voltageLevelId1("vl1")
                        .voltageLevelId2("vl2")
                        .name("idLineWithoutFirstPosition")
                        .node1(1)
                        .node2(1)
                        .bus1("bus1")
                        .bus2("bus2")
                        .connectableBus1("bus1")
                        .connectableBus2("bus2")
                        .r(1)
                        .x(1)
                        .g1(1)
                        .b1(1)
                        .g2(1)
                        .b2(1)
                        .p1(0)
                        .q1(0)
                        .p2(0)
                        .q2(0)
                        .fictitious(true)
                        .properties(new HashMap<>(Map.of("property1", "value1", "property2", "value2")))
                        .aliasesWithoutType(new HashSet<>(Set.of("alias1")))
                        .aliasByType(new HashMap<>(Map.of("aliasInt", "valueAliasInt", "aliasDouble", "valueAliasDouble")))
                        .position1(null)
                        .position2(null)
                        .mergedXnode(MergedXnodeAttributes.builder().rdp(50.).build())
                        .selectedOperationalLimitsGroupId1("group1")
                        .operationalLimitsGroups1(Map.of("group1", OperationalLimitsGroupAttributes.builder()
                                .id("group1")
                                .currentLimits(LimitsAttributes.builder().permanentLimit(20.).build())
                                .build()))
                        .build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/lines")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(lineWithoutFirstPositions))))
                .andExpect(status().isCreated());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/lines/idLineWithoutFirstPosition")
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].id").value("idLineWithoutFirstPosition"))
                .andExpect(jsonPath("data[0].attributes.p1").value(0.))
                .andExpect(jsonPath("data[0].attributes.bus2").value("bus2"))
                .andExpect(jsonPath("data[0].attributes.node1").value(1))
                .andExpect(jsonPath("data[0].attributes.fictitious").value(true))
                .andExpect(jsonPath("data[0].attributes.properties[\"property1\"]").value("value1"))
                .andExpect(jsonPath("data[0].attributes.aliasByType[\"aliasDouble\"]").value("valueAliasDouble"))
                .andExpect(jsonPath("data[0].attributes.aliasesWithoutType").value("alias1"))
                .andExpect(jsonPath("data[0].attributes.mergedXnode.rdp").value(50.0))
                .andExpect(jsonPath("data[0].attributes.operationalLimitsGroups1[\"group1\"].currentLimits.permanentLimit").value(20.));

        Resource<LineAttributes> resLine2 = Resource.lineBuilder()
            .id("idLine2")
            .attributes(LineAttributes.builder()
                .voltageLevelId1("vl12")
                .voltageLevelId2("vl22")
                .name("idLine2")
                .node1(5)
                .node2(7)
                .bus1("bus12")
                .bus2("bus22")
                .connectableBus1("bus12")
                .connectableBus2("bus22")
                .r(8)
                .x(9)
                .g1(3)
                .b1(12)
                .g2(1)
                .b2(1)
                .p1(30)
                .q1(0)
                .p2(0)
                .q2(0)
                .fictitious(false)
                .properties(new HashMap<>(Map.of("property12", "value12", "property22", "value22")))
                .aliasesWithoutType(new HashSet<>(Set.of("alias12")))
                .aliasByType(new HashMap<>(Map.of("aliasInt2", "valueAliasInt2", "aliasDouble2", "valueAliasDouble2")))
                .position1(ConnectablePositionAttributes.builder().label("labPosition12").order(4).direction(ConnectablePosition.Direction.BOTTOM).build())
                .position2(ConnectablePositionAttributes.builder().label("labPosition22").order(9).direction(ConnectablePosition.Direction.TOP).build())
                .mergedXnode(MergedXnodeAttributes.builder().rdp(80.).build())
                .selectedOperationalLimitsGroupId1("group1")
                .operationalLimitsGroups1(Map.of("group1", OperationalLimitsGroupAttributes.builder()
                        .id("group1")
                        .currentLimits(LimitsAttributes.builder().permanentLimit(20.).build())
                        .build()))
                .build())
            .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/lines")
            .contentType(APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(Collections.singleton(resLine2))))
            .andExpect(status().isCreated());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/lines")
            .contentType(APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
            .andExpect(jsonPath("data[0].id").value("idLine"))
            .andExpect(jsonPath("data[0].attributes.p1").value(100.))
            .andExpect(jsonPath("data[0].attributes.properties[\"property1\"]").value("newValue1"))
            .andExpect(jsonPath("data[1].id").value("idLineWithoutFirstPosition"))
            .andExpect(jsonPath("data[1].attributes.p1").value(0.))
            .andExpect(jsonPath("data[1].attributes.properties[\"property1\"]").value("value1"))
            .andExpect(jsonPath("data[2].id").value("idLine2"))
            .andExpect(jsonPath("data[2].attributes.p1").value(30.))
            .andExpect(jsonPath("data[2].attributes.properties[\"property12\"]").value("value12"));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/voltage-levels/vl1/lines")
            .contentType(APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
            .andExpect(jsonPath("data[0].id").value("idLine"))
            .andExpect(jsonPath("data[0].attributes.voltageLevelId1").value("vl1"))
            .andExpect(jsonPath("data[0].attributes.voltageLevelId2").value("vl2"))
            .andExpect(jsonPath("data[0].attributes.p1").value(100.))
            .andExpect(jsonPath("data[0].attributes.properties[\"property1\"]").value("newValue1"));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/voltage-levels/vl12/lines")
            .contentType(APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
            .andExpect(jsonPath("data[0].id").value("idLine2"))
            .andExpect(jsonPath("data[0].attributes.voltageLevelId1").value("vl12"))
            .andExpect(jsonPath("data[0].attributes.voltageLevelId2").value("vl22"));

        // generator creation and update
        RegulatingPointAttributes regulatingPointAttributes = RegulatingPointAttributes.builder()
            .regulatedEquipmentId("id")
            .regulatingTerminal(TerminalRefAttributes.builder().connectableId("idEq").side("ONE").build())
            .localTerminal(TerminalRefAttributes.builder().connectableId("id").build())
            .build();
        Resource<GeneratorAttributes> generator = Resource.generatorBuilder()
                .id("id")
                .attributes(GeneratorAttributes.builder()
                        .voltageLevelId("vl1")
                        .name("gen1")
                        .energySource(EnergySource.HYDRO)
                        .reactiveLimits(MinMaxReactiveLimitsAttributes.builder().maxQ(10).minQ(10).build())
                        .regulatingPoint(regulatingPointAttributes)
                        .build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/generators")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(generator))))
                .andExpect(status().isCreated());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/generators")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.regulatingPoint.regulatingTerminal.connectableId").value("idEq"))
                .andExpect(jsonPath("data[0].attributes.regulatingPoint.regulatingTerminal.side").value("ONE"))
                .andExpect(jsonPath("data[0].attributes.regulatingPoint.localTerminal.connectableId").value("id"))
                .andExpect(jsonPath("data[0].attributes.reactiveLimits.kind").value("MIN_MAX"))
                .andExpect(jsonPath("data[0].attributes.reactiveLimits.minQ").value(10.))
                .andExpect(jsonPath("data[0].attributes.reactiveLimits.maxQ").value(10.));

        generator.getAttributes().getRegulatingPoint().getRegulatingTerminal().setConnectableId("idEq2");
        generator.getAttributes().getRegulatingPoint().getRegulatingTerminal().setSide("TWO");
        generator.getAttributes().setReactiveLimits(ReactiveCapabilityCurveAttributes.builder()
            .points(new TreeMap<>(Map.of(
                    50., ReactiveCapabilityCurvePointAttributes.builder().p(50.).minQ(11.).maxQ(76.).build(),
                    50.12, ReactiveCapabilityCurvePointAttributes.builder().p(50.12).minQ(11.12).maxQ(76.12).build()
            ))).build());

        mvc.perform(put("/" + VERSION + "/networks/" + NETWORK_UUID + "/generators")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(generator))))
                .andExpect(status().isOk());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/generators")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.regulatingPoint.regulatingTerminal.connectableId").value("idEq2"))
                .andExpect(jsonPath("data[0].attributes.regulatingPoint.regulatingTerminal.side").value("TWO"))
                .andExpect(jsonPath("data[0].attributes.regulatingPoint.localTerminal.connectableId").value("id"))
                .andExpect(jsonPath("data[0].attributes.reactiveLimits.kind").value("CURVE"))
                .andExpect(jsonPath("data[0].attributes.reactiveLimits.points[\"50.0\"].p").value(50.))
                .andExpect(jsonPath("data[0].attributes.reactiveLimits.points[\"50.0\"].minQ").value(11.))
                .andExpect(jsonPath("data[0].attributes.reactiveLimits.points[\"50.0\"].maxQ").value(76.))
                .andExpect(jsonPath("data[0].attributes.reactiveLimits.points[\"50.12\"].p").value(50.12))
                .andExpect(jsonPath("data[0].attributes.reactiveLimits.points[\"50.12\"].minQ").value(11.12))
                .andExpect(jsonPath("data[0].attributes.reactiveLimits.points[\"50.12\"].maxQ").value(76.12));

        // battery creation and update
        Resource<BatteryAttributes> battery = Resource.batteryBuilder()
                .id("batteryId")
                .attributes(BatteryAttributes.builder()
                        .voltageLevelId("vl1")
                        .name("battery1")
                        .targetP(250)
                        .targetQ(100)
                        .maxP(500)
                        .minP(100)
                        .reactiveLimits(MinMaxReactiveLimitsAttributes.builder().maxQ(10).minQ(10).build())
                        .build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/batteries")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(battery))))
                .andExpect(status().isCreated());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/batteries")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.targetP").value("250.0"))
                .andExpect(jsonPath("data[0].attributes.targetQ").value("100.0"))
                .andExpect(jsonPath("data[0].attributes.maxP").value("500.0"))
                .andExpect(jsonPath("data[0].attributes.minP").value("100.0"));

        battery.getAttributes().setP(310);
        battery.getAttributes().setQ(120);
        mvc.perform(put("/" + VERSION + "/networks/" + NETWORK_UUID + "/batteries")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(battery))))
                .andExpect(status().isOk());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/batteries/" + battery.getId())
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.targetP").value("250.0"))
                .andExpect(jsonPath("data[0].attributes.targetQ").value("100.0"))
                .andExpect(jsonPath("data[0].attributes.maxP").value("500.0"))
                .andExpect(jsonPath("data[0].attributes.minP").value("100.0"))
                .andExpect(jsonPath("data[0].attributes.p").value("310.0"))
                .andExpect(jsonPath("data[0].attributes.q").value("120.0"));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/voltage-levels/vl1/batteries")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.targetP").value("250.0"))
                .andExpect(jsonPath("data[0].attributes.targetQ").value("100.0"))
                .andExpect(jsonPath("data[0].attributes.maxP").value("500.0"))
                .andExpect(jsonPath("data[0].attributes.minP").value("100.0"))
                .andExpect(jsonPath("data[0].attributes.p").value("310.0"))
                .andExpect(jsonPath("data[0].attributes.q").value("120.0"));

        mvc.perform(delete("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/batteries/battery1")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk());

        // shunt compensator creation and update
        Resource<ShuntCompensatorAttributes> shuntCompensator = Resource.shuntCompensatorBuilder()
                .id("idShunt")
                .attributes(ShuntCompensatorAttributes.builder()
                        .voltageLevelId("vl1")
                        .name("shunt1")
                        .model(ShuntCompensatorLinearModelAttributes.builder().bPerSection(1).gPerSection(2).maximumSectionCount(3).build())
                        .p(100.)
                        .build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/shunt-compensators")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(shuntCompensator))))
                .andExpect(status().isCreated());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/shunt-compensators")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.model.bperSection").value(1))
                .andExpect(jsonPath("data[0].attributes.model.gperSection").value(2))
                .andExpect(jsonPath("data[0].attributes.model.maximumSectionCount").value(3))
                .andExpect(jsonPath("data[0].attributes.p").value(100.));

        ((ShuntCompensatorLinearModelAttributes) shuntCompensator.getAttributes().getModel()).setBPerSection(15); // changing bPerSection value
        ((ShuntCompensatorLinearModelAttributes) shuntCompensator.getAttributes().getModel()).setGPerSection(22); // changing gPerSection value
        shuntCompensator.getAttributes().setP(200.);  // changing p value

        mvc.perform(put("/" + VERSION + "/networks/" + NETWORK_UUID + "/shunt-compensators")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(shuntCompensator))))
                .andExpect(status().isOk());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/shunt-compensators")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.model.bperSection").value(15))
                .andExpect(jsonPath("data[0].attributes.model.gperSection").value(22))
                .andExpect(jsonPath("data[0].attributes.p").value(200.));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/shunt-compensators/idShunt")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.model.bperSection").value(15))
                .andExpect(jsonPath("data[0].attributes.model.gperSection").value(22))
                .andExpect(jsonPath("data[0].attributes.p").value(200.));
        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/voltage-levels/vl1/shunt-compensators")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.model.bperSection").value(15))
                .andExpect(jsonPath("data[0].attributes.model.gperSection").value(22))
                .andExpect(jsonPath("data[0].attributes.p").value(200.));

        // dangling line creation and update
        Resource<DanglingLineAttributes> danglingLine = Resource.danglingLineBuilder()
                .id("idDanglingLine")
                .attributes(DanglingLineAttributes.builder()
                        .voltageLevelId("vl1")
                        .name("dl1")
                        .fictitious(true)
                        .node(5)
                        .p0(10)
                        .q0(20)
                        .r(6)
                        .x(7)
                        .g(8)
                        .b(9)
                        .generation(DanglingLineGenerationAttributes.builder()
                                .minP(1)
                                .maxP(2)
                                .targetP(3)
                                .targetQ(4)
                                .targetV(5)
                                .voltageRegulationOn(false)
                                .reactiveLimits(MinMaxReactiveLimitsAttributes.builder().minQ(20).maxQ(30).build())
                                .build())
                        .pairingKey("XN1")
                        .selectedOperationalLimitsGroupId("group1")
                        .operationalLimitsGroups(Map.of("group1", OperationalLimitsGroupAttributes.builder()
                                .id("group1")
                                .currentLimits(LimitsAttributes.builder().permanentLimit(20.).build())
                                .build()))
                        .p(100.)
                        .q(200)
                        .build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/dangling-lines")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(danglingLine))))
                .andExpect(status().isCreated());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/dangling-lines")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.p0").value(10))
                .andExpect(jsonPath("data[0].attributes.g").value(8))
                .andExpect(jsonPath("data[0].attributes.generation.maxP").value(2))
                .andExpect(jsonPath("data[0].attributes.generation.targetV").value(5))
                .andExpect(jsonPath("data[0].attributes.generation.voltageRegulationOn").value(false))
                .andExpect(jsonPath("data[0].attributes.generation.reactiveLimits.maxQ").value(30));

        danglingLine.getAttributes().getGeneration().setMaxP(33);
        danglingLine.getAttributes().getGeneration().setVoltageRegulationOn(true);
        danglingLine.getAttributes().getGeneration().setTargetQ(54);

        mvc.perform(put("/" + VERSION + "/networks/" + NETWORK_UUID + "/dangling-lines")
                .contentType(APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(Collections.singleton(danglingLine))))
                .andExpect(status().isOk());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/dangling-lines")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.generation.maxP").value(33))
                .andExpect(jsonPath("data[0].attributes.generation.targetQ").value(54))
                .andExpect(jsonPath("data[0].attributes.generation.voltageRegulationOn").value(true));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/dangling-lines/idDanglingLine")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.generation.maxP").value(33))
                .andExpect(jsonPath("data[0].attributes.generation.targetQ").value(54))
                .andExpect(jsonPath("data[0].attributes.generation.voltageRegulationOn").value(true));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/voltage-levels/vl1/dangling-lines")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.generation.maxP").value(33))
                .andExpect(jsonPath("data[0].attributes.generation.targetQ").value(54))
                .andExpect(jsonPath("data[0].attributes.generation.voltageRegulationOn").value(true));

        // ground creation and update
        Resource<GroundAttributes> ground = Resource.groundBuilder()
                .id("idGround")
                .attributes(GroundAttributes.builder()
                        .voltageLevelId("vl1")
                        .name("ground1")
                        .fictitious(true)
                        .node(5)
                        .p(10)
                        .q(20)
                        .position(ConnectablePositionAttributes.builder()
                                .direction(ConnectablePosition.Direction.BOTTOM)
                                .label("label")
                                .order(1)
                                .build())
                        .build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/grounds")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(ground))))
                .andExpect(status().isCreated());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/grounds")
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.p").value(10))
                .andExpect(jsonPath("data[0].attributes.q").value(20))
                .andExpect(jsonPath("data[0].attributes.node").value(5))
                .andExpect(jsonPath("data[0].attributes.fictitious").value(true))
                .andExpect(jsonPath("data[0].attributes.voltageLevelId").value("vl1"))
                .andExpect(jsonPath("data[0].attributes.position.order").value(1));

        ground.getAttributes().setP(30);
        ground.getAttributes().setQ(40);
        ground.getAttributes().setNode(6);

        mvc.perform(put("/" + VERSION + "/networks/" + NETWORK_UUID + "/grounds")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(ground))))
                .andExpect(status().isOk());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/grounds")
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.p").value(30))
                .andExpect(jsonPath("data[0].attributes.q").value(40))
                .andExpect(jsonPath("data[0].attributes.node").value(6))
                .andExpect(jsonPath("data[0].attributes.voltageLevelId").value("vl1"));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/grounds/idGround")
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.p").value(30))
                .andExpect(jsonPath("data[0].attributes.q").value(40))
                .andExpect(jsonPath("data[0].attributes.node").value(6));

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/voltage-levels/vl1/grounds")
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.p").value(30))
                .andExpect(jsonPath("data[0].attributes.q").value(40))
                .andExpect(jsonPath("data[0].attributes.node").value(6));

        // Test removals
        mvc.perform(delete("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/switches/b1")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk());

        mvc.perform(delete("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/voltage-levels/baz")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk());

        mvc.perform(delete("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/substations/bar")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk());

        // tie line creation and update
        Resource<TieLineAttributes> tieLine = Resource.tieLineBuilder()
                .id("idTieLine")
                .attributes(TieLineAttributes.builder().name("TieLine").fictitious(false).danglingLine1Id("half1").danglingLine2Id("half2")
                        .build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/tie-lines")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(tieLine))))
                .andExpect(status().isCreated());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/tie-lines")
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.danglingLine1Id").value("half1"))
                .andExpect(jsonPath("data[0].attributes.danglingLine2Id").value("half2"));

        tieLine.getAttributes().setDanglingLine1Id("halfDl1");
        mvc.perform(put("/" + VERSION + "/networks/" + NETWORK_UUID + "/tie-lines")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(tieLine))))
                .andExpect(status().isOk());

        mvc.perform(delete("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + Resource.INITIAL_VARIANT_NUM + "/tie-lines/idTieLine")
                .contentType(APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    public void networkCloneVariantTest() throws Exception {
        // create a simple network with just one substation
        Resource<NetworkAttributes> n1 = Resource.networkBuilder()
                .id("n1")
                .variantNum(0)
                .attributes(NetworkAttributes.builder()
                        .uuid(NETWORK_UUID)
                        .variantId(VariantManagerConstants.INITIAL_VARIANT_ID)
                        .caseDate(ZonedDateTime.parse("2015-01-01T00:00:00.000Z"))
                        .build())
                .build();
        mvc.perform(post("/" + VERSION + "/networks")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(n1))))
                .andExpect(status().isCreated());

        Resource<SubstationAttributes> s1 = Resource.substationBuilder()
                .id("s1")
                .attributes(SubstationAttributes.builder()
                        .country(Country.FR)
                        .tso("RTE")
                        .build())
                .build();
        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/substations")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(s1))))
                .andExpect(status().isCreated());

        // clone the initial variant
        mvc.perform(put("/" + VERSION + "/networks/" + NETWORK_UUID + "/" + 0 + "/to/" + 1 + "?targetVariantId=v"))
                .andExpect(status().isOk());

        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID)
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(content().json("[{\"id\":\"InitialState\",\"num\":0},{\"id\":\"v\",\"num\":1}]"));
    }

    @Test
    public void cloneNetworkTest() throws Exception {
        //Initialize network
        Resource<NetworkAttributes> n1 = Resource.networkBuilder()
                .id("n1")
                .variantNum(0)
                .attributes(NetworkAttributes.builder()
                        .uuid(NETWORK_UUID)
                        .variantId(VariantManagerConstants.INITIAL_VARIANT_ID)
                        .caseDate(ZonedDateTime.parse("2015-01-01T00:00:00.000Z"))
                        .build())
                .build();
        mvc.perform(post("/" + VERSION + "/networks")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(n1))))
                .andExpect(status().isCreated());
        RegulatingPointAttributes regulatingPointAttributes = RegulatingPointAttributes.builder()
            .regulatedEquipmentId("id")
            .resourceType(ResourceType.GENERATOR)
            .regulatingTerminal(TerminalRefAttributes.builder().connectableId("idEq").side("ONE").build())
            .localTerminal(TerminalRefAttributes.builder().connectableId("id").build())
            .build();
        Resource<GeneratorAttributes> generator = Resource.generatorBuilder()
                .id("id")
                .attributes(GeneratorAttributes.builder()
                        .voltageLevelId("vl1")
                        .name("gen1")
                        .energySource(EnergySource.HYDRO)
                        .reactiveLimits(MinMaxReactiveLimitsAttributes.builder().maxQ(10).minQ(10).build())
                        .regulatingPoint(regulatingPointAttributes)
                        .build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/generators")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(generator))))
                .andExpect(status().isCreated());

        //Set up second variant
        Resource<NetworkAttributes> n2 = Resource.networkBuilder()
                .id("n2")
                .variantNum(1)
                .attributes(NetworkAttributes.builder()
                        .uuid(NETWORK_UUID)
                        .variantId("v2")
                        .caseDate(ZonedDateTime.parse("2015-01-01T00:00:00.000Z"))
                        .build())
                .build();
        mvc.perform(post("/" + VERSION + "/networks")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(n2))))
                .andExpect(status().isCreated());

        //Set up third variant
        Resource<NetworkAttributes> n3 = Resource.networkBuilder()
                .id("n3")
                .variantNum(2)
                .attributes(NetworkAttributes.builder()
                        .uuid(NETWORK_UUID)
                        .variantId("v3")
                        .caseDate(ZonedDateTime.parse("2015-01-01T00:00:00.000Z"))
                        .build())
                .build();
        mvc.perform(post("/" + VERSION + "/networks")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(n3))))
                .andExpect(status().isCreated());

        Resource<ShuntCompensatorAttributes> shuntCompensator = Resource.shuntCompensatorBuilder()
                .id("idShunt")
                .attributes(ShuntCompensatorAttributes.builder()
                        .voltageLevelId("vl1")
                        .name("shunt1")
                        .model(ShuntCompensatorLinearModelAttributes.builder().bPerSection(1).gPerSection(2).maximumSectionCount(3).build())
                        .p(100.)
                        .build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/shunt-compensators")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(shuntCompensator))))
                .andExpect(status().isCreated());

        //Clone the third variant
        UUID clonedNetworkUuid = UUID.randomUUID();
        mvc.perform(post("/" + VERSION + "/networks/" + clonedNetworkUuid + "?duplicateFrom=" + NETWORK_UUID + "&targetVariantIds=" + String.join(",", List.of("v2", "v3", "nonExistingVariant")))
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk());

        mvc.perform(get("/" + VERSION + "/networks/" + clonedNetworkUuid)
                        .contentType(APPLICATION_JSON))
                 .andExpect(status().isOk())
                 .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                 .andExpect(content().json("[{\"id\":\"v2\",\"num\":0},{\"id\":\"v3\",\"num\":1}]"));

        //Check the generator is present in the cloned network
        mvc.perform(get("/" + VERSION + "/networks/" + clonedNetworkUuid + "/" + 1 + "/generators")
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.regulatingPoint.regulatingTerminal.connectableId").value("idEq"))
                .andExpect(jsonPath("data[0].attributes.regulatingPoint.regulatingTerminal.side").value("ONE"))
                .andExpect(jsonPath("data[0].attributes.regulatingPoint.localTerminal.connectableId").value("id"))
                .andExpect(jsonPath("data[0].attributes.reactiveLimits.kind").value("MIN_MAX"))
                .andExpect(jsonPath("data[0].attributes.reactiveLimits.minQ").value(10.))
                .andExpect(jsonPath("data[0].attributes.reactiveLimits.maxQ").value(10.));

        //Check the shunt is present in the cloned network
        mvc.perform(get("/" + VERSION + "/networks/" + clonedNetworkUuid + "/" + 1 + "/shunt-compensators")
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].attributes.model.bperSection").value(1))
                .andExpect(jsonPath("data[0].attributes.model.gperSection").value(2))
                .andExpect(jsonPath("data[0].attributes.model.maximumSectionCount").value(3))
                .andExpect(jsonPath("data[0].attributes.p").value(100.));
    }

    @Test
    public void getExtensionAttributesTest() throws Exception {
        setupExtensionAttributesTest();
        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/0/identifiables/id/extensions/" + ActivePowerControl.NAME))
                .andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON))
                .andExpect(jsonPath("data[0].extensionName").value(ActivePowerControl.NAME))
                .andExpect(jsonPath("data[0].participate").value(true))
                .andExpect(jsonPath("data[0].droop").value(24))
                .andExpect(jsonPath("data[0].participationFactor").value(0.6))
                .andExpect(jsonPath("data[0].minTargetP").value(4.0))
                .andExpect(jsonPath("data[0].maxTargetP").value(6.0));
    }

    @Test
    public void getAllExtensionsAttributesByResourceTypeAndExtensionNameTest() throws Exception {
        setupExtensionAttributesTest();
        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/0/identifiables/types/" + ResourceType.GENERATOR + "/extensions/" + ActivePowerControl.NAME))
                .andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON))
                .andExpect(jsonPath("$.id.extensionName").value(ActivePowerControl.NAME))
                .andExpect(jsonPath("$.id.participate").value(true))
                .andExpect(jsonPath("$.id.droop").value(24))
                .andExpect(jsonPath("$.id.participationFactor").value(0.6))
                .andExpect(jsonPath("$.id.minTargetP").value(4.0))
                .andExpect(jsonPath("$.id.maxTargetP").value(6.0))
                .andExpect(jsonPath("$.id2.extensionName").value(ActivePowerControl.NAME))
                .andExpect(jsonPath("$.id2.participate").value(false))
                .andExpect(jsonPath("$.id2.droop").value(12))
                .andExpect(jsonPath("$.id2.participationFactor").value(0.7))
                .andExpect(jsonPath("$.id2.minTargetP").value(8.0))
                .andExpect(jsonPath("$.id2.maxTargetP").value(10.0));
    }

    @Test
    public void getAllExtensionsAttributesByIdentifiableIdTest() throws Exception {
        setupExtensionAttributesTest();
        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/0/identifiables/id/extensions"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON))
                .andExpect(jsonPath("$.activePowerControl.extensionName").value(ActivePowerControl.NAME))
                .andExpect(jsonPath("$.activePowerControl.participate").value(true))
                .andExpect(jsonPath("$.activePowerControl.droop").value(24))
                .andExpect(jsonPath("$.activePowerControl.participationFactor").value(0.6))
                .andExpect(jsonPath("$.activePowerControl.minTargetP").value(4.0))
                .andExpect(jsonPath("$.activePowerControl.maxTargetP").value(6.0))
                .andExpect(jsonPath("$.startup.extensionName").value(GeneratorStartup.NAME))
                .andExpect(jsonPath("$.startup.plannedActivePowerSetpoint").value(12.0))
                .andExpect(jsonPath("$.startup.startupCost").value(34.0))
                .andExpect(jsonPath("$.startup.marginalCost").value(5.0))
                .andExpect(jsonPath("$.startup.plannedOutageRate").value(6.0))
                .andExpect(jsonPath("$.startup.forcedOutageRate").value(8.0));
    }

    @Test
    public void getAllExtensionsAttributesByResourceTypeTest() throws Exception {
        setupExtensionAttributesTest();
        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/0/identifiables/types/" + ResourceType.GENERATOR + "/extensions"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(APPLICATION_JSON))
                .andExpect(jsonPath("$.id.activePowerControl.extensionName").value(ActivePowerControl.NAME))
                .andExpect(jsonPath("$.id.activePowerControl.participate").value(true))
                .andExpect(jsonPath("$.id.activePowerControl.droop").value(24))
                .andExpect(jsonPath("$.id.activePowerControl.participationFactor").value(0.6))
                .andExpect(jsonPath("$.id.activePowerControl.minTargetP").value(4.0))
                .andExpect(jsonPath("$.id.activePowerControl.maxTargetP").value(6.0))
                .andExpect(jsonPath("$.id.startup.extensionName").value(GeneratorStartup.NAME))
                .andExpect(jsonPath("$.id.startup.plannedActivePowerSetpoint").value(12.0))
                .andExpect(jsonPath("$.id.startup.startupCost").value(34.0))
                .andExpect(jsonPath("$.id.startup.marginalCost").value(5.0))
                .andExpect(jsonPath("$.id.startup.plannedOutageRate").value(6.0))
                .andExpect(jsonPath("$.id.startup.forcedOutageRate").value(8.0))
                .andExpect(jsonPath("$.id2.activePowerControl.extensionName").value(ActivePowerControl.NAME))
                .andExpect(jsonPath("$.id2.activePowerControl.participate").value(false))
                .andExpect(jsonPath("$.id2.activePowerControl.droop").value(12))
                .andExpect(jsonPath("$.id2.activePowerControl.participationFactor").value(0.7))
                .andExpect(jsonPath("$.id2.activePowerControl.minTargetP").value(8.0))
                .andExpect(jsonPath("$.id2.activePowerControl.maxTargetP").value(10.0))
                .andExpect(jsonPath("$.id2.startup.extensionName").value(GeneratorStartup.NAME))
                .andExpect(jsonPath("$.id2.startup.plannedActivePowerSetpoint").value(10.0))
                .andExpect(jsonPath("$.id2.startup.startupCost").value(23.0))
                .andExpect(jsonPath("$.id2.startup.marginalCost").value(7.0))
                .andExpect(jsonPath("$.id2.startup.plannedOutageRate").value(9.0))
                .andExpect(jsonPath("$.id2.startup.forcedOutageRate").value(10.0));

    }

    @Test
    public void removeExtensionAttributesTest() throws Exception {
        setupExtensionAttributesTest();
        mvc.perform(delete("/" + VERSION + "/networks/" + NETWORK_UUID + "/0/identifiables/id/extensions/" + ActivePowerControl.NAME))
                .andExpect(status().isOk());
        mvc.perform(get("/" + VERSION + "/networks/" + NETWORK_UUID + "/0/identifiables/id/extensions/" + ActivePowerControl.NAME))
                .andExpect(status().isNotFound());
    }

    private void setupExtensionAttributesTest() throws Exception {
        // Create network
        Resource<NetworkAttributes> n1 = Resource.networkBuilder()
                .id("n1")
                .variantNum(0)
                .attributes(NetworkAttributes.builder()
                        .uuid(NETWORK_UUID)
                        .variantId(VariantManagerConstants.INITIAL_VARIANT_ID)
                        .caseDate(ZonedDateTime.parse("2015-01-01T00:00:00.000Z"))
                        .build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(n1))))
                .andExpect(status().isCreated());
        // Create first generator with two extensions
        Resource<GeneratorAttributes> generator = Resource.generatorBuilder()
                .id("id")
                .attributes(GeneratorAttributes.builder()
                        .voltageLevelId("vl1")
                        .name("gen1")
                        .energySource(EnergySource.HYDRO)
                        .extensionAttributes(Map.of(
                                ActivePowerControl.NAME, new ActivePowerControlAttributes(true, 24, 0.6, 4, 6),
                                GeneratorStartup.NAME, new GeneratorStartupAttributes(12, 34, 5, 6, 8)))
                        .build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/generators")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(generator))))
                .andExpect(status().isCreated());

        // Create second generator with two extensions
        Resource<GeneratorAttributes> generator2 = Resource.generatorBuilder()
                .id("id2")
                .attributes(GeneratorAttributes.builder()
                        .voltageLevelId("vl1")
                        .name("gen2")
                        .energySource(EnergySource.NUCLEAR)
                        .extensionAttributes(Map.of(
                                ActivePowerControl.NAME, new ActivePowerControlAttributes(false, 12, 0.7, 8, 10),
                                GeneratorStartup.NAME, new GeneratorStartupAttributes(10, 23, 7, 9, 10)
                        ))
                        .build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/generators")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(generator2))))
                .andExpect(status().isCreated());
    }
}
