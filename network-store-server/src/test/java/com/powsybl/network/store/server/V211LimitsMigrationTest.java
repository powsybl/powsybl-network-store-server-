/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Lists;
import com.powsybl.iidm.network.LimitType;
import com.powsybl.iidm.network.VariantManagerConstants;
import com.powsybl.network.store.model.*;
import com.powsybl.network.store.server.dto.LimitsInfos;
import com.powsybl.network.store.server.dto.OwnerInfo;
import com.powsybl.network.store.server.dto.PermanentLimitAttributes;
import com.powsybl.network.store.server.exceptions.UncheckedSqlException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.powsybl.network.store.model.NetworkStoreApi.VERSION;
import static com.powsybl.network.store.server.NetworkStoreRepository.BATCH_SIZE;
import static com.powsybl.network.store.server.QueryCatalog.*;
import static com.powsybl.network.store.server.Utils.bindValues;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@SpringBootTest
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
class V211LimitsMigrationTest {

    private static final UUID NETWORK_UUID = UUID.fromString("7928181c-7977-4592-ba19-88027e4254e4");

    @Autowired
    private NetworkStoreRepository networkStoreRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MockMvc mvc;

    // to be kept ???
    @BeforeEach
    void setup() {
        this.objectMapper.registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                .configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false);
    }

    @Test
    void insertTemporaryLimitsInLinesTest() throws Exception {
        createNetwork();
        createLine();
        createDanglineLine();
        create2WTLine();
        create3WTLine();

        LimitsInfos limits = new LimitsInfos();
        List<TemporaryLimitAttributes> temporaryLimitsA = new ArrayList<>();
        TemporaryLimitAttributes templimitAOkSide1a = TemporaryLimitAttributes.builder()
                .side(1)
                .acceptableDuration(100)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .build();

        TemporaryLimitAttributes templimitAOkSide2a = TemporaryLimitAttributes.builder()
                .side(2)
                .acceptableDuration(100)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .build();

        TemporaryLimitAttributes templimitAOkSide2b = TemporaryLimitAttributes.builder()
                .side(2)
                .acceptableDuration(200)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .build();
        temporaryLimitsA.add(templimitAOkSide1a);
        temporaryLimitsA.add(templimitAOkSide2a);
        temporaryLimitsA.add(templimitAOkSide2b);
        limits.setTemporaryLimits(temporaryLimitsA);

        PermanentLimitAttributes permanentLimitAttributes = PermanentLimitAttributes.builder()
                .side(1)
                .limitType(LimitType.CURRENT)
                .operationalLimitsGroupId("group1")
                .value(300)
                .build();
        limits.setPermanentLimits(List.of(permanentLimitAttributes));

        insertLimitsOldFashion("l1", ResourceType.LINE, limits);
        insertLimitsOldFashion("dl1", ResourceType.DANGLING_LINE, limits);
        insertLimitsOldFashion("2wt", ResourceType.TWO_WINDINGS_TRANSFORMER, limits);
        insertLimitsOldFashion("3wt", ResourceType.THREE_WINDINGS_TRANSFORMER, limits);

        mvc.perform(MockMvcRequestBuilders.put("/" + VERSION + "/migration/" + NETWORK_UUID)
                        .contentType(APPLICATION_JSON))
                .andExpect(status().isOk());

        assertEquals(0, countPermanentLimitsOldFashion("l1"));
        assertEquals(0, countPermanentLimitsOldFashion("dl1"));
        assertEquals(0, countPermanentLimitsOldFashion("2wt"));
        assertEquals(0, countPermanentLimitsOldFashion("3wt"));

        //TODO: check limits in new table
    }

    private void insertLimitsOldFashion(String equipmentId, ResourceType resourceType, LimitsInfos limits) {
        OwnerInfo ownerInfo = new OwnerInfo(equipmentId, resourceType, NETWORK_UUID, 0);
        insertPermanentLimitsOldFashion(Map.of(ownerInfo, limits));
        insertTemporaryLimitsOldFashion(Map.of(ownerInfo, limits));
    }

    public void createNetwork() throws Exception {
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
    }

    public void createLine() throws Exception {
        Resource<LineAttributes> resLine = Resource.lineBuilder()
                .id("l1")
                .attributes(LineAttributes.builder().build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/lines")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(resLine))))
                .andExpect(status().isCreated());
    }

    public void createDanglineLine() throws Exception {
        Resource<DanglingLineAttributes> danglingLine = Resource.danglingLineBuilder()
                .id("dl1")
                .attributes(DanglingLineAttributes.builder().build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/dangling-lines")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(danglingLine))))
                .andExpect(status().isCreated());
    }

    public void create2WTLine() throws Exception {
        Resource<TwoWindingsTransformerAttributes> twoWindingsTransformer = Resource.twoWindingsTransformerBuilder()
                .id("2wt")
                .attributes(TwoWindingsTransformerAttributes.builder().build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/2-windings-transformers")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(twoWindingsTransformer))))
                .andExpect(status().isCreated());
    }

    public void create3WTLine() throws Exception {
        Resource<ThreeWindingsTransformerAttributes> twoWindingsTransformer = Resource.threeWindingsTransformerBuilder()
                .id("3wt")
                .attributes(ThreeWindingsTransformerAttributes.builder()
                        .leg1(new LegAttributes())
                        .leg2(new LegAttributes())
                        .leg3(new LegAttributes())
                        .build())
                .build();

        mvc.perform(post("/" + VERSION + "/networks/" + NETWORK_UUID + "/3-windings-transformers")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Collections.singleton(twoWindingsTransformer))))
                .andExpect(status().isCreated());
    }

    public static String buildInsertTemporaryLimitsOldFashionQuery() {
        return "insert into temporarylimit(" +
                EQUIPMENT_ID_COLUMN + ", " + EQUIPMENT_TYPE_COLUMN + ", " +
                NETWORK_UUID_COLUMN + ", " +
                VARIANT_NUM_COLUMN + ", " +
                "operationallimitsgroupid, " + SIDE_COLUMN + ", limittype, " +
                NAME_COLUMN + ", value_, acceptableDuration, fictitious)" +
                " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    public static String buildInsertPermanentLimitsOldFashionQuery() {
        return "insert into permanentlimit(" +
                EQUIPMENT_ID_COLUMN + ", " + EQUIPMENT_TYPE_COLUMN + ", " +
                NETWORK_UUID_COLUMN + ", " +
                VARIANT_NUM_COLUMN + ", " +
                "operationallimitsgroupid, " + SIDE_COLUMN + ", limittype, value_)" +
                " values (?, ?, ?, ?, ?, ?, ?, ?)";
    }

    public int countPermanentLimitsOldFashion(String equipmentId) {
        try (var connection = networkStoreRepository.getDataSource().getConnection()) {
            try (var preparedStmt = connection.prepareStatement("select count(*) from temporarylimit where " + EQUIPMENT_ID_COLUMN + " = ?")) {
                preparedStmt.setString(1, equipmentId);
                try (ResultSet resultSet = preparedStmt.executeQuery()) {
                    resultSet.next();
                    return resultSet.getInt(1);
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public void insertPermanentLimitsOldFashion(Map<OwnerInfo, LimitsInfos> limitsInfos) {
        Map<OwnerInfo, List<PermanentLimitAttributes>> permanentLimits = limitsInfos.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPermanentLimits()));
        try (var connection = networkStoreRepository.getDataSource().getConnection()) {
            try (var preparedStmt = connection.prepareStatement(buildInsertPermanentLimitsOldFashionQuery())) {
                List<Object> values = new ArrayList<>(8);
                List<Map.Entry<OwnerInfo, List<PermanentLimitAttributes>>> list = new ArrayList<>(permanentLimits.entrySet());
                for (List<Map.Entry<OwnerInfo, List<PermanentLimitAttributes>>> subUnit : Lists.partition(list, BATCH_SIZE)) {
                    for (Map.Entry<OwnerInfo, List<PermanentLimitAttributes>> entry : subUnit) {
                        for (PermanentLimitAttributes permanentLimit : entry.getValue()) {
                            values.clear();
                            // In order, from the QueryCatalog.buildInsertTemporaryLimitsQuery SQL query :
                            // equipmentId, equipmentType, networkUuid, variantNum, operationalLimitsGroupId, side, limitType, value
                            values.add(entry.getKey().getEquipmentId());
                            values.add(entry.getKey().getEquipmentType().toString());
                            values.add(entry.getKey().getNetworkUuid());
                            values.add(entry.getKey().getVariantNum());
                            values.add(permanentLimit.getOperationalLimitsGroupId());
                            values.add(permanentLimit.getSide());
                            values.add(permanentLimit.getLimitType().toString());
                            values.add(permanentLimit.getValue());
                            bindValues(preparedStmt, values, objectMapper);
                            preparedStmt.addBatch();
                        }
                    }
                    preparedStmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    public void insertTemporaryLimitsOldFashion(Map<OwnerInfo, LimitsInfos> limitsInfos) {
        Map<OwnerInfo, List<TemporaryLimitAttributes>> temporaryLimits = limitsInfos.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getTemporaryLimits()));
        try (var connection = networkStoreRepository.getDataSource().getConnection()) {
            try (var preparedStmt = connection.prepareStatement(buildInsertTemporaryLimitsOldFashionQuery())) {
                List<Object> values = new ArrayList<>(11);
                List<Map.Entry<OwnerInfo, List<TemporaryLimitAttributes>>> list = new ArrayList<>(temporaryLimits.entrySet());
                for (List<Map.Entry<OwnerInfo, List<TemporaryLimitAttributes>>> subUnit : Lists.partition(list, BATCH_SIZE)) {
                    for (Map.Entry<OwnerInfo, List<TemporaryLimitAttributes>> entry : subUnit) {
                        for (TemporaryLimitAttributes temporaryLimit : entry.getValue()) {
                            values.clear();
                            // In order, from the QueryCatalog.buildInsertTemporaryLimitsQuery SQL query :
                            // equipmentId, equipmentType, networkUuid, variantNum, operationalLimitsGroupId, side, limitType, name, value, acceptableDuration, fictitious
                            values.add(entry.getKey().getEquipmentId());
                            values.add(entry.getKey().getEquipmentType().toString());
                            values.add(entry.getKey().getNetworkUuid());
                            values.add(entry.getKey().getVariantNum());
                            values.add(temporaryLimit.getOperationalLimitsGroupId());
                            values.add(temporaryLimit.getSide());
                            values.add(temporaryLimit.getLimitType().toString());
                            values.add(temporaryLimit.getName());
                            values.add(temporaryLimit.getValue());
                            values.add(temporaryLimit.getAcceptableDuration());
                            values.add(temporaryLimit.isFictitious());
                            bindValues(preparedStmt, values, objectMapper);
                            preparedStmt.addBatch();
                        }
                    }
                    preparedStmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }
}
