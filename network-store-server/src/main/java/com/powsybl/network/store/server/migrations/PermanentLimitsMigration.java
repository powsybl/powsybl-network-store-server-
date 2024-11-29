/*
  Copyright (c) 2024, RTE (http://www.rte-france.com)
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server.migrations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.powsybl.iidm.network.LimitType;
import com.powsybl.network.store.model.ResourceType;
import com.powsybl.network.store.server.PermanentLimitSqlData;
import com.powsybl.network.store.server.dto.OwnerInfo;
import com.powsybl.network.store.server.dto.PermanentLimitAttributes;
import liquibase.change.custom.CustomSqlChange;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.CustomChangeException;
import liquibase.exception.DatabaseException;
import liquibase.exception.SetupException;
import liquibase.exception.ValidationErrors;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.InsertStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Etienne Lesot <etienne.lesot at rte-france.com>
 */
public class PermanentLimitsMigration implements CustomSqlChange {
    private static final Logger LOGGER = LoggerFactory.getLogger(PermanentLimitsMigration.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public SqlStatement[] generateStatements(Database database) throws CustomChangeException {
        JdbcConnection connection = (JdbcConnection) database.getConnection();
        List<SqlStatement> statements = new ArrayList<>();
        String requestStatement = "select equipmentid, equipmenttype, networkUuid, variantNum, operationalLimitsGroupId, side," +
            "limitType, value_ from permanentlimit";
        try (PreparedStatement stmt = connection.prepareStatement(requestStatement)) {
            Map<OwnerInfo, List<PermanentLimitAttributes>> oldPermanentLimits = getPermanentLimits(stmt);
            prepareStatements(oldPermanentLimits, database, statements);
        } catch (SQLException e) {
            throw new CustomChangeException(e);
        } catch (JsonProcessingException | DatabaseException e) {
            throw new RuntimeException(e);
        }
        return statements.toArray(new SqlStatement[0]);
    }

    private Map<OwnerInfo, List<PermanentLimitAttributes>> getPermanentLimits(PreparedStatement stmt) throws SQLException {
        ResultSet resultSet = stmt.executeQuery();
        Map<OwnerInfo, List<PermanentLimitAttributes>> map = new HashMap<>();
        while (resultSet.next()) {
            PermanentLimitAttributes permanentLimit = new PermanentLimitAttributes();
            OwnerInfo owner = new OwnerInfo();
            owner.setEquipmentId(resultSet.getString(1));
            owner.setEquipmentType(ResourceType.valueOf(resultSet.getString(2)));
            owner.setNetworkUuid(UUID.fromString(resultSet.getString(3)));
            owner.setVariantNum(resultSet.getInt(4));
            permanentLimit.setOperationalLimitsGroupId(resultSet.getString(5));
            permanentLimit.setSide(resultSet.getInt(6));
            permanentLimit.setLimitType(LimitType.valueOf(resultSet.getString(7)));
            permanentLimit.setValue(resultSet.getDouble(9));
            map.computeIfAbsent(owner, k -> new ArrayList<>());
            map.get(owner).add(permanentLimit);
        }
        return map;
    }

    private void prepareStatements(Map<OwnerInfo, List<PermanentLimitAttributes>> oldPermanentLimits, Database database, List<SqlStatement> statements) throws SQLException, JsonProcessingException {
        List<Map.Entry<OwnerInfo, List<PermanentLimitAttributes>>> list = new ArrayList<>(oldPermanentLimits.entrySet());
        for (List<Map.Entry<OwnerInfo, List<PermanentLimitAttributes>>> subUnit : Lists.partition(list, 1000)) {
            for (Map.Entry<OwnerInfo, List<PermanentLimitAttributes>> entry : subUnit) {
                if (!entry.getValue().isEmpty()) {
                    List<PermanentLimitSqlData> permanentLimitSqlData = entry.getValue().stream().map(PermanentLimitSqlData::of).toList();
                    String serializedpermanentLimitSqlData = mapper.writeValueAsString(permanentLimitSqlData);
                    statements.add(new InsertStatement(database.getDefaultCatalogName(), database.getDefaultSchemaName(), "newpermanentlimits")
                        .addColumnValue("equipmentId", entry.getKey().getEquipmentId())
                        .addColumnValue("equipmentType", entry.getKey().getEquipmentType())
                        .addColumnValue("networkuuid", entry.getKey().getNetworkUuid())
                        .addColumnValue("variantnum", entry.getKey().getVariantNum())
                        .addColumnValue("permanentlimits", serializedpermanentLimitSqlData)
                    );
                }

            }
        }
    }

    @Override
    public String getConfirmationMessage() {
        return "permanent limits was successfully updated";
    }

    @Override
    public void setUp() throws SetupException {
        LOGGER.info("Set up migration for permanent limits");
    }

    @Override
    public void setFileOpener(ResourceAccessor resourceAccessor) {
        LOGGER.info("Set file opener for permanent limits");
    }

    @Override
    public ValidationErrors validate(Database database) {
        return new ValidationErrors();
    }
}
