/*
  Copyright (c) 2024, RTE (http://www.rte-france.com)
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server.migrations;

import com.google.common.collect.Lists;
import com.powsybl.iidm.network.LimitType;
import com.powsybl.network.store.model.ResourceType;
import com.powsybl.network.store.model.TemporaryLimitAttributes;
import com.powsybl.network.store.server.dto.OwnerInfo;
import liquibase.change.custom.CustomSqlChange;
import liquibase.database.Database;
import liquibase.exception.CustomChangeException;
import liquibase.exception.SetupException;
import liquibase.exception.ValidationErrors;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.InsertStatement;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Etienne Lesot <etienne.lesot at rte-france.com>
 */
public class TemporaryLimitsMigration implements CustomSqlChange {
    private static final Logger LOGGER = LoggerFactory.getLogger(TemporaryLimitsMigration.class);

    @Override
    public SqlStatement[] generateStatements(Database database) throws CustomChangeException {
        PgConnection connection = (PgConnection) database.getConnection();
        List<SqlStatement> statements = new ArrayList<>();
        String requestStatement = "select equipmentid, equipmenttype, networkUuid, variantNum, operationalLimitsGroupId, side," +
            "limitType, name, value_, acceptableDuration, fictitious from temporarylimit";
        try (PreparedStatement stmt = connection.prepareStatement(requestStatement)) {
            Map<OwnerInfo, List<TemporaryLimitAttributes>> oldTemporaryLimits = getTemporaryLimits(stmt);
            prepareStatements(oldTemporaryLimits, connection, database, statements);
        } catch (SQLException e) {
            throw new CustomChangeException(e);
        }
        return statements.toArray(new SqlStatement[0]);
    }

    private Map<OwnerInfo, List<TemporaryLimitAttributes>> getTemporaryLimits(PreparedStatement stmt) throws SQLException {
        ResultSet resultSet = stmt.executeQuery();
        Map<OwnerInfo, List<TemporaryLimitAttributes>> map = new HashMap<>();
        while (resultSet.next()) {
            TemporaryLimitAttributes temporaryLimit = new TemporaryLimitAttributes();
            OwnerInfo owner = new OwnerInfo();
            owner.setEquipmentId(resultSet.getString(1));
            owner.setEquipmentType(ResourceType.valueOf(resultSet.getString(2)));
            owner.setNetworkUuid(UUID.fromString(resultSet.getString(3)));
            owner.setVariantNum(resultSet.getInt(4));
            temporaryLimit.setOperationalLimitsGroupId(resultSet.getString(5));
            temporaryLimit.setSide(resultSet.getInt(6));
            temporaryLimit.setLimitType(LimitType.valueOf(resultSet.getString(7)));
            temporaryLimit.setName(resultSet.getString(8));
            temporaryLimit.setValue(resultSet.getDouble(9));
            temporaryLimit.setAcceptableDuration(resultSet.getInt(10));
            temporaryLimit.setFictitious(resultSet.getBoolean(11));
            map.computeIfAbsent(owner, k -> new ArrayList<>());
            map.get(owner).add(temporaryLimit);
        }
        return map;
    }

    private void prepareStatements(Map<OwnerInfo, List<TemporaryLimitAttributes>> oldTemporaryLimits, PgConnection connection, Database database, List<SqlStatement> statements) throws SQLException {
        List<Map.Entry<OwnerInfo, List<TemporaryLimitAttributes>>> list = new ArrayList<>(oldTemporaryLimits.entrySet());
        for (List<Map.Entry<OwnerInfo, List<TemporaryLimitAttributes>>> subUnit : Lists.partition(list, 1000)) {
            for (Map.Entry<OwnerInfo, List<TemporaryLimitAttributes>> entry : subUnit) {
                if (!entry.getValue().isEmpty()) {
                    Array array = connection.createArrayOf("TemporaryLimitClass", entry.getValue().stream().map(temporaryLimitAttributes ->
                            "(" + temporaryLimitAttributes.getOperationalLimitsGroupId() + ","
                                + temporaryLimitAttributes.getSide() + ","
                                + temporaryLimitAttributes.getLimitType() + ","
                                + temporaryLimitAttributes.getName() + ","
                                + temporaryLimitAttributes.getValue() + ","
                                + temporaryLimitAttributes.getAcceptableDuration() + ","
                                + temporaryLimitAttributes.isFictitious()
                                + ")")
                        .toArray());
                    statements.add(new InsertStatement(database.getDefaultCatalogName(), database.getDefaultSchemaName(), "newtemporarylimits")
                        .addColumnValue("equipmentId", entry.getKey().getEquipmentId())
                        .addColumnValue("equipmentType", entry.getKey().getEquipmentType())
                        .addColumnValue("networkuuid", entry.getKey().getNetworkUuid())
                        .addColumnValue("variantnum", entry.getKey().getVariantNum())
                        .addColumnValue("temporarylimits", array)
                    );
                }

            }
        }
    }

    @Override
    public String getConfirmationMessage() {
        return "temporary limits was successfully updated";
    }

    @Override
    public void setUp() throws SetupException {
        LOGGER.info("Set up migration for temporary limits");
    }

    @Override
    public void setFileOpener(ResourceAccessor resourceAccessor) {
        LOGGER.info("Set file opener for temporary limits");
    }

    @Override
    public ValidationErrors validate(Database database) {
        return new ValidationErrors();
    }
}
