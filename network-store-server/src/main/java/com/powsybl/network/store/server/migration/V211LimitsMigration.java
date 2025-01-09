/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.network.store.server.ExtensionHandler;
import com.powsybl.network.store.server.Mappings;
import com.powsybl.network.store.server.NetworkStoreRepository;
import liquibase.change.custom.CustomTaskChange;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.ValidationErrors;
import liquibase.resource.ResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
public class V211LimitsMigration implements CustomTaskChange {

    private static final Logger LOGGER = LoggerFactory.getLogger(V211LimitsMigration.class);

    private NetworkStoreRepository repository;

    public void init(Database database) {
        DataSource dataSource = new SingleConnectionDataSource(((JdbcConnection) database.getConnection()).getUnderlyingConnection(), true);
        ObjectMapper mapper = new ObjectMapper();
        this.repository = new NetworkStoreRepository(dataSource, mapper, new Mappings(), new ExtensionHandler(dataSource, mapper));
    }

    @Override
    public void execute(Database database) {
        init(database);
        JdbcConnection connection = (JdbcConnection) database.getConnection();
        try (PreparedStatement stmt = connection.prepareStatement("select distinct uuid from network ")) {
            ResultSet networkUuids = stmt.executeQuery();
            while (networkUuids.next()) {
                UUID networkUuid = UUID.fromString(networkUuids.getString(1));
                repository.migrateV211Limits(networkUuid);
            }
        } catch (SQLException | DatabaseException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public String getConfirmationMessage() {
        return "V2.11 limits were successfully migrated";
    }

    @Override
    public void setUp() {
        LOGGER.info("Set up migration for limits");
    }

    @Override
    public void setFileOpener(ResourceAccessor resourceAccessor) {
        LOGGER.info("Set file opener for limits migration");
    }

    @Override
    public ValidationErrors validate(Database database) {
        return new ValidationErrors();
    }
}
