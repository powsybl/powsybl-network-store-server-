package com.powsybl.network.store.server.migration;

import com.powsybl.network.store.server.NetworkStoreRepository;
import liquibase.change.custom.CustomSqlChange;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.ValidationErrors;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
public class V211LimitsMigration implements CustomSqlChange {

    private static final Logger LOGGER = LoggerFactory.getLogger(V211LimitsMigration.class);

    //TODO : how to inject Spring dependency here ?
    private NetworkStoreRepository repository;

    @Override
    public SqlStatement[] generateStatements(Database database) {
        JdbcConnection connection = (JdbcConnection) database.getConnection();
        List<SqlStatement> statements = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement("select uuid from network")) {
            ResultSet networkUuids = stmt.executeQuery();
            while (networkUuids.next()) {
                UUID networkUuid = UUID.fromString(networkUuids.getString(1));
                repository.migrateV211Limits(networkUuid);
            }
        } catch (SQLException | DatabaseException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return statements.toArray(new SqlStatement[0]);
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
