/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.commons.PowsyblException;
import com.powsybl.network.store.model.IdentifiableAttributes;
import com.powsybl.network.store.server.exceptions.UncheckedSqlException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public final class Utils {
    private Utils() throws IllegalAccessException {
        throw new IllegalAccessException("Utility class can not be initialize.");
    }

    private static boolean isCustomTypeJsonified(Class<?> clazz) {
        return !(
                Integer.class.equals(clazz) || Long.class.equals(clazz)
                        || Float.class.equals(clazz) || Double.class.equals(clazz)
                        || String.class.equals(clazz) || Boolean.class.equals(clazz)
                        || UUID.class.equals(clazz)
                        || Date.class.isAssignableFrom(clazz) // java.util.Date and java.sql.Date
            );
    }

    static void bindValues(PreparedStatement statement, List<Object> values, ObjectMapper mapper) throws SQLException {
        int idx = 0;
        for (Object o : values) {
            if (o instanceof Instant d) {
                statement.setDate(++idx, new java.sql.Date(d.toEpochMilli()));
            } else if (o == null || !isCustomTypeJsonified(o.getClass())) {
                statement.setObject(++idx, o);
            } else if (o instanceof Array array) {
                statement.setArray(++idx, array);
            } else {
                try {
                    statement.setObject(++idx, mapper.writeValueAsString(o));
                } catch (JsonProcessingException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    static void bindAttributes(ResultSet resultSet, int columnIndex, ColumnMapping columnMapping, IdentifiableAttributes attributes, ObjectMapper mapper) {
        try {
            Object value = null;
            if (columnMapping.getClassR() == null || isCustomTypeJsonified(columnMapping.getClassR())) {
                String str = resultSet.getString(columnIndex);
                if (str != null) {
                    if (columnMapping.getClassMapKey() != null && columnMapping.getClassMapValue() != null) {
                        value = mapper.readValue(str, mapper.getTypeFactory().constructMapType(Map.class, columnMapping.getClassMapKey(), columnMapping.getClassMapValue()));
                    } else {
                        if (columnMapping.getClassR() == null) {
                            throw new PowsyblException("Invalid mapping config");
                        }
                        if (columnMapping.getClassR() == Instant.class) {
                            value = resultSet.getTimestamp(columnIndex).toInstant();
                        } else {
                            value = mapper.readValue(str, columnMapping.getClassR());
                        }
                    }
                }
            } else {
                value = resultSet.getObject(columnIndex, columnMapping.getClassR());
            }
            if (value != null) {
                columnMapping.set(attributes, value);
            }
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
