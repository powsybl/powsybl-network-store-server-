/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.network.store.server;

import static com.powsybl.network.store.server.QueryCatalog.*;

/**
 * @author Antoine Bouhours <antoine.bouhours at rte-france.com>
 */
public final class QueryExtensionCatalog {
    static final String EXTENSION_TABLE = "extension";

    private QueryExtensionCatalog() {
    }

    public static String buildCloneExtensionsQuery() {
        return "insert into " + EXTENSION_TABLE + "(" + EQUIPMENT_ID_COLUMN + ", " + EQUIPMENT_TYPE_COLUMN +
                ", " + NETWORK_UUID_COLUMN + ", " + VARIANT_NUM_COLUMN + ", name, value_) select " +
                EQUIPMENT_ID_COLUMN + ", " + EQUIPMENT_TYPE_COLUMN +
                ", ?, ?, name, value_ from " + EXTENSION_TABLE + " where " + NETWORK_UUID_COLUMN +
                " = ? and " + VARIANT_NUM_COLUMN + " = ?";
    }

    public static String buildExtensionsQuery(String columnNameForWhereClause) {
        return "select " + EQUIPMENT_ID_COLUMN + ", " +
                EQUIPMENT_TYPE_COLUMN + ", " +
                NETWORK_UUID_COLUMN + ", " +
                VARIANT_NUM_COLUMN + ", " +
                "name, value_ " +
                "from " + EXTENSION_TABLE + " where " +
                NETWORK_UUID_COLUMN + " = ? and " +
                VARIANT_NUM_COLUMN + " = ? and " +
                columnNameForWhereClause + " = ?";
    }

    public static String buildExtensionsWithInClauseQuery(String columnNameForInClause, int numberOfValues) {
        if (numberOfValues < 1) {
            throw new IllegalArgumentException(MINIMAL_VALUE_REQUIREMENT_ERROR);
        }
        return "select " + EQUIPMENT_ID_COLUMN + ", " +
                EQUIPMENT_TYPE_COLUMN + ", " +
                NETWORK_UUID_COLUMN + ", " +
                VARIANT_NUM_COLUMN + ", " +
                "name, value_ " +
                "from " + EXTENSION_TABLE + " where " +
                NETWORK_UUID_COLUMN + " = ? and " +
                VARIANT_NUM_COLUMN + " = ? and " +
                columnNameForInClause + " in (" +
                "?, ".repeat(numberOfValues - 1) + "?)";
    }

    public static String buildInsertExtensionsQuery() {
        return "insert into " + EXTENSION_TABLE + "(" +
                EQUIPMENT_ID_COLUMN + ", " + EQUIPMENT_TYPE_COLUMN + ", " +
                NETWORK_UUID_COLUMN + " ," +
                VARIANT_NUM_COLUMN + ", name, value_)" +
                " values (?, ?, ?, ?, ?, ?)";
    }

    public static String buildDeleteExtensionsVariantEquipmentINQuery(int numberOfValues) {
        if (numberOfValues < 1) {
            throw new IllegalArgumentException(MINIMAL_VALUE_REQUIREMENT_ERROR);
        }
        return "delete from " + EXTENSION_TABLE + " where " +
                NETWORK_UUID_COLUMN + " = ? and " +
                VARIANT_NUM_COLUMN + " = ? and " +
                EQUIPMENT_ID_COLUMN + " in (" +
                "?, ".repeat(numberOfValues - 1) + "?)";
    }

    public static String buildDeleteExtensionsVariantQuery() {
        return "delete from " + EXTENSION_TABLE + " where " +
                NETWORK_UUID_COLUMN + " = ? and " +
                VARIANT_NUM_COLUMN + " = ?";
    }

    public static String buildDeleteExtensionsQuery() {
        return "delete from " + EXTENSION_TABLE + " where " +
                NETWORK_UUID_COLUMN + " = ?";
    }
}
