package com.powsybl.network.store.server;

import static com.powsybl.network.store.server.QueryCatalog.*;

public final class TombstonedQueryUtils {
    private TombstonedQueryUtils() throws IllegalAccessException {
        throw new IllegalAccessException("Utility class can not be initialize.");
    }

    public static String buildInsertQuery(String tableName) {
        return "insert into " + tableName + " (" + NETWORK_UUID_COLUMN + ", " + VARIANT_NUM_COLUMN + ", " + EQUIPMENT_ID_COLUMN + ") " +
                "values (?, ?, ?)";
    }

    public static String buildGetQuery(String tableName) {
        return "select " + EQUIPMENT_ID_COLUMN + " FROM " + tableName + " WHERE " + NETWORK_UUID_COLUMN + " = ? AND " + VARIANT_NUM_COLUMN + " = ?";
    }

    public static String buildDeleteQuery(String tableName) {
        return "delete from " + tableName +
                " where " +
                NETWORK_UUID_COLUMN + " = ?";
    }

    public static String buildDeleteVariantQuery(String tableName) {
        return "delete from " + tableName +
                " where " +
                NETWORK_UUID_COLUMN + " = ?" + " and " +
                VARIANT_NUM_COLUMN + " = ?";
    }

    public static String buildCloneQuery(String tableName) {
        return "insert into " + tableName + " (" +
                NETWORK_UUID_COLUMN + ", " +
                VARIANT_NUM_COLUMN + ", " +
                EQUIPMENT_ID_COLUMN + ") " +
                "select " +
                "?" + "," +
                "?" + "," +
                EQUIPMENT_ID_COLUMN +
                " from " + tableName + " " +
                "where " +
                NETWORK_UUID_COLUMN + " = ?" + " and " +
                VARIANT_NUM_COLUMN + " = ? ";
    }
}
