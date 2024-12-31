package com.powsybl.network.store.server.migration;

//Class to be removed when limits are fully migrated - should be after v2.13 deployment
public final class V211LimitsQueryCatalog {
    public static final String MINIMAL_VALUE_REQUIREMENT_ERROR = "Function should not be called without at least one value.";

    static final String NETWORK_UUID_COLUMN = "networkUuid";
    static final String VARIANT_NUM_COLUMN = "variantNum";
    public static final String EQUIPMENT_TYPE_COLUMN = "equipmentType";
    public static final String EQUIPMENT_ID_COLUMN = "equipmentId";
    static final String V211_TEMPORARY_LIMITS = "temporarylimit";
    static final String V211_PERMANENT_LIMITS = "permanentlimit";

    private V211LimitsQueryCatalog() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static String buildGetV211TemporaryLimitWithInClauseQuery(String columnNameForInClause, int numberOfValues) {
        if (numberOfValues < 1) {
            throw new IllegalArgumentException(MINIMAL_VALUE_REQUIREMENT_ERROR);
        }
        return "select " + EQUIPMENT_ID_COLUMN + ", " +
                EQUIPMENT_TYPE_COLUMN + ", " +
                NETWORK_UUID_COLUMN + ", " +
                VARIANT_NUM_COLUMN + ", " +
                "operationallimitsgroupid, side, limittype, name, value_, acceptableduration, fictitious" +
                " from " + V211_TEMPORARY_LIMITS + " where " +
                NETWORK_UUID_COLUMN + " = ? and " +
                VARIANT_NUM_COLUMN + " = ? and " +
                columnNameForInClause + " in (" +
                "?, ".repeat(numberOfValues - 1) + "?)";
    }

    public static String buildDeleteV211TemporaryLimitsVariantEquipmentINQuery(int numberOfValues) {
        if (numberOfValues < 1) {
            throw new IllegalArgumentException(MINIMAL_VALUE_REQUIREMENT_ERROR);
        }
        return "delete from " + V211_TEMPORARY_LIMITS + " where " +
                NETWORK_UUID_COLUMN + " = ? and " +
                VARIANT_NUM_COLUMN + " = ? and " +
                EQUIPMENT_ID_COLUMN + " in (" +
                "?, ".repeat(numberOfValues - 1) + "?)";
    }

    public static String buildGetV211TemporaryLimitQuery(String columnNameForWhereClause) {
        return "select " + EQUIPMENT_ID_COLUMN + ", " +
                EQUIPMENT_TYPE_COLUMN + ", " +
                NETWORK_UUID_COLUMN + ", " +
                VARIANT_NUM_COLUMN + ", operationallimitsgroupid, side, limittype, name, value_, acceptableduration, fictitious" +
                " from " + V211_TEMPORARY_LIMITS + " where " +
                NETWORK_UUID_COLUMN + " = ? and " +
                VARIANT_NUM_COLUMN + " = ? and " +
                columnNameForWhereClause + " = ?";
    }

    public static String buildDeleteV211PermanentLimitsVariantEquipmentINQuery(int numberOfValues) {
        if (numberOfValues < 1) {
            throw new IllegalArgumentException(MINIMAL_VALUE_REQUIREMENT_ERROR);
        }
        return "delete from " + V211_PERMANENT_LIMITS + " where " +
                NETWORK_UUID_COLUMN + " = ? and " +
                VARIANT_NUM_COLUMN + " = ? and " +
                EQUIPMENT_ID_COLUMN + " in (" +
                "?, ".repeat(numberOfValues - 1) + "?)";
    }

    public static String buildGetV211PermanentLimitWithInClauseQuery(String columnNameForInClause, int numberOfValues) {
        if (numberOfValues < 1) {
            throw new IllegalArgumentException(MINIMAL_VALUE_REQUIREMENT_ERROR);
        }
        return "select " + EQUIPMENT_ID_COLUMN + ", " +
                EQUIPMENT_TYPE_COLUMN + ", " +
                NETWORK_UUID_COLUMN + ", " +
                VARIANT_NUM_COLUMN + ", operationallimitsgroupid, side, limittype, value_ " +
                " from " + V211_PERMANENT_LIMITS + " where " +
                NETWORK_UUID_COLUMN + " = ? and " +
                VARIANT_NUM_COLUMN + " = ? and " +
                columnNameForInClause + " in (" +
                "?, ".repeat(numberOfValues - 1) + "?)";
    }

    public static String buildGetV211PermanentLimitQuery(String columnNameForWhereClause) {
        return "select " + EQUIPMENT_ID_COLUMN + ", " +
                EQUIPMENT_TYPE_COLUMN + ", " +
                NETWORK_UUID_COLUMN + ", " +
                VARIANT_NUM_COLUMN + ", operationallimitsgroupid, side, limittype, value_" +
                " from " + V211_TEMPORARY_LIMITS + " where " +
                NETWORK_UUID_COLUMN + " = ? and " +
                VARIANT_NUM_COLUMN + " = ? and " +
                columnNameForWhereClause + " = ?";
    }

}
