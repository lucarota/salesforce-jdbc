package com.ascendix.jdbc.salesforce.metadata;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.Arrays;
import lombok.Getter;

@Getter
public enum TypeInfo {

    SHORT_TYPE_INFO("tinyint", Types.TINYINT, 3, 0, 0, 10, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    SMALLINT_TYPE_INFO("smallint", Types.SMALLINT, 5, 0, 0, 10, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    INT_TYPE_INFO("int", Types.INTEGER, 10, 0, 0, 10, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    LONG_TYPE_INFO("bigint", Types.BIGINT, 19, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    FLOAT_TYPE_INFO("float", Types.FLOAT, 7, -38, 38, 10, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    DOUBLE_TYPE_INFO("double", Types.DOUBLE, 17, -324, 306, 10, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    NUMERIC_TYPE_INFO("numeric", Types.NUMERIC, 38, 0, 38, 10, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    DECIMAL_TYPE_INFO("decimal", Types.DECIMAL, 38, 0, 38, 10, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    BIT_TYPE_INFO("bit", Types.BOOLEAN, 1, 0, 0, 10, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    DATE_TYPE_INFO("date", Types.DATE, 10, 0, 0, 0, "'", "'",
        DatabaseMetaData.typePredBasic, false, false),
    TIME_TYPE_INFO("time", Types.TIME, 10, 0, 0, 0, "'", "'",
        DatabaseMetaData.typePredBasic, false, false),
    DATETIME_TYPE_INFO("datetime", Types.TIMESTAMP, 10, 0, 0, 0, "'", "'",
        DatabaseMetaData.typeSearchable, false, false),
    STRING_TYPE_INFO("string", Types.VARCHAR, 0x7fffffff, 0, 0, 0, "'", "'",
        DatabaseMetaData.typeSearchable, false, false),
    CHAR_TYPE_INFO("char", Types.CHAR, 2000, 0, 0, 0, "'", "'",
        DatabaseMetaData.typeSearchable, false, false),
    BINARY_TYPE_INFO("binary", Types.BINARY, 2000, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    BOOL_TYPE_INFO("boolean", Types.BOOLEAN, 1, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    UUID_TYPE_INFO("uuid", 66647, 0, 0, 0, 0, null, null,
        DatabaseMetaData.typeSearchable, false, false),
    OTHER_TYPE_INFO("other", Types.OTHER, 0x7fffffff, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),

    ID_TYPE_INFO("id", Types.VARCHAR, 0x7fffffff, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    MASTERRECORD_TYPE_INFO("masterrecord", Types.VARCHAR, 0x7fffffff, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    REFERENCE_TYPE_INFO("reference", Types.VARCHAR, 0x7fffffff, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    ADDRESS_TYPE_INFO("address", Types.VARCHAR, 0x7fffffff, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    ENCRYPTEDSTRING_TYPE_INFO("encryptedstring", Types.VARCHAR, 0x7fffffff, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    EMAIL_TYPE_INFO("email", Types.VARCHAR, 0x7fffffff, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    PHONE_TYPE_INFO("phone", Types.VARCHAR, 0x7fffffff, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    URL_TYPE_INFO("url", Types.VARCHAR, 0x7fffffff, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    TEXTAREA_TYPE_INFO("textarea", Types.LONGVARCHAR, 0x7fffffff, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    BASE64_TYPE_INFO("base64", Types.BLOB, 0x7fffffff, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    BYTE_TYPE_INFO("byte", Types.VARBINARY, 10, 0, 0, 10, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    PERCENT_TYPE_INFO("percent", Types.DOUBLE, 17, -324, 306, 10, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    CURRENCY_TYPE_INFO("currency", Types.DOUBLE, 17, -324, 306, 10, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    PICKLIST_TYPE_INFO("picklist", Types.ARRAY, 0, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    MULTIPICKLIST_TYPE_INFO("multipicklist", Types.ARRAY, 0, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    COMBOBOX_TYPE_INFO("combobox", Types.ARRAY, 0, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false),
    ANYTYPE_TYPE_INFO("anytype", Types.OTHER, 0, 0, 0, 0, null, null,
        DatabaseMetaData.typePredBasic, false, false);

    private final String typeName;
    private final int sqlDataType;
    private final int precision;
    private final int minScale;
    private final int maxScale;
    private final int radix;
    private final String prefix;
    private final String suffix;
    private final int searchable;
    private final boolean unsigned;
    private final boolean autoIncrement;

    TypeInfo(String typeName, int sqlDataType, int precision, int minScale, int maxScale, int radix,
        String prefix, String suffix, int searchable, boolean unsigned, boolean autoIncrement) {
        this.typeName = typeName;
        this.sqlDataType = sqlDataType;
        this.precision = precision;
        this.minScale = minScale;
        this.maxScale = maxScale;
        this.radix = radix;
        this.prefix = prefix;
        this.suffix = suffix;
        this.searchable = searchable;
        this.unsigned = unsigned;
        this.autoIncrement = autoIncrement;
    }

    public static TypeInfo lookupTypeInfo(String forceTypeName) {
        String typeName = forceTypeName.replaceFirst("\\A_+", "");
        return Arrays.stream(TypeInfo.values())
            .filter(entry -> typeName.equals(entry.getTypeName()))
            .findAny()
            .orElse(OTHER_TYPE_INFO);
    }
}
