package com.ascendix.jdbc.salesforce.metadata;

import static com.ascendix.jdbc.salesforce.metadata.TypeInfo.BOOL_TYPE_INFO;
import static com.ascendix.jdbc.salesforce.metadata.TypeInfo.INT_TYPE_INFO;
import static com.ascendix.jdbc.salesforce.metadata.TypeInfo.LONG_TYPE_INFO;
import static com.ascendix.jdbc.salesforce.metadata.TypeInfo.SHORT_TYPE_INFO;
import static com.ascendix.jdbc.salesforce.metadata.TypeInfo.STRING_TYPE_INFO;

import com.ascendix.jdbc.salesforce.ForceDriver;
import com.ascendix.jdbc.salesforce.connection.ForceConnection;
import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.resultset.CachedResultSet;
import com.ascendix.jdbc.salesforce.resultset.CachedResultSetMetaData;
import com.ascendix.jdbc.salesforce.statement.ForcePreparedStatement;
import com.sforce.ws.ConnectorConfig;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ForceDatabaseMetaData implements DatabaseMetaData, Serializable {

    private static final Logger logger = Logger.getLogger(ForceDriver.SF_JDBC_DRIVER_NAME);

    public static final String DEFAULT_SCHEMA = "Salesforce";
    public static final String DEFAULT_CATALOG = "database";
    public static final String DEFAULT_TABLE_TYPE = "TABLE";
    public static final String ENTITY_TABLE_TYPE = "ENTITY";

    private final transient PartnerService partnerService;
    private transient ForceConnection connection;
    private List<Table> tablesCache;
    private int counter;
    private final Properties connInfo = new Properties();

    public ForceDatabaseMetaData(ForceConnection connection) {
        this.connection = connection;
        this.partnerService = new PartnerService(connection.getPartnerConnection());
        ConnectorConfig connectorConfig = this.connection.getPartnerConnection().getConfig();
        connInfo.setProperty("config.auth_endpoint", connectorConfig.getAuthEndpoint());
        connInfo.setProperty("config.rest_endpoint", Objects.toString(connectorConfig.getRestEndpoint(), ""));
        connInfo.setProperty("config.service_endpoint", connectorConfig.getServiceEndpoint());
    }

    private ForceDatabaseMetaData() {
        this.partnerService = null;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) {
        logger.fine("[Meta] getTables catalog=" + catalog + " schema=" + schemaPattern + " table=" + tableNamePattern);
        List<ColumnMap<String, Object>> rows = new ArrayList<>();
        List<String> typeList = types == null ? null : Arrays.asList(types);
        ColumnMap<String, Object> firstRow = null;
        for (Table table : getTables()) {
            String name = table.getName();
            boolean queryable = table.isQueryable();

            if ((typeList == null || (queryable && typeList.contains(DEFAULT_TABLE_TYPE))) &&
                (tableNamePattern == null || "%".equals(tableNamePattern.trim()) || name.equalsIgnoreCase(
                    tableNamePattern))) {
                ColumnMap<String, Object> map = new ColumnMap<>();
                map.put("TABLE_CAT", DEFAULT_CATALOG, TypeInfo.STRING_TYPE_INFO);
                map.put("TABLE_SCHEM", DEFAULT_SCHEMA, STRING_TYPE_INFO);
                map.put("TABLE_NAME", name, STRING_TYPE_INFO);
                map.put("TABLE_TYPE", queryable ? DEFAULT_TABLE_TYPE : ENTITY_TABLE_TYPE, STRING_TYPE_INFO);
                map.put("REMARKS", table.getComments(), STRING_TYPE_INFO);
                map.put("TYPE_CAT", null, STRING_TYPE_INFO);
                map.put("TYPE_SCHEM", null, STRING_TYPE_INFO);
                map.put("TYPE_NAME", null, STRING_TYPE_INFO);
                map.put("SELF_REFERENCING_COL_NAME", null, STRING_TYPE_INFO);
                map.put("REF_GENERATION", null, STRING_TYPE_INFO);
                rows.add(map);
                if (firstRow == null) {
                    firstRow = map;
                }
            }
        }
        logger.info(
            "[Meta] getTables RESULT catalog=" + catalog + " schema=" + schemaPattern + " table=" + tableNamePattern +
                "\n  firstRowFound=" + (firstRow != null ? "yes" : "no") + " TablesFound=" + rows.size());
        return new CachedResultSet(rows, ForcePreparedStatement.createMetaData(firstRow));
    }

    private List<Table> getTables() {
        if (tablesCache == null) {
            logger.info("[Meta] getTables requested - fetching");
            tablesCache = partnerService.getTables();
        } else {
            logger.info("[Meta] getTables requested - from cache");
        }
        return tablesCache;
    }

    public Table findTableInfo(String tableName) {
        return getTables().stream()
            .filter(table -> table.getName().equalsIgnoreCase(tableName))
            .findFirst()
            .orElse(null);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
        String columnNamePattern) {
        AtomicInteger ordinal = new AtomicInteger(1);
        logger.info("[Meta] getColumns catalog=" + catalog + " schema=" + schemaPattern + " table=" + tableNamePattern
            + " column=" + columnNamePattern);
        List<ColumnMap<String, Object>> rows = getTables().stream()
            .filter(table -> tableNamePattern == null || "%".equals(tableNamePattern.trim()) || table.getName()
                .equalsIgnoreCase(tableNamePattern))
            .flatMap(table -> table.getColumns().stream())
            .filter(column -> columnNamePattern == null || "%".equals(columnNamePattern.trim()) || column.getName()
                .equalsIgnoreCase(columnNamePattern))
            .map(column -> new ColumnMap<String, Object>() {{
                TypeInfo typeInfo = TypeInfo.lookupTypeInfo(column.getType());
                put("TABLE_CAT", DEFAULT_CATALOG, STRING_TYPE_INFO);
                put("TABLE_SCHEM", DEFAULT_SCHEMA, STRING_TYPE_INFO);
                put("TABLE_NAME", column.getTable().getName(), STRING_TYPE_INFO);
                put("COLUMN_NAME", column.getName(), STRING_TYPE_INFO);
                put("DATA_TYPE", typeInfo != null ? typeInfo.getSqlDataType() : Types.OTHER, INT_TYPE_INFO);
                put("TYPE_NAME", column.getType(), STRING_TYPE_INFO);
                put("COLUMN_SIZE", column.getLength(), INT_TYPE_INFO);
                put("BUFFER_LENGTH", 0, INT_TYPE_INFO);
                put("DECIMAL_DIGITS", 0, INT_TYPE_INFO);
                put("NUM_PREC_RADIX", typeInfo != null ? typeInfo.getRadix() : 10, INT_TYPE_INFO);
                put("NULLABLE",
                    column.isNillable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls,
                    INT_TYPE_INFO);
                put("REMARKS", column.getComments(), STRING_TYPE_INFO);
                put("COLUMN_DEF", null, STRING_TYPE_INFO);
                put("SQL_DATA_TYPE", null, INT_TYPE_INFO);
                put("SQL_DATETIME_SUB", null, INT_TYPE_INFO);
                put("CHAR_OCTET_LENGTH", 0, INT_TYPE_INFO);
                put("ORDINAL_POSITION", ordinal.getAndIncrement(), INT_TYPE_INFO);
                put("IS_NULLABLE", column.isNillable() ? "YES" : "NO", STRING_TYPE_INFO);
                put("SCOPE_CATLOG", null, STRING_TYPE_INFO);
                put("SCOPE_SCHEMA", null, STRING_TYPE_INFO);
                put("SCOPE_TABLE", null, STRING_TYPE_INFO);
                put("SOURCE_DATA_TYPE",
                    (short) TypeInfo.lookupTypeInfo(column.getType()).getSqlDataType(),
                    SHORT_TYPE_INFO);
                put("IS_AUTOINCREMENT", "", STRING_TYPE_INFO);
                put("IS_GENERATEDCOLUMN", "", STRING_TYPE_INFO);
            }})
            .collect(Collectors.toList());
        ColumnMap<String, Object> firstRow = !rows.isEmpty() ? rows.get(0) : null;
        logger.info(
            "[Meta] getColumns RESULT catalog=" + catalog + " schema=" + schemaPattern + " table=" + tableNamePattern
                + " column=" + columnNamePattern +
                "\n  firstRowFound=" + (firstRow != null ? "yes" : "no") + " ColumnsFound=" + rows.size());
        return new CachedResultSet(rows, ForcePreparedStatement.createMetaData(firstRow));
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        ColumnMap<String, Object> row = new ColumnMap<>();
        row.put("TABLE_SCHEM", DEFAULT_SCHEMA, STRING_TYPE_INFO);
        row.put("TABLE_CATALOG", DEFAULT_CATALOG, STRING_TYPE_INFO);
        row.put("IS_DEFAULT", true, BOOL_TYPE_INFO);
        return new CachedResultSet(row, ForcePreparedStatement.createMetaData(row));
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String tableName) throws SQLException {
        logger.info("[Meta] getPrimaryKeys RESULT catalog=" + catalog + " schema=" + schema + " table=" + tableName);
        List<ColumnMap<String, Object>> maps = new ArrayList<>();
        ColumnMap<String, Object> firstRow = null;
        for (Table table : getTables()) {
            if (tableName == null || "%".equals(tableName.trim()) || table.getName().equalsIgnoreCase(tableName)) {
                for (Column column : table.getColumns()) {
                    if (column.getName().equalsIgnoreCase("Id")) {
                        ColumnMap<String, Object> map = new ColumnMap<>();
                        map.put("TABLE_CAT", DEFAULT_CATALOG, STRING_TYPE_INFO);
                        map.put("TABLE_SCHEM", DEFAULT_SCHEMA, STRING_TYPE_INFO);
                        map.put("TABLE_NAME", table.getName(), STRING_TYPE_INFO);
                        map.put("COLUMN_NAME", column.getName(), STRING_TYPE_INFO);
                        map.put("KEY_SEQ", (short) 1, SHORT_TYPE_INFO);
                        map.put("PK_NAME",
                            DEFAULT_CATALOG + "_" + DEFAULT_SCHEMA + table.getName() + "_PRIMARYKEY",
                            STRING_TYPE_INFO);
                        maps.add(map);
                        if (firstRow == null) {
                            firstRow = map;
                        }
                    }
                }
            }
        }
        logger.info("[Meta] getPrimaryKeys RESULT catalog=" + catalog + " schema=" + schema + " table=" + tableName +
            "\n  firstRowFound=" + (firstRow != null ? "yes" : "no") + " KeysFound=" + maps.size());
        return new CachedResultSet(maps, ForcePreparedStatement.createMetaData(firstRow));
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String tableName) throws SQLException {
        List<ColumnMap<String, Object>> maps = new ArrayList<>();
        ColumnMap<String, Object> firstRow = null;
        for (Table table : getTables()) {
            if (tableName == null || "%".equals(tableName.trim()) || table.getName().equalsIgnoreCase(tableName)) {
                for (Column column : table.getColumns()) {
                    if (column.getReferencedTable() != null && column.getReferencedColumn() != null) {
                        ColumnMap<String, Object> map = new ColumnMap<>();
                        map.put("PKTABLE_CAT", DEFAULT_CATALOG, STRING_TYPE_INFO);
                        map.put("PKTABLE_SCHEM", DEFAULT_SCHEMA, STRING_TYPE_INFO);
                        map.put("PKTABLE_NAME", column.getReferencedTable(), STRING_TYPE_INFO);
                        map.put("PKCOLUMN_NAME", column.getReferencedColumn(), STRING_TYPE_INFO);
                        map.put("FKTABLE_CAT", DEFAULT_CATALOG, STRING_TYPE_INFO);
                        map.put("FKTABLE_SCHEM", DEFAULT_SCHEMA, STRING_TYPE_INFO);
                        map.put("FKTABLE_NAME", tableName, STRING_TYPE_INFO);
                        map.put("FKCOLUMN_NAME", column.getName(), STRING_TYPE_INFO);
                        map.put("KEY_SEQ", Integer.valueOf(counter++).shortValue(), SHORT_TYPE_INFO);
                        map.put("UPDATE_RULE", (short) 0, SHORT_TYPE_INFO);
                        map.put("DELETE_RULE", (short) 0, STRING_TYPE_INFO);
                        map.put("FK_NAME",
                            DEFAULT_CATALOG + "_" + DEFAULT_SCHEMA + "_" + table.getName() + "_FOREIGNKEY",
                            STRING_TYPE_INFO);
                        map.put("PK_NAME",
                            DEFAULT_CATALOG + "_" + DEFAULT_SCHEMA + "_" + table.getName() + "_PRIMARYKEY",
                            STRING_TYPE_INFO);
                        map.put("DEFERRABILITY", (short) 0, SHORT_TYPE_INFO);
                        maps.add(map);
                        if (firstRow == null) {
                            firstRow = map;
                        }
                    }
                }
            }
        }
        return new CachedResultSet(maps, ForcePreparedStatement.createMetaData(firstRow));
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String tableName, boolean unique,
        boolean approximate) {
        List<ColumnMap<String, Object>> maps = new ArrayList<>();
        ColumnMap<String, Object> firstRow = null;
        for (Table table : getTables()) {
            if (tableName == null || "%".equals(tableName.trim()) || table.getName().equalsIgnoreCase(tableName)) {
                for (Column column : table.getColumns()) {
                    if (column.getName().equalsIgnoreCase("Id")) {
                        ColumnMap<String, Object> map = new ColumnMap<>();
                        map.put("TABLE_CAT", DEFAULT_CATALOG, STRING_TYPE_INFO);
                        map.put("TABLE_SCHEM", DEFAULT_SCHEMA, STRING_TYPE_INFO);
                        map.put("TABLE_NAME", table.getName(), STRING_TYPE_INFO);
                        map.put("NON_UNIQUE", true, BOOL_TYPE_INFO);
                        map.put("INDEX_QUALIFIER", null, STRING_TYPE_INFO);
                        map.put("INDEX_NAME",
                            DEFAULT_CATALOG + "_" + DEFAULT_SCHEMA + "_" + table.getName() + "_Index_" + counter++,
                            STRING_TYPE_INFO);
                        map.put("TYPE", DatabaseMetaData.tableIndexOther, SHORT_TYPE_INFO);
                        map.put("ORDINAL_POSITION", counter, SHORT_TYPE_INFO);
                        map.put("COLUMN_NAME", "Id", STRING_TYPE_INFO);
                        map.put("ASC_OR_DESC", "A", STRING_TYPE_INFO);
                        map.put("CARDINALITY", 1, LONG_TYPE_INFO);
                        map.put("PAGES", 1, LONG_TYPE_INFO);
                        map.put("FILTER_CONDITION", null, STRING_TYPE_INFO);

                        maps.add(map);

                        if (firstRow == null) {
                            firstRow = map;
                        }
                    }
                }
            }
        }
        return new CachedResultSet(maps, ForcePreparedStatement.createMetaData(firstRow));
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        ColumnMap<String, Object> row = new ColumnMap<>();
        row.put("TABLE_CAT", DEFAULT_CATALOG, STRING_TYPE_INFO);
        return new CachedResultSet(row, ForcePreparedStatement.createMetaData(row));
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        ColumnMap<String, Object> firstRow = null;
        List<ColumnMap<String, Object>> rows = new ArrayList<>();
        for (TypeInfo typeInfo : TypeInfo.values()) {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("TYPE_NAME", typeInfo.getTypeName(), STRING_TYPE_INFO);
            row.put("DATA_TYPE", typeInfo.getSqlDataType(), INT_TYPE_INFO);
            row.put("PRECISION", typeInfo.getPrecision(), INT_TYPE_INFO);
            row.put("LITERAL_PREFIX", typeInfo.getPrefix(), STRING_TYPE_INFO);
            row.put("LITERAL_SUFFIX", typeInfo.getSuffix(), STRING_TYPE_INFO);
            row.put("CREATE_PARAMS", null, STRING_TYPE_INFO);
            row.put("NULLABLE", (short) 1, SHORT_TYPE_INFO);
            row.put("CASE_SENSITIVE", false, BOOL_TYPE_INFO);
            row.put("SEARCHABLE", (short) typeInfo.getSearchable(), SHORT_TYPE_INFO);
            row.put("UNSIGNED_ATTRIBUTE", typeInfo.isUnsigned(), BOOL_TYPE_INFO);
            row.put("FIXED_PREC_SCALE", false, BOOL_TYPE_INFO);
            row.put("AUTO_INCREMENT", typeInfo.isAutoIncrement(), BOOL_TYPE_INFO);
            row.put("LOCAL_TYPE_NAME", typeInfo.getTypeName(), STRING_TYPE_INFO);
            row.put("MINIMUM_SCALE", typeInfo.getMinScale(), SHORT_TYPE_INFO);
            row.put("MAXIMUM_SCALE", typeInfo.getMaxScale(), SHORT_TYPE_INFO);
            row.put("SQL_DATA_TYPE", typeInfo.getSqlDataType(), INT_TYPE_INFO);
            row.put("SQL_DATETIME_SUB", null, INT_TYPE_INFO);
            row.put("NUM_PREC_RADIX", typeInfo.getRadix(), INT_TYPE_INFO);

            rows.add(row);
            if (firstRow == null) {
                firstRow = row;
            }
        }
        return new CachedResultSet(rows, ForcePreparedStatement.createMetaData(firstRow));
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getURL() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getUserName() throws SQLException {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Salesforce";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return String.valueOf(getDatabaseMajorVersion());
    }

    @Override
    public String getDriverName() throws SQLException {
        return "Ascendix JDBC driver for Salesforce";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return getDriverMajorVersion() + "." + getDriverMinorVersion() + ".0";
    }

    @Override
    public int getDriverMajorVersion() {
        return 1;
    }

    @Override
    public int getDriverMinorVersion() {
        return 5;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        // Retrieves whether this database treats mixed case unquoted SQL identifiers as case insensitive and stores them in mixed case.
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        // Retrieves whether this database treats mixed case unquoted SQL identifiers as case insensitive and stores them in mixed case.
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        // TODO Auto-generated method stub
        return DEFAULT_SCHEMA;
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        // TODO Auto-generated method stub
        return DEFAULT_CATALOG;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        // TODO Auto-generated method stub
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
        throws SQLException {
        logger.finer(
            "[Meta] getProcedures requested NOT_IMPLEMENTED catalog=" + catalog + " schema=" + schemaPattern + " proc="
                + procedureNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
        String columnNamePattern) throws SQLException {
        logger.finer(
            "[Meta] getProcedureColumns requested NOT_IMPLEMENTED catalog=" + catalog + " schema=" + schemaPattern
                + " procs=" + procedureNamePattern + " col=" + columnNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        logger.finest("[Meta] getTableTypes requested IMPLEMENTED");
        ColumnMap<String, Object> row = new ColumnMap<>();
        row.put("TABLE_TYPE", DEFAULT_TABLE_TYPE, STRING_TYPE_INFO);
        return new CachedResultSet(row, ForcePreparedStatement.createMetaData(row));
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
        throws SQLException {
        logger.finer(
            "[Meta] getColumnPrivileges requested NOT_IMPLEMENTED catalog=" + catalog + " schema=" + schema + " table="
                + table + " column=" + columnNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
        throws SQLException {
        logger.finer(
            "[Meta] getTablePrivileges requested NOT_IMPLEMENTED catalog=" + catalog + " schema=" + schemaPattern
                + " table=" + tableNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
        throws SQLException {
        logger.finer(
            "[Meta] getBestRowIdentifier requested NOT_IMPLEMENTED catalog=" + catalog + " schema=" + schema + " table="
                + table);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        logger.finer(
            "[Meta] getVersionColumns requested NOT_IMPLEMENTED catalog=" + catalog + " schema=" + schema + " table="
                + table);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        logger.finer(
            "[Meta] getExportedKeys requested NOT_IMPLEMENTED catalog=" + catalog + " schema=" + schema + " table="
                + table);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
        String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        logger.finer("[Meta] getCrossReference requested NOT_IMPLEMENTED parentCat=" + parentCatalog + " parentSc="
            + parentSchema + " parentTable=" + parentTable + " catalog=" + foreignCatalog + " schema=" + foreignSchema
            + " table=" + foreignTable);

        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
        throws SQLException {
        logger.finer("[Meta] getUDTs requested NOT_IMPLEMENTED");
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        // FIXME: ci servono
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        // FIXME: ci servono
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        logger.finer(
            "[Meta] getSuperTypes requested NOT_IMPLEMENTED catalog=" + catalog + " schema=" + schemaPattern + " type="
                + typeNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        logger.finer("[Meta] getSuperTables requested NOT_IMPLEMENTED catalog=" + catalog + " schema=" + schemaPattern
            + " table=" + tableNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
        String attributeNamePattern) throws SQLException {
        logger.finer(
            "[Meta] getAttributes requested NOT_IMPLEMENTED catalog=" + catalog + " schema=" + schemaPattern + " type="
                + typeNamePattern + " attr=" + attributeNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 52;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return getSchemas();
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        List<ColumnMap<String, Object>> rows = new ArrayList<>();
        this.connInfo.forEach((k, v) -> {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.add("property", k, STRING_TYPE_INFO);
            row.add("value", v, STRING_TYPE_INFO);
            rows.add(row);
        });
        return new CachedResultSet(rows, new CachedResultSetMetaData() {
            @Override
            public int getColumnCount() throws SQLException {
                return 2;
            }

            @Override
            public String getColumnLabel(int column) throws SQLException {
                return this.getColumnName(column);
            }

            public String getColumnName(int column) throws SQLException {
                return switch (column) {
                    case 1 -> "property";
                    case 2 -> "value";
                    default -> null;
                };
            }

            public int getColumnType(int column) throws SQLException {
                return java.sql.Types.VARCHAR;
            }

            public String getColumnTypeName(int column) throws SQLException {
                return "text";
            }
        });
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
        throws SQLException {
        logger.finer(
            "[Meta] getSuperTables requested NOT_IMPLEMENTED catalog=" + catalog + " schema=" + schemaPattern + " func="
                + functionNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
        String columnNamePattern) throws SQLException {
        logger.finer(
            "[Meta] getSuperTables requested NOT_IMPLEMENTED catalog=" + catalog + " schema=" + schemaPattern + " func="
                + functionNamePattern + " column=" + columnNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
        String columnNamePattern) throws SQLException {
        logger.finer("[Meta] getPseudoColumns requested NOT_IMPLEMENTED catalog=" + catalog + " schema=" + schemaPattern
            + " table=" + tableNamePattern + " column=" + columnNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public static void main(String[] args) throws SQLException {
        ForceDatabaseMetaData metadata = new ForceDatabaseMetaData();
        System.out.println(metadata.getDriverName() + " version " + metadata.getDriverVersion() + " for API "
            + metadata.getDatabaseProductVersion());

        if (args.length > 0) {
            System.out.println("Test the tables from the url ");
            ForceDriver driver = new ForceDriver();
            ForceConnection connection = (ForceConnection) driver.connect(args[0], new Properties());

            ForceDatabaseMetaData metaData = new ForceDatabaseMetaData(connection);
            ResultSet schemas = metaData.getSchemas();
            ResultSet catalogs = metaData.getCatalogs();
            String[] types = null;
            ResultSet tables = metaData.getTables("catalog", "", "%", types);
            int count = 0;
            while (tables.next()) {
                System.out.println(" " + tables.getString("TABLE_NAME"));
                count++;
            }
            System.out.println(count + " Tables total");
        }
    }
}
