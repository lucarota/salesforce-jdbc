package com.ascendix.jdbc.salesforce.metadata;

import static com.ascendix.jdbc.salesforce.metadata.TypeInfo.BOOL_TYPE_INFO;
import static com.ascendix.jdbc.salesforce.metadata.TypeInfo.INT_TYPE_INFO;
import static com.ascendix.jdbc.salesforce.metadata.TypeInfo.LONG_TYPE_INFO;
import static com.ascendix.jdbc.salesforce.metadata.TypeInfo.SHORT_TYPE_INFO;
import static com.ascendix.jdbc.salesforce.metadata.TypeInfo.STRING_TYPE_INFO;

import com.ascendix.jdbc.salesforce.ForceDriver;
import com.ascendix.jdbc.salesforce.connection.ForceConnection;
import com.ascendix.jdbc.salesforce.connection.ForceService;
import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.resultset.CachedResultSet;
import com.ascendix.jdbc.salesforce.resultset.CachedResultSetMetaData;
import com.ascendix.jdbc.salesforce.statement.ForcePreparedStatement;
import com.ascendix.jdbc.salesforce.utils.PatternToRegexUtils;
import com.sforce.ws.ConnectionException;
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
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ForceDatabaseMetaData implements DatabaseMetaData, Serializable {

    public static final String DEFAULT_SCHEMA = "Salesforce";
    public static final String DEFAULT_CATALOG = "Ascendix";
    public static final String DEFAULT_TABLE_TYPE = "TABLE";
    public static final String ENTITY_TABLE_TYPE = "ENTITY";

    private final transient PartnerService partnerService;
    private transient ForceConnection connection;
    private final Properties connInfo = new Properties();

    public ForceDatabaseMetaData(ForceConnection connection, PartnerService partnerService) {
        this.connection = connection;
        this.partnerService = partnerService;
        ConnectorConfig connectorConfig = this.connection.getPartnerConnection().getConfig();
        connInfo.setProperty("config.auth_endpoint", connectorConfig.getAuthEndpoint());
        connInfo.setProperty("config.rest_endpoint", Objects.toString(connectorConfig.getRestEndpoint(), ""));
        connInfo.setProperty("config.service_endpoint", connectorConfig.getServiceEndpoint());
    }

    private ForceDatabaseMetaData() {
        this.partnerService = null;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        log.trace("[Meta] getTables catalog={} schema={} table={}", catalog, schemaPattern, tableNamePattern);
        List<ColumnMap<String, Object>> rows = new ArrayList<>();
        List<String> typeList = types == null ? null : Arrays.asList(types);
        ColumnMap<String, Object> firstRow = null;
        for (Table table : getTables(tableNamePattern)) {
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
        log.trace("[Meta] getTables RESULT catalog={} schema={} table={} firstRowFound={} TablesFound={}",
            catalog, schemaPattern, tableNamePattern, (firstRow != null ? "yes" : "no"), rows.size());
        return new CachedResultSet(rows, ForcePreparedStatement.createMetaData(firstRow));
    }

    private List<Table> getTables(String tablePattern) throws SQLException {
        try {
            log.trace("[Meta] getTables requested - fetching");
            if (tablePattern == null || "%".equals(tablePattern)) {
                return partnerService.getTables();
            } else {
                return partnerService.getTables(tablePattern);
            }
        } catch (ConnectionException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        AtomicInteger ordinal = new AtomicInteger(1);
        log.debug("[Meta] getColumns catalog={} schema={} table={} column={}", catalog, schemaPattern,
                tableNamePattern, columnNamePattern);
        List<ColumnMap<String, Object>> rows = getTables(tableNamePattern).stream()
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
                .collect(Collectors.toUnmodifiableList());
        ColumnMap<String, Object> firstRow = !rows.isEmpty() ? rows.get(0) : null;
        log.debug("[Meta] getColumns RESULT catalog={} schema={} table={} column={} firstRowFound={} ColumnsFound={}",
                catalog, schemaPattern, tableNamePattern, columnNamePattern, firstRow != null ? "yes" : "no", rows.size());
        return new CachedResultSet(rows, ForcePreparedStatement.createMetaData(firstRow));
    }

    @Override
    public ResultSet getSchemas() {
        ColumnMap<String, Object> row = new ColumnMap<>();
        row.put("TABLE_SCHEM", DEFAULT_SCHEMA, STRING_TYPE_INFO);
        row.put("TABLE_CATALOG", DEFAULT_CATALOG, STRING_TYPE_INFO);
        row.put("IS_DEFAULT", true, BOOL_TYPE_INFO);
        return new CachedResultSet(row, ForcePreparedStatement.createMetaData(row));
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String tableName) throws SQLException {
        log.debug("[Meta] getPrimaryKeys RESULT catalog={} schema={} table={}", catalog, schema, tableName);
        List<ColumnMap<String, Object>> maps = new ArrayList<>();
        ColumnMap<String, Object> firstRow = null;
        for (Table table : getTables(tableName)) {
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
                                String.format("%s_%s_%s_PRIMARYKEY", DEFAULT_CATALOG, DEFAULT_SCHEMA, table.getName()),
                                STRING_TYPE_INFO);
                        maps.add(map);
                        if (firstRow == null) {
                            firstRow = map;
                        }
                    }
                }
            }
        }
        log.debug("[Meta] getPrimaryKeys RESULT catalog={} schema={} table={} firstRowFound={} KeysFound={}",
            catalog, schema, tableName, (firstRow != null ? "yes" : "no"), maps.size());
        return new CachedResultSet(maps, ForcePreparedStatement.createMetaData(firstRow));
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String tableName) throws SQLException {
        List<ColumnMap<String, Object>> maps = new ArrayList<>();
        ColumnMap<String, Object> firstRow = null;
        for (Table table : getTables(tableName)) {
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
                        map.put("KEY_SEQ", (short) 1, SHORT_TYPE_INFO);
                        map.put("UPDATE_RULE", (short) DatabaseMetaData.importedKeyNoAction, SHORT_TYPE_INFO);
                        map.put("DELETE_RULE", (short) DatabaseMetaData.importedKeyNoAction, STRING_TYPE_INFO);
                        map.put("FK_NAME",
                                String.format("FK_%s_%s_REF_%s_%s", column.getReferencedTable(), column.getName(), column.getReferencedTable(), column.getReferencedColumn()),
                                STRING_TYPE_INFO);
                        map.put("PK_NAME",
                                String.format("PK_%s_%s", column.getReferencedTable(), column.getReferencedColumn()),
                                STRING_TYPE_INFO);
                        map.put("DEFERRABILITY", (short) DatabaseMetaData.importedKeyNotDeferrable, SHORT_TYPE_INFO);
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
                                  boolean approximate) throws SQLException {
        List<ColumnMap<String, Object>> maps = new ArrayList<>();
        ColumnMap<String, Object> firstRow = null;
        for (Table table : getTables(tableName)) {
            if (tableName == null || "%".equals(tableName.trim()) || table.getName().equalsIgnoreCase(tableName)) {
                for (Column column : table.getColumns()) {
                    if (!column.isIndexed()) {
                        continue;
                    }
                    ColumnMap<String, Object> map = new ColumnMap<>();
                    map.put("TABLE_CAT", DEFAULT_CATALOG, STRING_TYPE_INFO);
                    map.put("TABLE_SCHEM", DEFAULT_SCHEMA, STRING_TYPE_INFO);
                    map.put("TABLE_NAME", table.getName(), STRING_TYPE_INFO);
                    map.put("NON_UNIQUE", !column.isUnique(), BOOL_TYPE_INFO);
                    map.put("INDEX_QUALIFIER", "", STRING_TYPE_INFO);
                    map.put("INDEX_NAME",
                            String.format("%S_%S_%S_%S_INDEX", DEFAULT_CATALOG, DEFAULT_SCHEMA, table.getName(),
                                    column.getName().equals("Id") ? "PRIMARYKEY" : column.getName()),
                            STRING_TYPE_INFO);
                    map.put("TYPE", DatabaseMetaData.tableIndexOther, SHORT_TYPE_INFO);
                    map.put("ORDINAL_POSITION", (short) 1, SHORT_TYPE_INFO);
                    map.put("COLUMN_NAME", column.getName(), STRING_TYPE_INFO);
                    map.put("ASC_OR_DESC", "A", STRING_TYPE_INFO);
                    map.put("CARDINALITY", 1, LONG_TYPE_INFO);
                    map.put("PAGES", 0, LONG_TYPE_INFO);
                    map.put("FILTER_CONDITION", null, STRING_TYPE_INFO);

                    maps.add(map);

                    if (firstRow == null) {
                        firstRow = map;
                    }
                }
            }
        }
        return new CachedResultSet(maps, ForcePreparedStatement.createMetaData(firstRow));
    }

    @Override
    public ResultSet getCatalogs() {
        ColumnMap<String, Object> row = new ColumnMap<>();
        row.put("TABLE_CAT", DEFAULT_CATALOG, STRING_TYPE_INFO);
        return new CachedResultSet(row, ForcePreparedStatement.createMetaData(row));
    }

    @Override
    public ResultSet getTypeInfo() {
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
    public <T> T unwrap(Class<T> iface) {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return false;
    }

    @Override
    public String getURL() {
        return null;
    }

    @Override
    public String getUserName() {
        return "";
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    @Override
    public String getDatabaseProductName() {
        return DEFAULT_SCHEMA;
    }

    @Override
    public String getDatabaseProductVersion() {
        return String.valueOf(getDatabaseMajorVersion());
    }

    @Override
    public String getDriverName() {
        return "Ascendix JDBC driver for Salesforce";
    }

    @Override
    public String getDriverVersion() {
        return getDriverMajorVersion() + "." + getDriverMinorVersion() + ".5";
    }

    @Override
    public int getDriverMajorVersion() {
        return 1;
    }

    @Override
    public int getDriverMinorVersion() {
        return 6;
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        // Retrieves whether this database treats mixed case unquoted SQL identifiers as case insensitive and stores them in mixed case.
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        // Retrieves whether this database treats mixed case unquoted SQL identifiers as case insensitive and stores them in mixed case.
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() {
        return "";
    }

    @Override
    public String getSQLKeywords() {
        return "";
    }

    @Override
    public String getNumericFunctions() {
        return "";
    }

    @Override
    public String getStringFunctions() {
        return "";
    }

    @Override
    public String getSystemFunctions() {
        return "";
    }

    @Override
    public String getTimeDateFunctions() {
        return "";
    }

    @Override
    public String getSearchStringEscape() {
        return "";
    }

    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return false;
    }

    @Override
    public boolean supportsConvert() {
        return true;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsGroupBy() {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return false;
    }

    @Override
    public String getSchemaTerm() {
        return DEFAULT_SCHEMA;
    }

    @Override
    public String getProcedureTerm() {
        return "";
    }

    @Override
    public String getCatalogTerm() {
        return DEFAULT_CATALOG;
    }

    @Override
    public boolean isCatalogAtStart() {
        return false;
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    @Override
    public boolean supportsUnion() {
        return false;
    }

    @Override
    public boolean supportsUnionAll() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 0;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return 0;
    }

    @Override
    public int getMaxRowSize() {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return false;
    }

    @Override
    public int getMaxStatementLength() {
        return 0;
    }

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return 0;
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
        log.info("[Meta] getProcedures requested NOT_IMPLEMENTED catalog={} schema={} proc={}", catalog, schemaPattern,
                procedureNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) {
        log.info("[Meta] getProcedureColumns requested NOT_IMPLEMENTED catalog={} schema={} proc={} col={}", catalog,
                schemaPattern, procedureNamePattern, columnNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getTableTypes() {
        log.trace("[Meta] getTableTypes requested IMPLEMENTED");
        ColumnMap<String, Object> row = new ColumnMap<>();
        row.put("TABLE_TYPE", DEFAULT_TABLE_TYPE, STRING_TYPE_INFO);
        return new CachedResultSet(row, ForcePreparedStatement.createMetaData(row));
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) {
        log.info("[Meta] getColumnPrivileges requested NOT_IMPLEMENTED catalog={} schema={} table={} col={}", catalog,
                schema, table, columnNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
        log.info("[Meta] getTablePrivileges requested NOT_IMPLEMENTED catalog={} schema={} table={}", catalog,
                schemaPattern, tableNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) {
        log.info("[Meta] getBestRowIdentifier requested NOT_IMPLEMENTED catalog={} schema={} table={}", catalog,
                schema, table);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) {
        log.info("[Meta] getVersionColumns requested NOT_IMPLEMENTED catalog={} schema={} table={}", catalog,
                schema, table);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) {
        log.info("[Meta] getExportedKeys requested NOT_IMPLEMENTED catalog={} schema={} table={}", catalog,
                schema, table);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) {
        log.trace(
                "[Meta] getCrossReference requested NOT_IMPLEMENTED parentCat={} parentSc={} parentTable={} catalog={} schema={} table={}",
                parentCatalog,
                parentSchema,
                parentTable,
                foreignCatalog,
                foreignSchema,
                foreignTable);

        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
        log.trace("[Meta] getUDTs requested NOT_IMPLEMENTED");
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return true;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) {
        log.info("[Meta] getSuperTypes requested NOT_IMPLEMENTED catalog={} schema={} type={}", catalog,
                schemaPattern, typeNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) {
        log.info("[Meta] getSuperTables requested NOT_IMPLEMENTED catalog={} schema={} table={}",
                catalog, schemaPattern, tableNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) {
        log.info("[Meta] getAttributes requested NOT_IMPLEMENTED catalog={} schema={} type={} attr={}", catalog,
                schemaPattern, typeNamePattern, attributeNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return false;
    }

    @Override
    public int getResultSetHoldability() {
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return Integer.parseInt(ForceService.DEFAULT_API_VERSION);
    }

    @Override
    public int getDatabaseMinorVersion() {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 0;
    }

    @Override
    public int getSQLStateType() {
        return DatabaseMetaData.sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) {
        return getSchemas();
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public ResultSet getClientInfoProperties() {
        List<ColumnMap<String, Object>> rows = new ArrayList<>();
        this.connInfo.forEach((k, v) -> {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.add("property", k, STRING_TYPE_INFO);
            row.add("value", v, STRING_TYPE_INFO);
            rows.add(row);
        });
        return new CachedResultSet(rows, new CachedResultSetMetaData() {
            @Override
            public int getColumnCount() {
                return 2;
            }

            @Override
            public String getColumnLabel(int column) {
                return this.getColumnName(column);
            }

            @Override
            public String getColumnName(int column) {
                return switch (column) {
                    case 1 -> "property";
                    case 2 -> "value";
                    default -> null;
                };
            }

            @Override
            public int getColumnType(int column) {
                return java.sql.Types.VARCHAR;
            }

            @Override
            public String getColumnTypeName(int column) {
                return "text";
            }
        });
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) {
        log.info("[Meta] getFunctions requested NOT_IMPLEMENTED catalog={} schema={} func={}", catalog,
                schemaPattern, functionNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) {
        log.info("[Meta] getFunctionColumns requested NOT_IMPLEMENTED catalog={} schema={} func={} col={}", catalog,
                schemaPattern, functionNamePattern, columnNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) {
        log.info("[Meta] getPseudoColumns requested NOT_IMPLEMENTED catalog={} schema={} table={}  column={}",
                catalog, schemaPattern, tableNamePattern, columnNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    public void cleanupGlobalCache() throws ConnectionException {
        this.partnerService.cleanupGlobalCache();
    }

    public static void main(String[] args) throws SQLException {
        ForceDatabaseMetaData metadata = new ForceDatabaseMetaData();
        System.out.println(metadata.getDriverName() + " version " + metadata.getDriverVersion() + " for API "
                + metadata.getDatabaseProductVersion());

        if (args.length > 0) {
            System.out.println("Test the tables from the url ");
            ForceDriver driver = new ForceDriver();
            ForceConnection connection = (ForceConnection) driver.connect(args[0], new Properties());

            ForceDatabaseMetaData metaData = new ForceDatabaseMetaData(connection,
                    new PartnerService(connection.getPartnerConnection()));
            ResultSet tables = metaData.getTables("catalog", "", "%", null);
            int count = 0;
            while (tables.next()) {
                System.out.println(" " + tables.getString("TABLE_NAME"));
                count++;
            }
            System.out.println(count + " Tables total");
        }
    }
}
