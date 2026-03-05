package it.rotaliano.jdbc.salesforce.metadata;

import static it.rotaliano.jdbc.salesforce.metadata.TypeInfo.BOOL_TYPE_INFO;
import static it.rotaliano.jdbc.salesforce.metadata.TypeInfo.INT_TYPE_INFO;
import static it.rotaliano.jdbc.salesforce.metadata.TypeInfo.LONG_TYPE_INFO;
import static it.rotaliano.jdbc.salesforce.metadata.TypeInfo.SHORT_TYPE_INFO;
import static it.rotaliano.jdbc.salesforce.metadata.TypeInfo.STRING_TYPE_INFO;

import it.rotaliano.jdbc.salesforce.ForceDriver;
import it.rotaliano.jdbc.salesforce.connection.ForceConnection;
import it.rotaliano.jdbc.salesforce.connection.ForceService;
import it.rotaliano.jdbc.salesforce.delegates.PartnerService;
import it.rotaliano.jdbc.salesforce.resultset.CachedResultSet;
import it.rotaliano.jdbc.salesforce.resultset.CachedResultSetMetaData;
import it.rotaliano.jdbc.salesforce.statement.ForcePreparedStatement;
import it.rotaliano.jdbc.salesforce.utils.Constants;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ForceDatabaseMetaData extends AbstractDatabaseMetaData implements Serializable {

    public static final String DEFAULT_SCHEMA = "Salesforce";
    public static final String DEFAULT_CATALOG = "rotaliano";
    public static final String DEFAULT_TABLE_TYPE = "TABLE";

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
        for (Table table : getTables(tableNamePattern)) {
            String name = table.getName();

            if ((typeList == null || typeList.contains(DEFAULT_TABLE_TYPE)) &&
                (tableNamePattern == null || "%".equals(tableNamePattern.trim()) || name.equalsIgnoreCase(
                    tableNamePattern))) {
                ColumnMap<String, Object> map = new ColumnMap<>();
                map.put("TABLE_CAT", DEFAULT_CATALOG, TypeInfo.STRING_TYPE_INFO);
                map.put("TABLE_SCHEM", DEFAULT_SCHEMA, STRING_TYPE_INFO);
                map.put("TABLE_NAME", name, STRING_TYPE_INFO);
                map.put("TABLE_TYPE", DEFAULT_TABLE_TYPE, STRING_TYPE_INFO);
                map.put("REMARKS", table.getComments(), STRING_TYPE_INFO);
                map.put("TYPE_CAT", null, STRING_TYPE_INFO);
                map.put("TYPE_SCHEM", null, STRING_TYPE_INFO);
                map.put("TYPE_NAME", null, STRING_TYPE_INFO);
                map.put("SELF_REFERENCING_COL_NAME", null, STRING_TYPE_INFO);
                map.put("REF_GENERATION", null, STRING_TYPE_INFO);
                rows.add(map);
            }
        }
        log.trace("[Meta] getTables RESULT catalog={} schema={} table={} TablesFound={}",
            catalog, schemaPattern, tableNamePattern, rows.size());
        return new CachedResultSet(rows, ForcePreparedStatement.createMetaData(rows.get(0)));
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
            .map(column -> {
                final ColumnMap<String, Object> cm = new ColumnMap<>();
                TypeInfo typeInfo = TypeInfo.lookupTypeInfo(column.getType());
                cm.put("TABLE_CAT", DEFAULT_CATALOG, STRING_TYPE_INFO);
                cm.put("TABLE_SCHEM", DEFAULT_SCHEMA, STRING_TYPE_INFO);
                cm.put("TABLE_NAME", column.getTable().getName(), STRING_TYPE_INFO);
                cm.put("COLUMN_NAME", column.getName(), STRING_TYPE_INFO);
                cm.put("DATA_TYPE", typeInfo != null ? typeInfo.getSqlDataType() : Types.OTHER, INT_TYPE_INFO);
                cm.put("TYPE_NAME", column.getType(), STRING_TYPE_INFO);
                cm.put("COLUMN_SIZE", column.getLength(), INT_TYPE_INFO);
                cm.put("BUFFER_LENGTH", 0, INT_TYPE_INFO);
                cm.put("DECIMAL_DIGITS", 0, INT_TYPE_INFO);
                cm.put("NUM_PREC_RADIX", typeInfo != null ? typeInfo.getRadix() : 10, INT_TYPE_INFO);
                cm.put("NULLABLE",
                    column.isNillable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls,
                    INT_TYPE_INFO);
                cm.put("REMARKS", column.getComments(), STRING_TYPE_INFO);
                cm.put("COLUMN_DEF", null, STRING_TYPE_INFO);
                cm.put("SQL_DATA_TYPE", null, INT_TYPE_INFO);
                cm.put("SQL_DATETIME_SUB", null, INT_TYPE_INFO);
                cm.put("CHAR_OCTET_LENGTH", 0, INT_TYPE_INFO);
                cm.put("ORDINAL_POSITION", ordinal.getAndIncrement(), INT_TYPE_INFO);
                cm.put("IS_NULLABLE", column.isNillable() ? "YES" : "NO", STRING_TYPE_INFO);
                cm.put("SCOPE_CATLOG", null, STRING_TYPE_INFO);
                cm.put("SCOPE_SCHEMA", null, STRING_TYPE_INFO);
                cm.put("SCOPE_TABLE", null, STRING_TYPE_INFO);
                cm.put("SOURCE_DATA_TYPE",
                    (short) TypeInfo.lookupTypeInfo(column.getType()).getSqlDataType(),
                    SHORT_TYPE_INFO);
                cm.put("IS_AUTOINCREMENT", "", STRING_TYPE_INFO);
                cm.put("IS_GENERATEDCOLUMN", "", STRING_TYPE_INFO);
                return cm;
            }).toList();
        log.debug("[Meta] getColumns RESULT catalog={} schema={} table={} column={} ColumnsFound={}",
            catalog, schemaPattern, tableNamePattern, columnNamePattern, rows.size());
        return new CachedResultSet(rows, ForcePreparedStatement.createMetaData(rows.get(0)));
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
                    }
                }
            }
        }
        log.debug("[Meta] getPrimaryKeys RESULT catalog={} schema={} table={} KeysFound={}",
            catalog,
            schema,
            tableName,
            maps.size());
        return new CachedResultSet(maps, ForcePreparedStatement.createMetaData(maps.get(0)));
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String tableName) throws SQLException {
        List<ColumnMap<String, Object>> maps = new ArrayList<>();
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
                            String.format("FK_%s_%s_REF_%s_%s",
                                column.getReferencedTable(),
                                column.getName(),
                                column.getReferencedTable(),
                                column.getReferencedColumn()),
                            STRING_TYPE_INFO);
                        map.put("PK_NAME",
                            String.format("PK_%s_%s", column.getReferencedTable(), column.getReferencedColumn()),
                            STRING_TYPE_INFO);
                        map.put("DEFERRABILITY", (short) DatabaseMetaData.importedKeyNotDeferrable, SHORT_TYPE_INFO);
                        maps.add(map);
                    }
                }
            }
        }
        return new CachedResultSet(maps, ForcePreparedStatement.createMetaData(maps.get(0)));
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String tableName, boolean unique,
        boolean approximate) throws SQLException {
        List<ColumnMap<String, Object>> maps = new ArrayList<>();
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
                }
            }
        }
        return new CachedResultSet(maps, ForcePreparedStatement.createMetaData(maps.get(0)));
    }

    @Override
    public ResultSet getCatalogs() {
        ColumnMap<String, Object> row = new ColumnMap<>();
        row.put("TABLE_CAT", DEFAULT_CATALOG, STRING_TYPE_INFO);
        return new CachedResultSet(row, ForcePreparedStatement.createMetaData(row));
    }

    @Override
    public ResultSet getTypeInfo() {
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
        }
        return new CachedResultSet(rows, ForcePreparedStatement.createMetaData(rows.get(0)));
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
        return "RotAliano JDBC driver for Salesforce";
    }

    @Override
    public String getDriverVersion() {
        return Constants.DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return Constants.DRIVER_MAJOR_VER;
    }

    @Override
    public int getDriverMinorVersion() {
        return Constants.DRIVER_MAJOR_VER;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        // Retrieves whether this database treats mixed case unquoted SQL identifiers as case insensitive and stores them in mixed case.
        return true;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        // Retrieves whether this database treats mixed case unquoted SQL identifiers as case insensitive and stores them in mixed case.
        return true;
    }

    @Override
    public boolean supportsConvert() {
        return true;
    }

    @Override
    public String getSchemaTerm() {
        return DEFAULT_SCHEMA;
    }

    @Override
    public String getCatalogTerm() {
        return DEFAULT_CATALOG;
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
        log.trace("[Meta] getProcedures requested NOT_IMPLEMENTED catalog={} schema={} proc={}", catalog, schemaPattern,
            procedureNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
        String columnNamePattern) {
        log.trace("[Meta] getProcedureColumns requested NOT_IMPLEMENTED catalog={} schema={} proc={} col={}", catalog,
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
        log.trace("[Meta] getColumnPrivileges requested NOT_IMPLEMENTED catalog={} schema={} table={} col={}", catalog,
            schema, table, columnNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
        log.trace("[Meta] getTablePrivileges requested NOT_IMPLEMENTED catalog={} schema={} table={}", catalog,
            schemaPattern, tableNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) {
        log.trace("[Meta] getBestRowIdentifier requested NOT_IMPLEMENTED catalog={} schema={} table={}", catalog,
            schema, table);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) {
        log.trace("[Meta] getVersionColumns requested NOT_IMPLEMENTED catalog={} schema={} table={}", catalog,
            schema, table);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) {
        log.trace("[Meta] getExportedKeys requested NOT_IMPLEMENTED catalog={} schema={} table={}", catalog,
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
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
        log.trace("[Meta] getUDTs requested NOT_IMPLEMENTED");
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return true;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) {
        log.trace("[Meta] getSuperTypes requested NOT_IMPLEMENTED catalog={} schema={} type={}", catalog,
            schemaPattern, typeNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) {
        log.trace("[Meta] getSuperTables requested NOT_IMPLEMENTED catalog={} schema={} table={}",
            catalog, schemaPattern, tableNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
        String attributeNamePattern) {
        log.trace("[Meta] getAttributes requested NOT_IMPLEMENTED catalog={} schema={} type={} attr={}", catalog,
            schemaPattern, typeNamePattern, attributeNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public int getDatabaseMajorVersion() {
        return Integer.parseInt(ForceService.DEFAULT_API_VERSION);
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
    public ResultSet getSchemas(String catalog, String schemaPattern) {
        return getSchemas();
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
        log.trace("[Meta] getFunctions requested NOT_IMPLEMENTED catalog={} schema={} func={}", catalog,
            schemaPattern, functionNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
        String columnNamePattern) {
        log.trace("[Meta] getFunctionColumns requested NOT_IMPLEMENTED catalog={} schema={} func={} col={}", catalog,
            schemaPattern, functionNamePattern, columnNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
        String columnNamePattern) {
        log.trace("[Meta] getPseudoColumns requested NOT_IMPLEMENTED catalog={} schema={} table={}  column={}",
            catalog, schemaPattern, tableNamePattern, columnNamePattern);
        return new CachedResultSet(CachedResultSetMetaData.EMPTY);
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
                new PartnerService(connection.getPartnerConnection(),null));
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
