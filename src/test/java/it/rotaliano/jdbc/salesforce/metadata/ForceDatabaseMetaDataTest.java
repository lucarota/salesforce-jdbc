package it.rotaliano.jdbc.salesforce.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectorConfig;
import it.rotaliano.jdbc.salesforce.connection.ForceConnection;
import it.rotaliano.jdbc.salesforce.connection.ForceService;
import it.rotaliano.jdbc.salesforce.delegates.PartnerService;
import it.rotaliano.jdbc.salesforce.utils.Constants;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class ForceDatabaseMetaDataTest {

    private ForceConnection connection;
    private PartnerService partnerService;
    private ForceDatabaseMetaData metaData;

    @BeforeEach
    void setUp() {
        connection = mock(ForceConnection.class);
        partnerService = mock(PartnerService.class);

        PartnerConnection partnerConnection = mock(PartnerConnection.class);
        ConnectorConfig config = new ConnectorConfig();
        config.setAuthEndpoint("https://login.salesforce.com/services/Soap/u/62.0");
        config.setServiceEndpoint("https://login.salesforce.com/services/Soap/u/62.0");
        config.setRestEndpoint("https://login.salesforce.com/services/data/v62.0");
        when(partnerConnection.getConfig()).thenReturn(config);
        when(connection.getPartnerConnection()).thenReturn(partnerConnection);

        metaData = new ForceDatabaseMetaData(connection, partnerService);
    }

    private Table createTestTable(String name) {
        Column idCol = new Column("Id", "id");
        idCol.setLength(18);
        idCol.setNillable(false);
        idCol.setUnique(true);
        idCol.setIndexed(true);

        Column nameCol = new Column("Name", "string");
        nameCol.setLength(255);
        nameCol.setNillable(true);
        nameCol.setComments("The name");

        Column refCol = new Column("AccountId", "reference");
        refCol.setLength(18);
        refCol.setReferencedTable("Account");
        refCol.setReferencedColumn("Id");
        refCol.setIndexed(true);

        return new Table(name, "Test table", List.of(idCol, nameCol, refCol));
    }

    @Nested
    class ConstantValues {

        @Test
        void getDatabaseProductName_returnsSalesforce() {
            assertEquals("Salesforce", metaData.getDatabaseProductName());
        }

        @Test
        void getDatabaseProductVersion_returnsApiVersion() {
            assertEquals(String.valueOf(Integer.parseInt(ForceService.DEFAULT_API_VERSION)),
                metaData.getDatabaseProductVersion());
        }

        @Test
        void getDriverName_returnsExpectedName() {
            assertEquals("RotAliano JDBC driver for Salesforce", metaData.getDriverName());
        }

        @Test
        void getDriverVersion_returnsConstant() {
            assertEquals(Constants.DRIVER_VERSION, metaData.getDriverVersion());
        }

        @Test
        void getDriverMajorVersion_returnsConstant() {
            assertEquals(Constants.DRIVER_MAJOR_VER, metaData.getDriverMajorVersion());
        }

        @Test
        void getDatabaseMajorVersion_returnsApiVersion() {
            assertEquals(Integer.parseInt(ForceService.DEFAULT_API_VERSION), metaData.getDatabaseMajorVersion());
        }

        @Test
        void getJDBCMajorVersion_returns4() {
            assertEquals(4, metaData.getJDBCMajorVersion());
        }

        @Test
        void getJDBCMinorVersion_returns0() {
            assertEquals(0, metaData.getJDBCMinorVersion());
        }

        @Test
        void getSQLStateType_returnsSqlStateSQL() {
            assertEquals(DatabaseMetaData.sqlStateSQL, metaData.getSQLStateType());
        }

        @Test
        void getSchemaTerm_returnsDefaultSchema() {
            assertEquals(ForceDatabaseMetaData.DEFAULT_SCHEMA, metaData.getSchemaTerm());
        }

        @Test
        void getCatalogTerm_returnsDefaultCatalog() {
            assertEquals(ForceDatabaseMetaData.DEFAULT_CATALOG, metaData.getCatalogTerm());
        }

        @Test
        void getCatalogSeparator_returnsDot() {
            assertEquals(".", metaData.getCatalogSeparator());
        }
    }

    @Nested
    class BooleanOverrides {

        @Test
        void supportsMixedCaseIdentifiers_returnsTrue() {
            assertTrue(metaData.supportsMixedCaseIdentifiers());
        }

        @Test
        void storesMixedCaseIdentifiers_returnsTrue() {
            assertTrue(metaData.storesMixedCaseIdentifiers());
        }

        @Test
        void supportsConvert_returnsTrue() {
            assertTrue(metaData.supportsConvert());
        }

        @Test
        void supportsGetGeneratedKeys_returnsTrue() {
            assertTrue(metaData.supportsGetGeneratedKeys());
        }

        @Test
        void supportsResultSetType_forwardOnly_returnsTrue() {
            assertTrue(metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
        }

        @Test
        void supportsResultSetType_scrollInsensitive_returnsFalse() {
            assertFalse(metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
        }

        @Test
        void supportsResultSetConcurrency_forwardReadOnly_returnsTrue() {
            assertTrue(metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        }

        @Test
        void supportsResultSetConcurrency_forwardUpdatable_returnsFalse() {
            assertFalse(metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
        }

        @Test
        void supportsResultSetConcurrency_scrollReadOnly_returnsFalse() {
            assertFalse(metaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
        }
    }

    @Nested
    class ConnectionAndInfo {

        @Test
        void getConnection_returnsForceConnection() {
            assertSame(connection, metaData.getConnection());
        }

        @Test
        void getClientInfoProperties_returnsResultSet() {
            ResultSet rs = metaData.getClientInfoProperties();
            assertNotNull(rs);
        }

        @Test
        void getClientInfoProperties_containsEndpoints() throws SQLException {
            ResultSet rs = metaData.getClientInfoProperties();
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertEquals(3, count);
        }
    }

    @Nested
    class SchemasAndCatalogs {

        @Test
        void getSchemas_returnsDefaultSchema() throws SQLException {
            ResultSet rs = metaData.getSchemas();
            assertTrue(rs.next());
            assertEquals(ForceDatabaseMetaData.DEFAULT_SCHEMA, rs.getString("TABLE_SCHEM"));
            assertEquals(ForceDatabaseMetaData.DEFAULT_CATALOG, rs.getString("TABLE_CATALOG"));
            assertFalse(rs.next());
        }

        @Test
        void getSchemasWithParams_delegatesToGetSchemas() throws SQLException {
            ResultSet rs = metaData.getSchemas("anyCatalog", "anySchema");
            assertTrue(rs.next());
            assertEquals(ForceDatabaseMetaData.DEFAULT_SCHEMA, rs.getString("TABLE_SCHEM"));
        }

        @Test
        void getCatalogs_returnsDefaultCatalog() throws SQLException {
            ResultSet rs = metaData.getCatalogs();
            assertTrue(rs.next());
            assertEquals(ForceDatabaseMetaData.DEFAULT_CATALOG, rs.getString("TABLE_CAT"));
            assertFalse(rs.next());
        }

        @Test
        void getTableTypes_returnsTable() throws SQLException {
            ResultSet rs = metaData.getTableTypes();
            assertTrue(rs.next());
            assertEquals(ForceDatabaseMetaData.DEFAULT_TABLE_TYPE, rs.getString("TABLE_TYPE"));
            assertFalse(rs.next());
        }
    }

    @Nested
    class GetTablesTests {

        @Test
        void getTables_withSpecificName_returnsMatchingTable() throws Exception {
            Table table = createTestTable("Account");
            when(partnerService.getTables("Account")).thenReturn(List.of(table));

            ResultSet rs = metaData.getTables(null, null, "Account", null);

            assertTrue(rs.next());
            assertEquals("Account", rs.getString("TABLE_NAME"));
            assertEquals(ForceDatabaseMetaData.DEFAULT_CATALOG, rs.getString("TABLE_CAT"));
            assertEquals(ForceDatabaseMetaData.DEFAULT_SCHEMA, rs.getString("TABLE_SCHEM"));
            assertEquals(ForceDatabaseMetaData.DEFAULT_TABLE_TYPE, rs.getString("TABLE_TYPE"));
            assertEquals("Test table", rs.getString("REMARKS"));
            assertFalse(rs.next());
        }

        @Test
        void getTables_withWildcard_returnsAllTables() throws Exception {
            Table t1 = createTestTable("Account");
            Table t2 = createTestTable("Contact");
            when(partnerService.getTables()).thenReturn(List.of(t1, t2));

            ResultSet rs = metaData.getTables(null, null, "%", null);

            assertTrue(rs.next());
            assertEquals("Account", rs.getString("TABLE_NAME"));
            assertTrue(rs.next());
            assertEquals("Contact", rs.getString("TABLE_NAME"));
            assertFalse(rs.next());
        }

        @Test
        void getTables_withTypeFilter_includesMatchingType() throws Exception {
            Table table = createTestTable("Account");
            when(partnerService.getTables("Account")).thenReturn(List.of(table));

            ResultSet rs = metaData.getTables(null, null, "Account", new String[]{"TABLE"});

            assertTrue(rs.next());
            assertEquals("Account", rs.getString("TABLE_NAME"));
            assertFalse(rs.next());
        }
    }

    @Nested
    class GetColumnsTests {

        @Test
        void getColumns_returnsColumnMetadata() throws Exception {
            Table table = createTestTable("Account");
            when(partnerService.getTables("Account")).thenReturn(List.of(table));

            ResultSet rs = metaData.getColumns(null, null, "Account", "%");

            assertTrue(rs.next());
            assertEquals("Id", rs.getString("COLUMN_NAME"));
            assertEquals("Account", rs.getString("TABLE_NAME"));
            assertEquals("id", rs.getString("TYPE_NAME"));
            assertEquals(18, rs.getInt("COLUMN_SIZE"));
            assertEquals(DatabaseMetaData.columnNoNulls, rs.getInt("NULLABLE"));
            assertEquals("NO", rs.getString("IS_NULLABLE"));

            assertTrue(rs.next());
            assertEquals("Name", rs.getString("COLUMN_NAME"));
            assertEquals(DatabaseMetaData.columnNullable, rs.getInt("NULLABLE"));
            assertEquals("YES", rs.getString("IS_NULLABLE"));
            assertEquals("The name", rs.getString("REMARKS"));

            assertTrue(rs.next());
            assertEquals("AccountId", rs.getString("COLUMN_NAME"));

            assertFalse(rs.next());
        }

        @Test
        void getColumns_withSpecificColumnName_filtersCorrectly() throws Exception {
            Table table = createTestTable("Account");
            when(partnerService.getTables("Account")).thenReturn(List.of(table));

            ResultSet rs = metaData.getColumns(null, null, "Account", "Name");

            assertTrue(rs.next());
            assertEquals("Name", rs.getString("COLUMN_NAME"));
            assertFalse(rs.next());
        }
    }

    @Nested
    class GetPrimaryKeysTests {

        @Test
        void getPrimaryKeys_returnsIdColumn() throws Exception {
            Table table = createTestTable("Account");
            when(partnerService.getTables("Account")).thenReturn(List.of(table));

            ResultSet rs = metaData.getPrimaryKeys(null, null, "Account");

            assertTrue(rs.next());
            assertEquals("Id", rs.getString("COLUMN_NAME"));
            assertEquals("Account", rs.getString("TABLE_NAME"));
            assertEquals(ForceDatabaseMetaData.DEFAULT_CATALOG, rs.getString("TABLE_CAT"));
            assertTrue(rs.getString("PK_NAME").contains("PRIMARYKEY"));
            assertFalse(rs.next());
        }
    }

    @Nested
    class GetImportedKeysTests {

        @Test
        void getImportedKeys_returnsForeignKeys() throws Exception {
            Table table = createTestTable("Contact");
            when(partnerService.getTables("Contact")).thenReturn(List.of(table));

            ResultSet rs = metaData.getImportedKeys(null, null, "Contact");

            assertTrue(rs.next());
            assertEquals("Account", rs.getString("PKTABLE_NAME"));
            assertEquals("Id", rs.getString("PKCOLUMN_NAME"));
            assertEquals("Contact", rs.getString("FKTABLE_NAME"));
            assertEquals("AccountId", rs.getString("FKCOLUMN_NAME"));
            assertFalse(rs.next());
        }
    }

    @Nested
    class GetIndexInfoTests {

        @Test
        void getIndexInfo_returnsIndexedColumns() throws Exception {
            Table table = createTestTable("Account");
            when(partnerService.getTables("Account")).thenReturn(List.of(table));

            ResultSet rs = metaData.getIndexInfo(null, null, "Account", false, false);

            // Id (indexed+unique) and AccountId (indexed)
            assertTrue(rs.next());
            assertEquals("Id", rs.getString("COLUMN_NAME"));
            assertFalse(rs.getBoolean("NON_UNIQUE"));

            assertTrue(rs.next());
            assertEquals("AccountId", rs.getString("COLUMN_NAME"));
            assertTrue(rs.getBoolean("NON_UNIQUE"));

            assertFalse(rs.next());
        }
    }

    @Nested
    class GetTypeInfoTests {

        @Test
        void getTypeInfo_returnsAllTypes() throws SQLException {
            ResultSet rs = metaData.getTypeInfo();
            int count = 0;
            while (rs.next()) {
                assertNotNull(rs.getString("TYPE_NAME"));
                count++;
            }
            assertEquals(TypeInfo.values().length, count);
        }
    }

    @Nested
    class NotImplementedMethods {

        @Test
        void getProcedures_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getProcedures(null, null, null);
            assertNotNull(rs);
            assertFalse(rs.next());
        }

        @Test
        void getProcedureColumns_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getProcedureColumns(null, null, null, null);
            assertFalse(rs.next());
        }

        @Test
        void getColumnPrivileges_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getColumnPrivileges(null, null, null, null);
            assertFalse(rs.next());
        }

        @Test
        void getTablePrivileges_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getTablePrivileges(null, null, null);
            assertFalse(rs.next());
        }

        @Test
        void getBestRowIdentifier_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getBestRowIdentifier(null, null, null, 0, false);
            assertFalse(rs.next());
        }

        @Test
        void getVersionColumns_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getVersionColumns(null, null, null);
            assertFalse(rs.next());
        }

        @Test
        void getExportedKeys_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getExportedKeys(null, null, null);
            assertFalse(rs.next());
        }

        @Test
        void getCrossReference_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getCrossReference(null, null, null, null, null, null);
            assertFalse(rs.next());
        }

        @Test
        void getUDTs_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getUDTs(null, null, null, null);
            assertFalse(rs.next());
        }

        @Test
        void getSuperTypes_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getSuperTypes(null, null, null);
            assertFalse(rs.next());
        }

        @Test
        void getSuperTables_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getSuperTables(null, null, null);
            assertFalse(rs.next());
        }

        @Test
        void getAttributes_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getAttributes(null, null, null, null);
            assertFalse(rs.next());
        }

        @Test
        void getFunctions_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getFunctions(null, null, null);
            assertFalse(rs.next());
        }

        @Test
        void getFunctionColumns_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getFunctionColumns(null, null, null, null);
            assertFalse(rs.next());
        }

        @Test
        void getPseudoColumns_returnsEmptyResultSet() throws SQLException {
            ResultSet rs = metaData.getPseudoColumns(null, null, null, null);
            assertFalse(rs.next());
        }
    }
}
