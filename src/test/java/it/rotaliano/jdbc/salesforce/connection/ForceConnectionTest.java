package it.rotaliano.jdbc.salesforce.connection;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectorConfig;
import it.rotaliano.jdbc.salesforce.delegates.PartnerService;
import it.rotaliano.jdbc.salesforce.metadata.ForceDatabaseMetaData;
import it.rotaliano.jdbc.salesforce.statement.ForcePreparedStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class ForceConnectionTest {

    private PartnerConnection partnerConnection;
    private PartnerService partnerService;
    private ForceConnection connection;

    @BeforeEach
    void setUp() {
        partnerConnection = mock(PartnerConnection.class);
        partnerService = mock(PartnerService.class);

        ConnectorConfig config = new ConnectorConfig();
        config.setAuthEndpoint("https://login.salesforce.com/services/Soap/u/62.0");
        config.setServiceEndpoint("https://login.salesforce.com/services/Soap/u/62.0");
        config.setRestEndpoint("https://login.salesforce.com/services/data/v62.0");
        when(partnerConnection.getConfig()).thenReturn(config);

        connection = new ForceConnection(partnerConnection, partnerService);
    }

    @Nested
    class ConstructorAndIdentity {

        @Test
        void getUUID_returnsNonNullUniqueId() {
            assertNotNull(connection.getUUID());
            assertFalse(connection.getUUID().isEmpty());
        }

        @Test
        void twoConnections_haveDifferentUUIDs() {
            ForceConnection other = new ForceConnection(partnerConnection, partnerService);
            assertFalse(connection.getUUID().equals(other.getUUID()));
        }

        @Test
        void getPartnerService_returnsInjectedService() {
            assertSame(partnerService, connection.getPartnerService());
        }
    }

    @Nested
    class PartnerConnectionTests {

        @Test
        void getPartnerConnection_returnsOriginalByDefault() {
            assertSame(partnerConnection, connection.getPartnerConnection());
        }
    }

    @Nested
    class MetadataTests {

        @Test
        void getMetaData_returnsForceDatabaseMetaData() {
            assertNotNull(connection.getMetaData());
            assertInstanceOf(ForceDatabaseMetaData.class, connection.getMetaData());
        }

        @Test
        void getMetaData_returnsSameInstanceOnMultipleCalls() {
            assertSame(connection.getMetaData(), connection.getMetaData());
        }
    }

    @Nested
    class SchemaAndCatalog {

        @Test
        void getSchema_returnsSalesforce() {
            assertEquals("Salesforce", connection.getSchema());
        }

        @Test
        void getCatalog_returnsDefaultCatalog() {
            assertEquals(ForceDatabaseMetaData.DEFAULT_CATALOG, connection.getCatalog());
        }

        @Test
        void setCatalog_doesNotThrow() {
            assertDoesNotThrow(() -> connection.setCatalog("anything"));
        }

        @Test
        void setSchema_doesNotThrow() {
            assertDoesNotThrow(() -> connection.setSchema("anything"));
        }
    }

    @Nested
    class StatementCreation {

        @Test
        void createStatement_returnsForcePreparedStatement() {
            assertInstanceOf(ForcePreparedStatement.class, connection.createStatement());
        }

        @Test
        void createStatementWithTypeAndConcurrency_returnsForcePreparedStatement() {
            Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            assertInstanceOf(ForcePreparedStatement.class, stmt);
        }

        @Test
        void createStatementWithHoldability_returnsForcePreparedStatement() {
            Statement stmt = connection.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            assertInstanceOf(ForcePreparedStatement.class, stmt);
        }

        @Test
        void prepareStatement_returnsForcePreparedStatement() {
            assertInstanceOf(ForcePreparedStatement.class, connection.prepareStatement("SELECT Id FROM Account"));
        }

        @Test
        void prepareStatementWithTypeAndConcurrency_returnsForcePreparedStatement() {
            assertInstanceOf(ForcePreparedStatement.class,
                connection.prepareStatement("SELECT Id FROM Account", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        }

        @Test
        void prepareStatementWithHoldability_returnsForcePreparedStatement() {
            assertInstanceOf(ForcePreparedStatement.class,
                connection.prepareStatement("SELECT Id FROM Account",
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
        }

        @Test
        void prepareStatementWithAutoGeneratedKeys_returnsForcePreparedStatement() {
            assertInstanceOf(ForcePreparedStatement.class,
                connection.prepareStatement("INSERT INTO Account(Name) VALUES('Test')", Statement.RETURN_GENERATED_KEYS));
        }

        @Test
        void prepareStatementWithColumnIndexes_returnsForcePreparedStatement() {
            assertInstanceOf(ForcePreparedStatement.class,
                connection.prepareStatement("INSERT INTO Account(Name) VALUES('Test')", new int[]{1}));
        }

        @Test
        void prepareStatementWithColumnNames_returnsForcePreparedStatement() {
            assertInstanceOf(ForcePreparedStatement.class,
                connection.prepareStatement("INSERT INTO Account(Name) VALUES('Test')", new String[]{"Id"}));
        }

        @Test
        void prepareCall_returnsNull() {
            assertNull(connection.prepareCall("CALL proc()"));
        }

        @Test
        void prepareCallWithTypeAndConcurrency_returnsNull() {
            assertNull(connection.prepareCall("CALL proc()", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        }

        @Test
        void prepareCallWithHoldability_returnsNull() {
            assertNull(connection.prepareCall("CALL proc()",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
        }
    }

    @Nested
    class TransactionAndState {

        @Test
        void getAutoCommit_returnsTrue() {
            assertTrue(connection.getAutoCommit());
        }

        @Test
        void setAutoCommit_doesNotThrow() {
            assertDoesNotThrow(() -> connection.setAutoCommit(false));
        }

        @Test
        void isClosed_returnsFalse() {
            assertFalse(connection.isClosed());
        }

        @Test
        void isReadOnly_returnsFalse() {
            assertFalse(connection.isReadOnly());
        }

        @Test
        void isValid_returnsTrue() {
            assertTrue(connection.isValid(0));
        }

        @Test
        void getTransactionIsolation_returnsNone() {
            assertEquals(Connection.TRANSACTION_NONE, connection.getTransactionIsolation());
        }

        @Test
        void getHoldability_returnsCloseCursorsAtCommit() {
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, connection.getHoldability());
        }

        @Test
        void getWarnings_returnsNull() {
            assertNull(connection.getWarnings());
        }

        @Test
        void nativeSQL_returnsNull() {
            assertNull(connection.nativeSQL("SELECT 1"));
        }

        @Test
        void noOpMethods_doNotThrow() {
            assertDoesNotThrow(() -> connection.commit());
            assertDoesNotThrow(() -> connection.rollback());
            assertDoesNotThrow(() -> connection.close());
            assertDoesNotThrow(() -> connection.setReadOnly(true));
            assertDoesNotThrow(() -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
            assertDoesNotThrow(() -> connection.clearWarnings());
            assertDoesNotThrow(() -> connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
            assertDoesNotThrow(() -> connection.rollback(null));
            assertDoesNotThrow(() -> connection.releaseSavepoint(null));
            assertDoesNotThrow(() -> connection.abort(null));
            assertDoesNotThrow(() -> connection.setNetworkTimeout(null, 1000));
        }

        @Test
        void getNetworkTimeout_returnsZero() {
            assertEquals(0, connection.getNetworkTimeout());
        }
    }

    @Nested
    class ClientInfoTests {

        @Test
        void setAndGetClientInfo_byName() {
            connection.setClientInfo("appName", "TestApp");
            assertEquals("TestApp", connection.getClientInfo("appName"));
        }

        @Test
        void getClientInfo_unknownKey_returnsNull() {
            assertNull(connection.getClientInfo("nonexistent"));
        }

        @Test
        void setClientInfo_withProperties() {
            Properties props = new Properties();
            props.setProperty("key1", "val1");
            props.setProperty("key2", "val2");
            connection.setClientInfo(props);

            assertEquals("val1", connection.getClientInfo("key1"));
            assertEquals("val2", connection.getClientInfo("key2"));
        }

        @Test
        void getClientInfo_returnsAllProperties() {
            connection.setClientInfo("a", "1");
            connection.setClientInfo("b", "2");
            Properties info = connection.getClientInfo();
            assertEquals("1", info.getProperty("a"));
            assertEquals("2", info.getProperty("b"));
        }
    }

    @Nested
    class UnsupportedOperations {

        @Test
        void unwrap_throwsUnsupportedOperationException() {
            assertThrows(UnsupportedOperationException.class, () -> connection.unwrap(Connection.class));
        }

        @Test
        void isWrapperFor_throwsUnsupportedOperationException() {
            assertThrows(UnsupportedOperationException.class, () -> connection.isWrapperFor(Connection.class));
        }

        @Test
        void getTypeMap_throwsUnsupportedOperationException() {
            assertThrows(UnsupportedOperationException.class, () -> connection.getTypeMap());
        }

        @Test
        void setSavepoint_throwsUnsupportedOperationException() {
            assertThrows(UnsupportedOperationException.class, () -> connection.setSavepoint());
        }

        @Test
        void setSavepointWithName_throwsUnsupportedOperationException() {
            assertThrows(UnsupportedOperationException.class, () -> connection.setSavepoint("sp1"));
        }
    }

    @Nested
    class NullReturningFactories {

        @Test
        void createClob_returnsNull() {
            assertNull(connection.createClob());
        }

        @Test
        void createBlob_returnsNull() {
            assertNull(connection.createBlob());
        }

        @Test
        void createNClob_returnsNull() {
            assertNull(connection.createNClob());
        }

        @Test
        void createSQLXML_returnsNull() {
            assertNull(connection.createSQLXML());
        }

        @Test
        void createArrayOf_returnsNull() {
            assertNull(connection.createArrayOf("VARCHAR", new Object[]{}));
        }

        @Test
        void createStruct_returnsNull() {
            assertNull(connection.createStruct("MY_TYPE", new Object[]{}));
        }
    }
}
