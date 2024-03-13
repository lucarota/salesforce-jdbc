package com.ascendix.jdbc.salesforce.connection;

import com.ascendix.jdbc.salesforce.ForceDriver;
import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.metadata.ForceDatabaseMetaData;
import com.ascendix.jdbc.salesforce.statement.ForcePreparedStatement;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ForceConnection implements Connection {

    private final PartnerConnection partnerConnection;
    /**
     * the updated partner connection in case if we want to support re-login command
     */
    private PartnerConnection partnerConnectionUpdated;

    private final DatabaseMetaData metadata;

    @Getter
    private final PartnerService partnerService;



    private final Map<String, DescribeSObjectResult> connectionCache = new HashMap<>();
    private final Properties clientInfo = new Properties();

    public ForceConnection(PartnerConnection partnerConnection, PartnerService partnerService) {
        this.partnerConnection = partnerConnection;
        this.partnerService = partnerService;
        this.metadata = new ForceDatabaseMetaData(this, partnerService);
    }

    public PartnerConnection getPartnerConnection() {
        if (partnerConnectionUpdated != null) {
            return partnerConnectionUpdated;
        }
        return partnerConnection;
    }

    public boolean updatePartnerConnection(String url, String userName, String userPass) throws ConnectionException {
        boolean result = false;
        String currentUserName = null;
        try {
            currentUserName = partnerConnection.getUserInfo().getUserName();
            log.trace(
            "[Conn] updatePartnerConnection IMPLEMENTED newUserName=" + userName + " oldUserName=" + currentUserName
                + " newUrl=" + url);
            ConnectorConfig partnerConfig = partnerConnection.getConfig();
            ForceConnectionInfo forceConnectionInfo = ForceDriver.parseConnectionUrl(url);
            forceConnectionInfo.setUserName(userName);
            forceConnectionInfo.setPassword(userPass);
            partnerConnectionUpdated = ForceService.createPartnerConnection(forceConnectionInfo);
            log.info("[Conn] updatePartnerConnection UPDATED to newUserName={}", userName);
            result = true;
        } catch (IOException e) {
            log.error(
                    "[Conn] updatePartnerConnection UPDATE FAILED to newUserName=" + userName + " currentUserName="
                            + currentUserName,
                    e);
            throw new ConnectionException(e.getMessage());
        } catch (ConnectionException e) {
            log.error(
                "[Conn] updatePartnerConnection UPDATE FAILED to newUserName=" + userName + " currentUserName="
                    + currentUserName,
                e);
            throw e;
        }

        return result;
    }

    public DatabaseMetaData getMetaData() {
        return metadata;
    }

    @Override
    public PreparedStatement prepareStatement(String soql) {
        log.trace("[Conn] prepareStatement IMPLEMENTED {}", soql);
        return new ForcePreparedStatement(this, soql);
    }

    @Override
    public String getSchema() {
        return "Salesforce";
    }

    public Map<String, DescribeSObjectResult> getCache() {
        return connectionCache;
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
    public Statement createStatement() {
        log.trace("[Conn] createStatement 1 IMPLEMENTED");
        return new ForcePreparedStatement(this);
    }

    @Override
    public CallableStatement prepareCall(String sql) {
        log.trace("[Conn] prepareCall NOT_IMPLEMENTED");
        return null;
    }

    @Override
    public String nativeSQL(String sql) {
        log.trace("[Conn] nativeSQL NOT_IMPLEMENTED");
        return null;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    @Override
    public void commit() throws SQLException {
        log.trace("[Conn] commit NOT_IMPLEMENTED");
    }

    @Override
    public void rollback() throws SQLException {
        log.trace("[Conn] rollback NOT_IMPLEMENTED");
    }

    @Override
    public void close() throws SQLException {
        log.trace("[Conn] close NOT_IMPLEMENTED");
    }

    @Override
    public boolean isClosed() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        // TODO Auto-generated method stub
        log.trace("[Conn] setCatalog NOT_IMPLEMENTED set to '{}'", catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        log.trace("[Conn] getCatalog IMPLEMENTED returning {}", ForceDatabaseMetaData.DEFAULT_CATALOG);
        return ForceDatabaseMetaData.DEFAULT_CATALOG;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new ForcePreparedStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        return new ForcePreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        log.trace("[Conn] prepareCall NOT_IMPLEMENTED {}", sql);
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException("Feature is not supported.");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        rollback();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        log.trace("[Conn] releaseSavepoint NOT_IMPLEMENTED");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        return new ForcePreparedStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws SQLException {
        return new ForcePreparedStatement(this, sql);
    }
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new ForcePreparedStatement(this, sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new ForcePreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return new ForcePreparedStatement(this, sql, columnNames.length > 0 ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws SQLException {
        log.trace("[Conn] prepareCall 2 NOT_IMPLEMENTED {}", sql);
        return null;
    }

    @Override
    public Clob createClob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        // TODO Auto-generated method stub
        log.trace("[Conn] isValid NOT_IMPLEMENTED");
        return true;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // TODO Auto-generated method stub
        log.trace("[Conn] setClientInfo 1 IMPLEMENTED {}={}", name, value);
        clientInfo.setProperty(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        log.trace("[Conn] setClientInfo 2 IMPLEMENTED properties<>");
        properties.stringPropertyNames()
            .forEach(propName -> clientInfo.setProperty(propName, properties.getProperty(propName)));
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        log.trace("[Conn] getClientInfo 1 IMPLEMENTED for '{}'", name);
        return clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        log.trace("[Conn] getClientInfo 2 IMPLEMENTED ");
        return clientInfo;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        // TODO Auto-generated method stub
        log.trace("[Conn] setSchema NOT_IMPLEMENTED");
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }
}
