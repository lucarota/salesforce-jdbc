package com.ascendix.jdbc.salesforce.connection;

import com.ascendix.jdbc.salesforce.ForceDriver;
import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.metadata.ForceDatabaseMetaData;
import com.ascendix.jdbc.salesforce.statement.ForcePreparedStatement;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ForceConnection implements Connection {

    public static final String FEATURE_IS_NOT_SUPPORTED = "Feature is not supported.";
    private final PartnerConnection partnerConnection;
    private final String uuid;
    /**
     * the updated partner connection in case if we want to support re-login command
     */
    private PartnerConnection partnerConnectionUpdated;

    private final DatabaseMetaData metadata;

    @Getter
    private final PartnerService partnerService;

    private final Properties clientInfo = new Properties();

    public ForceConnection(PartnerConnection partnerConnection, PartnerService partnerService) {
        this.partnerConnection = partnerConnection;
        this.partnerService = partnerService;
        this.metadata = new ForceDatabaseMetaData(this, partnerService);
        this.uuid = UUID.randomUUID().toString();
    }

    public PartnerConnection getPartnerConnection() {
        if (partnerConnectionUpdated != null) {
            return partnerConnectionUpdated;
        }
        return partnerConnection;
    }

    public boolean updatePartnerConnection(String url, String userName, String userPass) throws ConnectionException {
        String currentUserName = null;
        try {
            currentUserName = partnerConnection.getUserInfo().getUserName();
            log.trace(
            "[Conn] updatePartnerConnection IMPLEMENTED newUserName=" + userName + " oldUserName=" + currentUserName
                + " newUrl=" + url);
            ForceConnectionInfo forceConnectionInfo = ForceDriver.parseConnectionUrl(url);
            forceConnectionInfo.setUserName(userName);
            forceConnectionInfo.setPassword(userPass);
            partnerConnectionUpdated = ForceService.createPartnerConnection(forceConnectionInfo);
            log.info("[Conn] updatePartnerConnection UPDATED to newUserName={}", userName);
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

        return true;
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

    @Override
    public <T> T unwrap(Class<T> iface) {
        throw new UnsupportedOperationException(FEATURE_IS_NOT_SUPPORTED);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        throw new UnsupportedOperationException(FEATURE_IS_NOT_SUPPORTED);
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
        // NOT Implemented
    }

    @Override
    public boolean getAutoCommit() {
        return true;
    }

    @Override
    public void commit() {
        log.trace("[Conn] commit NOT_IMPLEMENTED");
    }

    @Override
    public void rollback() {
        log.trace("[Conn] rollback NOT_IMPLEMENTED");
    }

    @Override
    public void close() {
        log.trace("[Conn] close NOT_IMPLEMENTED");
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        // NOT Implemented
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void setCatalog(String catalog) {
        log.trace("[Conn] setCatalog NOT_IMPLEMENTED set to '{}'", catalog);
    }

    @Override
    public String getCatalog() {
        log.trace("[Conn] getCatalog IMPLEMENTED returning {}", ForceDatabaseMetaData.DEFAULT_CATALOG);
        return ForceDatabaseMetaData.DEFAULT_CATALOG;
    }

    @Override
    public void setTransactionIsolation(int level) {
        // NOT Implemented
    }

    @Override
    public int getTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
        // NOT Implemented
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) {
        return new ForcePreparedStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) {
        return new ForcePreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {
        log.trace("[Conn] prepareCall NOT_IMPLEMENTED {}", sql);
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() {
        throw new UnsupportedOperationException(FEATURE_IS_NOT_SUPPORTED);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) {
        // NOT Implemented
    }

    @Override
    public void setHoldability(int holdability) {
        // NOT Implemented
    }

    @Override
    public int getHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() {
        throw new UnsupportedOperationException(FEATURE_IS_NOT_SUPPORTED);
    }

    @Override
    public Savepoint setSavepoint(String name) {
        throw new UnsupportedOperationException(FEATURE_IS_NOT_SUPPORTED);
    }

    @Override
    public void rollback(Savepoint savepoint) {
        rollback();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) {
        log.trace("[Conn] releaseSavepoint NOT_IMPLEMENTED");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return new ForcePreparedStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) {
        return new ForcePreparedStatement(this, sql);
    }
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) {
        return new ForcePreparedStatement(this, sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) {
        return new ForcePreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) {
        return new ForcePreparedStatement(this, sql, columnNames.length > 0 ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) {
        log.trace("[Conn] prepareCall 2 NOT_IMPLEMENTED {}", sql);
        return null;
    }

    @Override
    public Clob createClob() {
        return null;
    }

    @Override
    public Blob createBlob() {
        return null;
    }

    @Override
    public NClob createNClob() {
        return null;
    }

    @Override
    public SQLXML createSQLXML() {
        return null;
    }

    @Override
    public boolean isValid(int timeout) {
        log.trace("[Conn] isValid NOT_IMPLEMENTED");
        return true;
    }

    @Override
    public void setClientInfo(String name, String value) {
        log.trace("[Conn] setClientInfo 1 IMPLEMENTED {}={}", name, value);
        clientInfo.setProperty(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) {
        log.trace("[Conn] setClientInfo 2 IMPLEMENTED properties<>");
        properties.stringPropertyNames()
            .forEach(propName -> clientInfo.setProperty(propName, properties.getProperty(propName)));
    }

    @Override
    public String getClientInfo(String name) {
        log.trace("[Conn] getClientInfo 1 IMPLEMENTED for '{}'", name);
        return clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() {
        log.trace("[Conn] getClientInfo 2 IMPLEMENTED ");
        return clientInfo;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) {
        return null;
    }

    @Override
    public void setSchema(String schema) {
        log.trace("[Conn] setSchema NOT_IMPLEMENTED");
    }

    @Override
    public void abort(Executor executor) {
        // NOT Implemented
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
        // NOT Implemented
    }

    @Override
    public int getNetworkTimeout() {
        return 0;
    }

    public String getUUID() {
        return this.uuid;
    }
}
