package it.rotaliano.jdbc.salesforce.statement;

import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Base class providing default stub implementations for {@link PreparedStatement} methods
 * that are not supported by the Salesforce JDBC driver. Subclasses override only the
 * methods they actually implement.
 */
public abstract class AbstractPreparedStatement implements PreparedStatement {

    // --- Statement stubs: no-op void methods ---

    @Override
    public void setQueryTimeout(int seconds) {
    }

    @Override
    public void cancel() {
    }

    @Override
    public void setCursorName(String name) {
    }

    @Override
    public void setMaxFieldSize(int max) {
    }

    @Override
    public void setEscapeProcessing(boolean enable) {
    }

    @Override
    public void closeOnCompletion() {
    }

    // --- Statement stubs: zero/false/constant returning ---

    @Override
    public int getMaxFieldSize() {
        return 0;
    }

    @Override
    public int getQueryTimeout() {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean getMoreResults(int current) {
        return false;
    }

    @Override
    public int getResultSetHoldability() {
        return 0;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public boolean isPoolable() {
        return false;
    }

    @Override
    public boolean isCloseOnCompletion() {
        return false;
    }

    // --- PreparedStatement stubs: throw SQLFeatureNotSupportedException ---

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("The addBatch is not implemented yet.");
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("The addBatch is not implemented yet.");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("The clearBatch is not implemented yet.");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("The executeBatch is not implemented yet.");
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setPoolable is not implemented yet.");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setRowId is not implemented yet.");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setNCharacterStream is not implemented yet.");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setNCharacterStream is not implemented yet.");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setNClob is not implemented yet.");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setNClob is not implemented yet.");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setNClob is not implemented yet.");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setBlob is not implemented yet.");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setBlob is not implemented yet.");
    }

    @Override
    public void setBlob(int i, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setBlob is not implemented yet.");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setSQLXML is not implemented yet.");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setBinaryStream is not implemented yet.");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setBinaryStream is not implemented yet.");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setBinaryStream is not implemented yet.");
    }

    @Override
    public void setArray(int i, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setArray is not implemented yet.");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setAsciiStream is not implemented yet.");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setAsciiStream is not implemented yet.");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setAsciiStream is not implemented yet.");
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setBytes is not implemented yet.");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setCharacterStream is not implemented yet.");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setCharacterStream is not implemented yet.");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setCharacterStream is not implemented yet.");
    }

    @Override
    public void setClob(int i, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setClob is not implemented yet.");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setClob is not implemented yet.");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setClob is not implemented yet.");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setDate is not implemented yet.");
    }

    @Override
    public void setRef(int i, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setRef is not implemented yet.");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setTime is not implemented yet.");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setTimestamp is not implemented yet.");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setURL is not implemented yet.");
    }

    /**
     * @deprecated Use {@code setCharacterStream}
     */
    @Deprecated(since = "1.2")
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("The setUnicodeStream is not implemented yet.");
    }
}
