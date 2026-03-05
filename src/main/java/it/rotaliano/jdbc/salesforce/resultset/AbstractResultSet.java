package it.rotaliano.jdbc.salesforce.resultset;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * Base class providing default no-op/stub implementations for {@link ResultSet} methods
 * that are not supported by the Salesforce JDBC driver. Subclasses override only the
 * methods they actually implement.
 */
public abstract class AbstractResultSet implements ResultSet {

    // --- Navigation stubs (return false) ---

    @Override
    public boolean absolute(int row) {
        return false;
    }

    @Override
    public boolean previous() {
        return false;
    }

    @Override
    public boolean relative(int rows) {
        return false;
    }

    // --- Row status stubs (return false) ---

    @Override
    public boolean rowDeleted() {
        return false;
    }

    @Override
    public boolean rowInserted() {
        return false;
    }

    @Override
    public boolean rowUpdated() {
        return false;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    // --- Zero-returning stubs ---

    @Override
    public int findColumn(String columnName) {
        return 0;
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public int getRow() {
        return 0;
    }

    // --- Null-returning stubs ---

    @Override
    public Array getArray(int i) {
        return null;
    }

    @Override
    public Array getArray(String colName) {
        return null;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) {
        return null;
    }

    @Override
    public InputStream getAsciiStream(String columnName) {
        return null;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) {
        return null;
    }

    @Override
    public Reader getCharacterStream(String columnName) {
        return null;
    }

    @Override
    public Clob getClob(int i) {
        return null;
    }

    @Override
    public Clob getClob(String colName) {
        return null;
    }

    @Override
    public String getCursorName() {
        return null;
    }

    @Override
    public Object getObject(int i, Map<String, Class<?>> map) {
        return null;
    }

    @Override
    public Object getObject(String colName, Map<String, Class<?>> map) {
        return null;
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) {
        return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) {
        return null;
    }

    @Override
    public Ref getRef(int i) {
        return null;
    }

    @Override
    public Ref getRef(String colName) {
        return null;
    }

    @Override
    public Statement getStatement() {
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) {
        return null;
    }

    @Override
    public Time getTime(String columnName, Calendar cal) {
        return null;
    }

    @Override
    public URL getURL(int columnIndex) {
        return null;
    }

    @Override
    public URL getURL(String columnName) {
        return null;
    }

    @Override
    @Deprecated(since = "1.2")
    public InputStream getUnicodeStream(int columnIndex) {
        return null;
    }

    @Override
    @Deprecated(since = "1.2")
    public InputStream getUnicodeStream(String columnName) {
        return null;
    }

    @Override
    public RowId getRowId(int columnIndex) {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) {
        return null;
    }

    @Override
    public NClob getNClob(int columnIndex) {
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel) {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) {
        return null;
    }

    @Override
    public String getNString(int columnIndex) {
        return null;
    }

    @Override
    public String getNString(String columnLabel) {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    // --- Unsupported stubs (throw) ---

    @Override
    public void insertRow() throws SQLException {
        throw new SQLException("Feature is not supported.", "HY000", 1);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public Date getDate(String columnName, Calendar cal) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public Timestamp getTimestamp(String columnName, Calendar cal) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public InputStream getBinaryStream(String columnName) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    // --- No-op void stubs ---

    @Override
    public void afterLast() {
    }

    @Override
    public void cancelRowUpdates() {
    }

    @Override
    public void close() {
    }

    @Override
    public void deleteRow() {
    }

    @Override
    public void moveToCurrentRow() {
    }

    @Override
    public void moveToInsertRow() {
    }

    @Override
    public void refreshRow() {
    }

    @Override
    public void setFetchDirection(int direction) {
    }

    @Override
    public void setFetchSize(int rows) {
    }

    @Override
    public void updateRow() {
    }

    // --- update*() stubs ---

    @Override
    public void updateArray(int columnIndex, Array x) {
    }

    @Override
    public void updateArray(String columnName, Array x) {
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) {
    }

    @Override
    public void updateAsciiStream(String columnName, InputStream x, int length) {
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) {
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) {
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) {
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) {
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) {
    }

    @Override
    public void updateBigDecimal(String columnName, BigDecimal x) {
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) {
    }

    @Override
    public void updateBinaryStream(String columnName, InputStream x, int length) {
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) {
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) {
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) {
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) {
    }

    @Override
    public void updateBlob(int columnIndex, java.sql.Blob x) {
    }

    @Override
    public void updateBlob(String columnName, java.sql.Blob x) {
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) {
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) {
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) {
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) {
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) {
    }

    @Override
    public void updateBoolean(String columnName, boolean x) {
    }

    @Override
    public void updateByte(int columnIndex, byte x) {
    }

    @Override
    public void updateByte(String columnName, byte x) {
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) {
    }

    @Override
    public void updateBytes(String columnName, byte[] x) {
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) {
    }

    @Override
    public void updateCharacterStream(String columnName, Reader reader, int length) {
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) {
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) {
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) {
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) {
    }

    @Override
    public void updateClob(int columnIndex, Clob x) {
    }

    @Override
    public void updateClob(String columnName, Clob x) {
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) {
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) {
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) {
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) {
    }

    @Override
    public void updateDate(int columnIndex, Date x) {
    }

    @Override
    public void updateDate(String columnName, Date x) {
    }

    @Override
    public void updateDouble(int columnIndex, double x) {
    }

    @Override
    public void updateDouble(String columnName, double x) {
    }

    @Override
    public void updateFloat(int columnIndex, float x) {
    }

    @Override
    public void updateFloat(String columnName, float x) {
    }

    @Override
    public void updateInt(int columnIndex, int x) {
    }

    @Override
    public void updateInt(String columnName, int x) {
    }

    @Override
    public void updateLong(int columnIndex, long x) {
    }

    @Override
    public void updateLong(String columnName, long x) {
    }

    @Override
    public void updateNull(int columnIndex) {
    }

    @Override
    public void updateNull(String columnName) {
    }

    @Override
    public void updateObject(int columnIndex, Object x) {
    }

    @Override
    public void updateObject(String columnName, Object x) {
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scale) {
    }

    @Override
    public void updateObject(String columnName, Object x, int scale) {
    }

    @Override
    public void updateRef(int columnIndex, Ref x) {
    }

    @Override
    public void updateRef(String columnName, Ref x) {
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) {
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) {
    }

    @Override
    public void updateShort(int columnIndex, short x) {
    }

    @Override
    public void updateShort(String columnName, short x) {
    }

    @Override
    public void updateString(int columnIndex, String x) {
    }

    @Override
    public void updateString(String columnName, String x) {
    }

    @Override
    public void updateTime(int columnIndex, Time x) {
    }

    @Override
    public void updateTime(String columnName, Time x) {
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) {
    }

    @Override
    public void updateTimestamp(String columnName, Timestamp x) {
    }

    @Override
    public void updateNString(int columnIndex, String nString) {
    }

    @Override
    public void updateNString(String columnLabel, String nString) {
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) {
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) {
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) {
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) {
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) {
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) {
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) {
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) {
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) {
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) {
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) {
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) {
    }
}
