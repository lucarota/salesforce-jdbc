package com.ascendix.jdbc.salesforce.resultset;

import com.ascendix.jdbc.salesforce.metadata.ColumnMap;
import com.ascendix.jdbc.salesforce.metadata.TypeInfo;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.serial.SerialBlob;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CachedResultSet implements ResultSet, Serializable {

    public static final CachedResultSet EMPTY = new CachedResultSet(Collections.emptyList(), new RowSetMetaDataImpl());

    @Serial
    private static final long serialVersionUID = 1L;

    private transient Integer index;
    private List<ColumnMap<String, Object>> rows;
    private ResultSetMetaData metadata;
    private SQLWarning warningsChain;

    private transient Iterator<List<ColumnMap<String, Object>>> rowSupplier;

    public CachedResultSet(List<ColumnMap<String, Object>> rows) {
        this.rows = new ArrayList<>(rows);
        this.index = -1;
    }

    public CachedResultSet(List<ColumnMap<String, Object>> rows, ResultSetMetaData metadata) {
        this(new ArrayList<>(rows));
        this.metadata = metadata;
    }

    public CachedResultSet(ResultSetMetaData metadata) {
        this(new ArrayList<>());
        this.metadata = metadata;
    }

    public CachedResultSet(ColumnMap<String, Object> singleRow) {
        this(new ArrayList<>(Collections.singletonList(singleRow)));
    }

    public CachedResultSet(ColumnMap<String, Object> singleRow, ResultSetMetaData metadata) {
        this(new ArrayList<>(Collections.singletonList(singleRow)), metadata);
    }

    public CachedResultSet(Iterator<List<ColumnMap<String, Object>>> rowSupplier, ResultSetMetaData metadata) {
        this(metadata);
        this.rowSupplier = rowSupplier;
    }

    public Object getObject(String columnName) {
        return rows.get(index).get(columnName);
    }

    public Object getObject(int columnIndex) {
        return rows.get(index).getByIndex(columnIndex);
    }

    protected void addRow(ColumnMap<String, Object> row) {
        rows.add(row);
    }

    private void increaseIndex() {
        index++;
    }

    public boolean first() {
        if (this.rowSupplier != null) throw new UnsupportedOperationException("Not implemented yet.");
        if (!rows.isEmpty()) {
            this.index = 0;
            return true;
        } else {
            return false;
        }
    }

    public boolean last() {
        if (this.rowSupplier != null) throw new UnsupportedOperationException("Not implemented yet.");
        if (!rows.isEmpty()) {
            this.index = rows.size() - 1;
            return true;
        } else {
            return false;
        }
    }

    private boolean nextSupplied() {
        if (this.rowSupplier != null) {
            if (this.rowSupplier.hasNext()) {
                this.rows = this.rowSupplier.next();
                this.index = -1;
                return !rows.isEmpty();
            } else {
                this.rows = Collections.emptyList();
                return false;
            }
        }
        return false;
    }

    public boolean next() {
        if (!rows.isEmpty()) {
            increaseIndex();
            if (index < rows.size()) return true;
        }
        return nextSupplied() && next();
    }

    public boolean isAfterLast() {
        if (this.rowSupplier != null) throw new UnsupportedOperationException("Not implemented yet.");
        return !rows.isEmpty() && index == rows.size();
    }

    public boolean isBeforeFirst() {
        if (this.rowSupplier != null) throw new UnsupportedOperationException("Not implemented yet.");
        return !rows.isEmpty() && index == -1;
    }

    public boolean isFirst() {
        if (this.rowSupplier != null) throw new UnsupportedOperationException("Not implemented yet.");
        return !rows.isEmpty() && index == 0;
    }

    public boolean isLast() {
        if (this.rowSupplier != null) throw new UnsupportedOperationException("Not implemented yet.");
        return !rows.isEmpty() && index == rows.size() - 1;
    }

    public ResultSetMetaData getMetaData() {
        return metadata != null ? metadata : CachedResultSetMetaData.EMPTY;
    }

    public void setFetchSize(int rows) {
        // NOT Implemented
    }

    public Date getDate(int columnIndex, Calendar cal) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    public Date getDate(String columnName, Calendar cal) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private class ColumnValueParser<T> {

        private final Function<String, T> conversion;
        private final Class<T> clazz;

        public ColumnValueParser(Function<String, T> parser, Class<T> clazz) {
            this.conversion = parser;
            this.clazz = clazz;
        }

        public Optional<T> parse(int columnIndex) {
            Object value = getObject(columnIndex);
            return parse(value);
        }

        public Optional<T> parse(String columnName) {
            Object value = getObject(columnName);
            return parse(value);
        }

        @SuppressWarnings("unchecked")
        private Optional<T> parse(Object o) {
            if (o == null) {
                return Optional.empty();
            }

            if (o instanceof String val) {
                return Optional.of(conversion.apply(val));
            }

            if (o.getClass().isAssignableFrom(clazz)) {
                return (Optional<T>) Optional.of(o);
            }

            if (Boolean.class.isAssignableFrom(clazz)) {
                if (o instanceof Number num) {
                    return (Optional<T>) Optional.of(num.intValue() == 1);
                }
            }

            return Optional.of(conversion.apply(o.toString()));
        }
    }

    public String getString(String columnName) {
        return convertToString(getObject(columnName));
    }

    public String getString(int columnIndex) {
        return convertToString(getObject(columnIndex));
    }

    public BigDecimal getBigDecimal(int columnIndex) {
        return new ColumnValueParser<>(BigDecimal::new, BigDecimal.class)
            .parse(columnIndex)
            .orElse(null);
    }

    public BigDecimal getBigDecimal(String columnName) {
        return new ColumnValueParser<>(BigDecimal::new, BigDecimal.class)
            .parse(columnName)
            .orElse(null);
    }

    protected java.util.Date parseDate(String dateRepr) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd").parse(dateRepr);
        } catch (ParseException e) {
            return null;
        }
    }

    public Date getDate(int columnIndex) {
        return new ColumnValueParser<>(this::parseDate, java.util.Date.class)
            .parse(columnIndex)
            .map(d -> new java.sql.Date(d.getTime()))
            .orElse(null);
    }

    public Date getDate(String columnName) {
        return new ColumnValueParser<>(this::parseDate, java.util.Date.class)
            .parse(columnName)
            .map(d -> new java.sql.Date(d.getTime()))
            .orElse(null);
    }

    private java.util.Date parseDateTime(String dateRepr) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").parse(dateRepr);
        } catch (ParseException e) {
            return null;
        }
    }

    public Timestamp getTimestamp(int columnIndex) {
        Object value = rows.get(index).getByIndex(columnIndex);
        if (value instanceof GregorianCalendar calendar) {
            return new java.sql.Timestamp(calendar.getTime().getTime());
        } else if (rows.get(index).getTypeInfoByIndex(columnIndex) == TypeInfo.DATE_TYPE_INFO) {
            return new ColumnValueParser<>(this::parseDate, java.util.Date.class)
                .parse(columnIndex)
                .map(d -> new java.sql.Timestamp(d.getTime()))
                .orElse(null);
        } else {
            return new ColumnValueParser<>(this::parseDateTime, java.util.Date.class)
                .parse(columnIndex)
                .map(d -> new java.sql.Timestamp(d.getTime()))
                .orElse(null);
        }
    }

    public Timestamp getTimestamp(String columnName) {
        Object value = rows.get(index).get(columnName);
        if (value instanceof GregorianCalendar cal) {
            return new java.sql.Timestamp(cal.getTime().getTime());
        } else if (rows.get(index).getTypeInfo(columnName) == TypeInfo.DATE_TYPE_INFO) {
            return new ColumnValueParser<>(this::parseDate, java.util.Date.class)
                .parse(columnName)
                .map(d -> new java.sql.Timestamp(d.getTime()))
                .orElse(null);

        } else {
            return new ColumnValueParser<>(this::parseDateTime, java.util.Date.class)
                .parse(columnName)
                .map(d -> new java.sql.Timestamp(d.getTime()))
                .orElse(null);
        }
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    public Timestamp getTimestamp(String columnName, Calendar cal) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private java.util.Date parseTime(String dateRepr) {
        try {
            return new SimpleDateFormat("HH:mm:ss.SSSX").parse(dateRepr);
        } catch (ParseException e) {
            return null;
        }
    }

    public Time getTime(String columnName) {
        return new ColumnValueParser<>(this::parseTime, java.util.Date.class)
            .parse(columnName)
            .map(d -> new Time(d.getTime()))
            .orElse(null);
    }

    public Time getTime(int columnIndex) {
        return new ColumnValueParser<>(this::parseTime, java.util.Date.class)
            .parse(columnIndex)
            .map(d -> new Time(d.getTime()))
            .orElse(null);
    }

    @Deprecated(since="1.2")
    public BigDecimal getBigDecimal(int columnIndex, int scale) {
        Optional<BigDecimal> result = new ColumnValueParser<>(BigDecimal::new, BigDecimal.class)
            .parse(columnIndex);

        return result.map(bigDecimal -> bigDecimal.setScale(scale, RoundingMode.HALF_EVEN)).orElse(null);
    }

    @Deprecated(since="1.2")
    public BigDecimal getBigDecimal(String columnName, int scale) {
        Optional<BigDecimal> result = new ColumnValueParser<>(BigDecimal::new, BigDecimal.class)
            .parse(columnName);
        return result.map(bigDecimal -> bigDecimal.setScale(scale, RoundingMode.HALF_EVEN)).orElse(null);
    }

    public float getFloat(int columnIndex) {
        return new ColumnValueParser<>(Float::parseFloat, Float.class)
            .parse(columnIndex)
            .orElse(0f);
    }

    public float getFloat(String columnName) {
        return new ColumnValueParser<>(Float::parseFloat, Float.class)
            .parse(columnName)
            .orElse(0f);
    }

    public double getDouble(int columnIndex) {
        return new ColumnValueParser<>(Double::parseDouble, Double.class)
            .parse(columnIndex)
            .orElse(0d);
    }

    public double getDouble(String columnName) {
        return new ColumnValueParser<>(Double::parseDouble, Double.class)
            .parse(columnName)
            .orElse(0d);
    }

    public long getLong(String columnName) {
        return new ColumnValueParser<>(Long::parseLong, Long.class)
            .parse(columnName)
            .orElse(0L);
    }

    public long getLong(int columnIndex) {
        return new ColumnValueParser<Long>(Long::parseLong, Long.class)
            .parse(columnIndex)
            .orElse(0L);
    }

    public int getInt(String columnName) {
        return new ColumnValueParser<>(Double::parseDouble, Double.class)
            .parse(columnName)
            .orElse(0d)
            .intValue();
    }

    public int getInt(int columnIndex) {
        return new ColumnValueParser<>(Double::parseDouble, Double.class)
            .parse(columnIndex)
            .orElse(0d)
            .intValue();
    }

    public short getShort(String columnName) {
        return new ColumnValueParser<>(Double::parseDouble, Double.class)
            .parse(columnName)
            .orElse(0d)
            .shortValue();
    }

    public short getShort(int columnIndex) {
        return new ColumnValueParser<>(Double::parseDouble, Double.class)
            .parse(columnIndex)
            .orElse(0d)
            .shortValue();
    }

    public InputStream getBinaryStream(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    public InputStream getBinaryStream(String columnName) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private Blob createBlob(byte[] data) {
        try {
            return new SerialBlob(data);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Blob getBlob(int columnIndex) {
        return new ColumnValueParser<>((v) -> Base64.getDecoder().decode(v), byte[].class)
            .parse(columnIndex)
            .map(this::createBlob)
            .orElse(null);
    }

    public Blob getBlob(String columnName) {
        return new ColumnValueParser<>((v) -> Base64.getDecoder().decode(v), byte[].class)
            .parse(columnName)
            .map(this::createBlob)
            .orElse(null);
    }

    public boolean getBoolean(int columnIndex) {
        return new ColumnValueParser<>(Boolean::parseBoolean, Boolean.class)
            .parse(columnIndex)
            .orElse(false);
    }

    public boolean getBoolean(String columnName) {
        return new ColumnValueParser<>(Boolean::parseBoolean, Boolean.class)
            .parse(columnName)
            .orElse(false);
    }

    public byte getByte(int columnIndex) {
        return new ColumnValueParser<>(Byte::parseByte, Byte.class)
            .parse(columnIndex)
            .orElse((byte) 0);
    }

    public byte getByte(String columnName) {
        return new ColumnValueParser<>(Byte::parseByte, Byte.class)
            .parse(columnName)
            .orElse((byte) 0);
    }

    public byte[] getBytes(int columnIndex) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    public byte[] getBytes(String columnName) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    //
    // Not implemented below here
    //

    public boolean absolute(int row) {
        return false;
    }

    public void afterLast() {
        log.warn("after last check");
    }

    public void beforeFirst() {
        if (this.rowSupplier != null) throw new UnsupportedOperationException("Not implemented yet.");
        if (!rows.isEmpty()) {
            this.index = -1;
        }
    }

    public void cancelRowUpdates() {
    }

    public void clearWarnings() {
        log.info("clearWarnings");
        this.warningsChain = null;
    }

    public void close() {

    }

    public void deleteRow() {

    }

    public int findColumn(String columnName) {
        return 0;
    }

    public Array getArray(int i) {
        return null;
    }

    public Array getArray(String colName) {
        return null;
    }

    public InputStream getAsciiStream(int columnIndex) {
        return null;
    }

    public InputStream getAsciiStream(String columnName) {
        return null;
    }

    public Reader getCharacterStream(int columnIndex) {
        return null;
    }

    public Reader getCharacterStream(String columnName) {
        return null;
    }

    public Clob getClob(int i) {
        return null;
    }

    public Clob getClob(String colName) {
        return null;
    }

    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    public String getCursorName() {
        return null;
    }

    public int getFetchDirection() {
        return ResultSet.FETCH_UNKNOWN;
    }

    public int getFetchSize() {
        return 0;
    }

    public Object getObject(int i, Map<String, Class<?>> map)
        {
        return null;
    }

    public Object getObject(String colName, Map<String, Class<?>> map)
        {
        return null;
    }

    public Ref getRef(int i) {
        return null;
    }

    public Ref getRef(String colName) {
        return null;
    }

    public int getRow() {
        return 0;
    }

    public Statement getStatement() {
        return null;
    }

    public Time getTime(int columnIndex, Calendar cal) {
        return null;
    }

    public Time getTime(String columnName, Calendar cal) {
        return null;
    }

    public int getType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public URL getURL(int columnIndex) {
        return null;
    }

    public URL getURL(String columnName) {
        return null;
    }

    @Deprecated(since="1.2")
    public InputStream getUnicodeStream(int columnIndex) {
        return null;
    }

    @Deprecated(since="1.2")
    public InputStream getUnicodeStream(String columnName) {
        return null;
    }

    public SQLWarning getWarnings() {
        return warningsChain;
    }

    public void addWarning(String reason) {
        log.info("Adding Warning: {}", reason);
        if (warningsChain != null) {
            SQLWarning last = warningsChain;
            while (last != null && last.getNextWarning() != null) {
                last = last.getNextWarning();
            }
            assert last != null;
            last.setNextWarning(new SQLWarning(reason));
        } else {
            warningsChain = new SQLWarning(reason);
        }
    }

    public void insertRow() throws SQLException {
        throw new SQLException("Feature is not supported.", "HY000", 1);
    }

    public void moveToCurrentRow() {
    }

    public void moveToInsertRow() {
    }

    public boolean previous() {

        return false;
    }

    public void refreshRow() {
    }

    public boolean relative(int rows) {

        return false;
    }

    public boolean rowDeleted() {

        return false;
    }

    public boolean rowInserted() {

        return false;
    }

    public boolean rowUpdated() {

        return false;
    }

    public void setFetchDirection(int direction) {
    }

    public void updateArray(int columnIndex, Array x) {
    }

    public void updateArray(String columnName, Array x) {
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length)
        {
    }

    public void updateAsciiStream(String columnName, InputStream x, int length)
        {
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x)
        {
    }

    public void updateBigDecimal(String columnName, BigDecimal x)
        {
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length)
        {
    }

    public void updateBinaryStream(String columnName, InputStream x, int length)
        {
    }

    public void updateBlob(int columnIndex, Blob x) {
    }

    public void updateBlob(String columnName, Blob x) {
    }

    public void updateBoolean(int columnIndex, boolean x) {
    }

    public void updateBoolean(String columnName, boolean x) {
    }

    public void updateByte(int columnIndex, byte x) {
    }

    public void updateByte(String columnName, byte x) {
    }

    public void updateBytes(int columnIndex, byte[] x) {
    }

    public void updateBytes(String columnName, byte[] x) {
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length)
        {
    }

    public void updateCharacterStream(String columnName, Reader reader,
        int length) {
    }

    public void updateClob(int columnIndex, Clob x) {
    }

    public void updateClob(String columnName, Clob x) {
    }

    public void updateDate(int columnIndex, Date x) {
    }

    public void updateDate(String columnName, Date x) {
    }

    public void updateDouble(int columnIndex, double x) {
    }

    public void updateDouble(String columnName, double x) {
    }

    public void updateFloat(int columnIndex, float x) {
    }

    public void updateFloat(String columnName, float x) {
    }

    public void updateInt(int columnIndex, int x) {
    }

    public void updateInt(String columnName, int x) {
    }

    public void updateLong(int columnIndex, long x) {
    }

    public void updateLong(String columnName, long x) {
    }

    public void updateNull(int columnIndex) {
    }

    public void updateNull(String columnName) {
    }

    public void updateObject(int columnIndex, Object x) {
    }

    public void updateObject(String columnName, Object x) {
    }

    public void updateObject(int columnIndex, Object x, int scale)
        {
    }

    public void updateObject(String columnName, Object x, int scale)
        {
    }

    public void updateRef(int columnIndex, Ref x) {
    }

    public void updateRef(String columnName, Ref x) {
    }

    public void updateRow() {
    }

    public void updateShort(int columnIndex, short x) {
    }

    public void updateShort(String columnName, short x) {
    }

    public void updateString(int columnIndex, String x) {
    }

    public void updateString(String columnName, String x) {
    }

    public void updateTime(int columnIndex, Time x) {
    }

    public void updateTime(String columnName, Time x) {
    }

    public void updateTimestamp(int columnIndex, Timestamp x)
        {
    }

    public void updateTimestamp(String columnName, Timestamp x)
        {
    }

    public boolean wasNull() {

        return false;
    }

    public <T> T unwrap(Class<T> iface) {

        return null;
    }

    public boolean isWrapperFor(Class<?> iface) {

        return false;
    }

    public RowId getRowId(int columnIndex) {

        return null;
    }

    public RowId getRowId(String columnLabel) {

        return null;
    }

    public void updateRowId(int columnIndex, RowId x) {
    }

    public void updateRowId(String columnLabel, RowId x) {
    }

    public int getHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    public boolean isClosed() {

        return false;
    }

    public void updateNString(int columnIndex, String nString) {
    }

    public void updateNString(String columnLabel, String nString) {
    }

    public void updateNClob(int columnIndex, NClob nClob) {
    }

    public void updateNClob(String columnLabel, NClob nClob) {
    }

    public NClob getNClob(int columnIndex) {

        return null;
    }

    public NClob getNClob(String columnLabel) {

        return null;
    }

    public SQLXML getSQLXML(int columnIndex) {

        return null;
    }

    public SQLXML getSQLXML(String columnLabel) {

        return null;
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) {
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) {
    }

    public String getNString(int columnIndex) {

        return null;
    }

    public String getNString(String columnLabel) {

        return null;
    }

    public Reader getNCharacterStream(int columnIndex) {

        return null;
    }

    public Reader getNCharacterStream(String columnLabel) {

        return null;
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) {
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) {
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) {
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) {
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) {
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) {
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) {
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) {
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) {
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) {
    }

    public void updateClob(int columnIndex, Reader reader, long length) {
    }

    public void updateClob(String columnLabel, Reader reader, long length) {
    }

    public void updateNClob(int columnIndex, Reader reader, long length) {
    }

    public void updateNClob(String columnLabel, Reader reader, long length) {
    }

    public void updateNCharacterStream(int columnIndex, Reader x) {
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) {
    }

    public void updateAsciiStream(int columnIndex, InputStream x) {
    }

    public void updateBinaryStream(int columnIndex, InputStream x) {
    }

    public void updateCharacterStream(int columnIndex, Reader x) {
    }

    public void updateAsciiStream(String columnLabel, InputStream x) {
    }

    public void updateBinaryStream(String columnLabel, InputStream x) {
    }

    public void updateCharacterStream(String columnLabel, Reader reader) {
    }

    public void updateBlob(int columnIndex, InputStream inputStream) {
    }

    public void updateBlob(String columnLabel, InputStream inputStream) {
    }

    public void updateClob(int columnIndex, Reader reader) {
    }

    public void updateClob(String columnLabel, Reader reader) {
    }

    public void updateNClob(int columnIndex, Reader reader) {
    }

    public void updateNClob(String columnLabel, Reader reader) {
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) {
        return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) {
        return null;
    }

    private String convertToString(Object o) {
        if (o == null) {
            return null;
        } else {
            return o instanceof String s ? s : String.valueOf(o);
        }
    }
}
