package it.rotaliano.jdbc.salesforce.resultset;

import it.rotaliano.jdbc.salesforce.metadata.ColumnMap;
import it.rotaliano.jdbc.salesforce.metadata.TypeInfo;
import it.rotaliano.jdbc.salesforce.exceptions.SalesforceRuntimeException;
import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.serial.SerialBlob;
import lombok.extern.slf4j.Slf4j;

/**
 * In-memory implementation of {@link ResultSet} that caches all rows.
 *
 * <p><b>Thread Safety:</b> This class is <b>NOT thread-safe</b>, consistent with
 * the JDBC specification which does not require ResultSet implementations to be
 * thread-safe. If multiple threads need to access the same ResultSet, external
 * synchronization must be provided by the application.
 *
 * <p>Specifically, the cursor position (row index) is shared mutable state that
 * is not synchronized. Concurrent calls to navigation methods ({@code next()},
 * {@code first()}, etc.) or data retrieval methods may result in undefined behavior.
 *
 * @see java.sql.ResultSet
 */
@Slf4j
public class CachedResultSet extends AbstractResultSet implements Serializable {

    public static final CachedResultSet EMPTY = new EmptyCachedResultSet();

    private static class EmptyCachedResultSet extends CachedResultSet {
        @Serial
        private static final long serialVersionUID = 1L;

        EmptyCachedResultSet() {
            super(Collections.emptyList(), new RowSetMetaDataImpl());
        }

        @Override
        public boolean next() {
            return false;
        }

        @Override
        protected void addRow(ColumnMap<String, Object> row) {
            throw new UnsupportedOperationException("EMPTY ResultSet is immutable");
        }

        @Override
        public void addWarning(String reason) {
            throw new UnsupportedOperationException("EMPTY ResultSet is immutable");
        }

        @Override
        public void clearWarnings() {
            // no-op: immutable empty has no warnings to clear
        }
    }

    @Serial
    private static final long serialVersionUID = 1L;

    // Thread-safe DateTimeFormatters (immutable)
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSSX");

    private transient Integer index;
    private List<ColumnMap<String, Object>> rows;
    private ResultSetMetaData metadata;
    private SQLWarning warningsChain;
    private boolean wasNull;

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
        if (this.rowSupplier != null) {
            throw new UnsupportedOperationException("Not implemented yet.");
        }
        if (!rows.isEmpty()) {
            this.index = 0;
            return true;
        } else {
            return false;
        }
    }

    public boolean last() {
        if (this.rowSupplier != null) {
            throw new UnsupportedOperationException("Not implemented yet.");
        }
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
            if (index < rows.size()) {
                return true;
            }
        }
        return nextSupplied() && next();
    }

    public boolean isAfterLast() {
        if (this.rowSupplier != null) {
            throw new UnsupportedOperationException("Not implemented yet.");
        }
        return !rows.isEmpty() && index == rows.size();
    }

    public boolean isBeforeFirst() {
        if (this.rowSupplier != null) {
            throw new UnsupportedOperationException("Not implemented yet.");
        }
        return !rows.isEmpty() && index == -1;
    }

    public boolean isFirst() {
        if (this.rowSupplier != null) {
            throw new UnsupportedOperationException("Not implemented yet.");
        }
        return !rows.isEmpty() && index == 0;
    }

    public boolean isLast() {
        if (this.rowSupplier != null) {
            throw new UnsupportedOperationException("Not implemented yet.");
        }
        return !rows.isEmpty() && index == rows.size() - 1;
    }

    public ResultSetMetaData getMetaData() {
        return metadata != null ? metadata : CachedResultSetMetaData.EMPTY;
    }

    /**
     * Static utility for parsing column values with type conversion.
     * Converted from instance inner class to static to eliminate per-call object allocation.
     */
    private static class ColumnValueParser {

        @SuppressWarnings("unchecked")
        static <T> Optional<T> parse(Object value, Function<String, T> conversion, Class<T> clazz) {
            if (value == null) {
                return Optional.empty();
            }

            if (value instanceof String val) {
                return Optional.ofNullable(conversion.apply(val));
            }

            if (value.getClass().isAssignableFrom(clazz)) {
                return (Optional<T>) Optional.of(value);
            }

            if (Boolean.class.isAssignableFrom(clazz)) {
                if (value instanceof Number num) {
                    return (Optional<T>) Optional.of(num.intValue() == 1);
                }
            }

            return Optional.ofNullable(conversion.apply(value.toString()));
        }
    }

    private <T> Optional<T> parseColumn(int columnIndex, Function<String, T> conversion, Class<T> clazz) {
        Object value = getObject(columnIndex);
        wasNull = (value == null);
        return ColumnValueParser.parse(value, conversion, clazz);
    }

    private <T> Optional<T> parseColumn(String columnName, Function<String, T> conversion, Class<T> clazz) {
        Object value = getObject(columnName);
        wasNull = (value == null);
        return ColumnValueParser.parse(value, conversion, clazz);
    }

    public String getString(String columnName) {
        return convertToString(getObject(columnName));
    }

    public String getString(int columnIndex) {
        return convertToString(getObject(columnIndex));
    }

    public BigDecimal getBigDecimal(int columnIndex) {
        return parseColumn(columnIndex, BigDecimal::new, BigDecimal.class)
            .orElse(null);
    }

    public BigDecimal getBigDecimal(String columnName) {
        return parseColumn(columnName, BigDecimal::new, BigDecimal.class)
            .orElse(null);
    }

    protected java.util.Date parseDate(String dateRepr) {
        if (dateRepr == null) {
            wasNull = true;
            return null;
        }
        try {
            wasNull = false;
            LocalDate localDate = LocalDate.parse(dateRepr, DATE_FORMATTER);
            return java.util.Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    public Date getDate(int columnIndex) {
        return parseColumn(columnIndex, this::parseDate, java.util.Date.class)
            .map(d -> new java.sql.Date(d.getTime()))
            .orElse(null);
    }

    public Date getDate(String columnName) {
        return parseColumn(columnName, this::parseDate, java.util.Date.class)
            .map(d -> new java.sql.Date(d.getTime()))
            .orElse(null);
    }

    private java.util.Date parseDateTime(String dateRepr) {
        if (dateRepr == null) {
            wasNull = true;
            return null;
        }
        try {
            wasNull = false;
            OffsetDateTime odt = OffsetDateTime.parse(dateRepr, DATETIME_FORMATTER);
            return java.util.Date.from(odt.toInstant());
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    public Timestamp getTimestamp(int columnIndex) {
        Object value = rows.get(index).getByIndex(columnIndex);
        wasNull = (value == null);
        if (value instanceof GregorianCalendar calendar) {
            return new java.sql.Timestamp(calendar.getTime().getTime());
        } else if (rows.get(index).getTypeInfoByIndex(columnIndex) == TypeInfo.DATE_TYPE_INFO) {
            return ColumnValueParser.parse(value, this::parseDate, java.util.Date.class)
                .map(d -> new java.sql.Timestamp(d.getTime()))
                .orElse(null);
        } else {
            return ColumnValueParser.parse(value, this::parseDateTime, java.util.Date.class)
                .map(d -> new java.sql.Timestamp(d.getTime()))
                .orElse(null);
        }
    }

    public Timestamp getTimestamp(String columnName) {
        Object value = rows.get(index).get(columnName);
        wasNull = (value == null);
        if (value instanceof GregorianCalendar cal) {
            return new java.sql.Timestamp(cal.getTime().getTime());
        } else if (rows.get(index).getTypeInfo(columnName) == TypeInfo.DATE_TYPE_INFO) {
            return ColumnValueParser.parse(value, this::parseDate, java.util.Date.class)
                .map(d -> new java.sql.Timestamp(d.getTime()))
                .orElse(null);
        } else {
            return ColumnValueParser.parse(value, this::parseDateTime, java.util.Date.class)
                .map(d -> new java.sql.Timestamp(d.getTime()))
                .orElse(null);
        }
    }

    private java.util.Date parseTime(String dateRepr) {
        if (dateRepr == null) {
            wasNull = true;
            return null;
        }
        try {
            wasNull = false;
            java.time.OffsetTime offsetTime = java.time.OffsetTime.parse(dateRepr, TIME_FORMATTER);
            LocalTime localTime = offsetTime.toLocalTime();
            return java.util.Date.from(localTime.atDate(LocalDate.EPOCH).atZone(ZoneId.systemDefault()).toInstant());
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    public Time getTime(String columnName) {
        return parseColumn(columnName, this::parseTime, java.util.Date.class)
            .map(d -> new Time(d.getTime()))
            .orElse(null);
    }

    public Time getTime(int columnIndex) {
        return parseColumn(columnIndex, this::parseTime, java.util.Date.class)
            .map(d -> new Time(d.getTime()))
            .orElse(null);
    }

    @Deprecated(since = "1.2")
    public BigDecimal getBigDecimal(int columnIndex, int scale) {
        return parseColumn(columnIndex, BigDecimal::new, BigDecimal.class)
            .map(bigDecimal -> bigDecimal.setScale(scale, RoundingMode.HALF_EVEN))
            .orElse(null);
    }

    @Deprecated(since = "1.2")
    public BigDecimal getBigDecimal(String columnName, int scale) {
        return parseColumn(columnName, BigDecimal::new, BigDecimal.class)
            .map(bigDecimal -> bigDecimal.setScale(scale, RoundingMode.HALF_EVEN))
            .orElse(null);
    }

    public float getFloat(int columnIndex) {
        return parseColumn(columnIndex, Float::parseFloat, Float.class)
            .orElse(0f);
    }

    public float getFloat(String columnName) {
        return parseColumn(columnName, Float::parseFloat, Float.class)
            .orElse(0f);
    }

    public double getDouble(int columnIndex) {
        return parseColumn(columnIndex, Double::parseDouble, Double.class)
            .orElse(0d);
    }

    public double getDouble(String columnName) {
        return parseColumn(columnName, Double::parseDouble, Double.class)
            .orElse(0d);
    }

    public long getLong(String columnName) {
        return parseColumn(columnName, Long::parseLong, Long.class)
            .orElse(0L);
    }

    public long getLong(int columnIndex) {
        return parseColumn(columnIndex, Long::parseLong, Long.class)
            .orElse(0L);
    }

    public int getInt(String columnName) {
        return parseColumn(columnName, Double::parseDouble, Double.class)
            .orElse(0d)
            .intValue();
    }

    public int getInt(int columnIndex) {
        return parseColumn(columnIndex, Double::parseDouble, Double.class)
            .orElse(0d)
            .intValue();
    }

    public short getShort(String columnName) {
        return parseColumn(columnName, Double::parseDouble, Double.class)
            .orElse(0d)
            .shortValue();
    }

    public short getShort(int columnIndex) {
        return parseColumn(columnIndex, Double::parseDouble, Double.class)
            .orElse(0d)
            .shortValue();
    }

    private Blob createBlob(byte[] data) {
        try {
            return new SerialBlob(data);
        } catch (SQLException e) {
            throw new SalesforceRuntimeException(
                    "Failed to create Blob from data", e);
        }
    }

    public Blob getBlob(int columnIndex) {
        return parseColumn(columnIndex, (v) -> Base64.getDecoder().decode(v), byte[].class)
            .map(this::createBlob)
            .orElse(null);
    }

    public Blob getBlob(String columnName) {
        return parseColumn(columnName, (v) -> Base64.getDecoder().decode(v), byte[].class)
            .map(this::createBlob)
            .orElse(null);
    }

    public boolean getBoolean(int columnIndex) {
        return parseColumn(columnIndex, Boolean::parseBoolean, Boolean.class)
            .orElse(false);
    }

    public boolean getBoolean(String columnName) {
        return parseColumn(columnName, Boolean::parseBoolean, Boolean.class)
            .orElse(false);
    }

    public byte getByte(int columnIndex) {
        return parseColumn(columnIndex, Byte::parseByte, Byte.class)
            .orElse((byte) 0);
    }

    public byte getByte(String columnName) {
        return parseColumn(columnName, Byte::parseByte, Byte.class)
            .orElse((byte) 0);
    }

    public byte[] getBytes(int columnIndex) {
        final String s = convertToString(getObject(columnIndex));
        if (s != null) {
            return s.getBytes();
        }
        return new byte[0];
    }

    public byte[] getBytes(String columnName) {
        final String s = convertToString(getObject(columnName));
        if (s != null) {
            return s.getBytes();
        }
        return new byte[0];
    }

    public void beforeFirst() {
        if (this.rowSupplier != null) {
            throw new UnsupportedOperationException("Not implemented yet.");
        }
        if (!rows.isEmpty()) {
            this.index = -1;
        }
    }

    public void clearWarnings() {
        log.trace("clearWarnings");
        this.warningsChain = null;
    }

    public int getType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    public int getFetchDirection() {
        return ResultSet.FETCH_UNKNOWN;
    }

    public SQLWarning getWarnings() {
        return warningsChain;
    }

    public void addWarning(String reason) {
        log.debug("Adding Warning: {}", reason);
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

    public boolean wasNull() {
        return wasNull;
    }

    public int getHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public void afterLast() {
        log.trace("after last check");
    }

    private String convertToString(Object o) {
        if (o == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return o instanceof String s ? s : String.valueOf(o);
        }
    }
}
