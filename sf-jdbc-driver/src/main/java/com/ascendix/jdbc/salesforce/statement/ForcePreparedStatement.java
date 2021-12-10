package com.ascendix.jdbc.salesforce.statement;

import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.resultset.CachedResultSet;
import com.ascendix.jdbc.salesforce.connection.ForceConnection;
import com.ascendix.jdbc.salesforce.delegates.ForceResultField;
import com.ascendix.jdbc.salesforce.metadata.ColumnMap;
import com.ascendix.jdbc.salesforce.metadata.ForceDatabaseMetaData;
import com.ascendix.jdbc.salesforce.statement.processor.*;
import com.sforce.ws.ConnectionException;
import org.apache.commons.lang3.StringUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mule.tools.soql.exception.SOQLParsingException;

import javax.sql.rowset.RowSetMetaDataImpl;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ForcePreparedStatement implements PreparedStatement {

    private static final String SF_JDBC_DRIVER_NAME = "SF JDBC driver";
    private static final Logger logger = Logger.getLogger(SF_JDBC_DRIVER_NAME);

    private final static String CACHE_HINT = "(?is)\\A\\s*(CACHE\\s*(GLOBAL|SESSION)).*";
    private final static int GB = 1073741824;
    private static final String SOSL_QUERY_RESULT = "SOSL_QUERY_RESULT";

    protected enum CacheMode {
        NO_CACHE, GLOBAL, SESSION
    }

    private String soqlQueryOriginal;
    private String soqlQuery;
    private ForceConnection connection;
    private PartnerService partnerService;
    private ResultSetMetaData metadata;
    private int fetchSize;
    private int maxRows;
    private List<Object> parameters = new ArrayList<>();
    private CacheMode cacheMode;
    private static DB cacheDb = DBMaker.tempFileDB().closeOnJvmShutdown().make();
    private int updateCount = -1;
    private boolean updateCountReturned = false;
    private ResultSet resultSet;
    private boolean resultSetReturned = false;
    private SQLWarning warnings = new SQLWarning();

    // TODO: Join caches and move it to ForceConnection class. Divide to session
    // and static global cache.
    private static HTreeMap<String, ResultSet> dataCache = cacheDb
            .hashMap("DataCache", Serializer.STRING, Serializer.ELSA)
            .expireAfterCreate(60, TimeUnit.MINUTES)
            .expireStoreSize(16 * GB)
            .create();
    private static HTreeMap<String, ResultSetMetaData> metadataCache = cacheDb
            .hashMap("MetadataCache", Serializer.STRING, Serializer.ELSA)
            .expireAfterCreate(60, TimeUnit.MINUTES)
            .expireStoreSize(1 * GB)
            .create();

    public ForcePreparedStatement(ForceConnection connection) {
        logger.info("[PrepStat] constructor conn IMPLEMENTED ");
        this.connection = connection;
    }

    public ForcePreparedStatement(ForceConnection connection, String soql) {
        logger.info("[PrepStat] constructor soql IMPLEMENTED "+soql);
        this.connection = connection;
        this.cacheMode = getCacheMode(soql);
        this.soqlQuery = removeCacheHints(soql);
    }

    public static <T extends Throwable> RuntimeException rethrowAsNonChecked(Throwable throwable) throws T {
        throw (T) throwable; // rely on vacuous cast
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        logger.info("[PrepStat] executeQuery IMPLEMENTED "+soqlQuery);
        this.updateCount = -1;
        this.updateCountReturned = false;
        this.resultSetReturned = false;
        this.resultSet = null;

        return cacheMode == CacheMode.NO_CACHE
                ? query()
                : dataCache.computeIfAbsent(getCacheKey(), s -> {
            try {
                return query();
            } catch (SQLException e) {
                rethrowAsNonChecked(e);
                return null;
            }
        });
    }

    private ResultSet query() throws SQLException {
        logger.info("[PrepStat] query IMPLEMENTED "+soqlQuery);
        if ("SELECT 'keep alive'".equals(soqlQuery)) {
            logger.info("[PrepStat] query KEEP ALIVE ");
            return new CachedResultSet(Collections.emptyList(), new RowSetMetaDataImpl());
        }
        prepareQuery();

        if (AdminQueryProcessor.isAdminQuery(soqlQuery)) {
            try {
                return AdminQueryProcessor.processQuery(this, soqlQuery, getPartnerService());
            } catch (ConnectionException | SOQLParsingException e) {
                throw new SQLException(e);
            }
        }
        InsertQueryAnalyzer insertQueryAnalyzer = getInsertQueryAnalyzer();
        if (InsertQueryProcessor.isInsertQuery(soqlQuery, insertQueryAnalyzer)) {
            try {
                return InsertQueryProcessor.processQuery(this, soqlQuery, getPartnerService(), insertQueryAnalyzer);
            } catch (ConnectionException | SOQLParsingException e) {
                throw new SQLException(e);
            }
        }
        UpdateQueryAnalyzer updateQueryAnalyzer = getUpdateQueryAnalyzer();
        if (UpdateQueryProcessor.isUpdateQuery(soqlQuery, updateQueryAnalyzer)) {
            try {
                return UpdateQueryProcessor.processQuery(this, soqlQuery, getPartnerService(), updateQueryAnalyzer);
            } catch (ConnectionException | SOQLParsingException e) {
                throw new SQLException(e);
            }
        }
        DeleteQueryAnalyzer deleteQueryAnalyzer = getDeleteQueryAnalyzer();
        if (DeleteQueryProcessor.isDeleteQuery(soqlQuery, deleteQueryAnalyzer)) {
            try {
                return DeleteQueryProcessor.processQuery(this, soqlQuery, getPartnerService(), deleteQueryAnalyzer);
            } catch (ConnectionException | SOQLParsingException e) {
                throw new SQLException(e);
            }
        }
        SoslQueryAnalyzer soslQueryAnalyzer = getSoslQueryAnalyzer();
        if (SoslQueryProcessor.isSoslQuery(soqlQuery, soslQueryAnalyzer)) {
            try {
                return SoslQueryProcessor.processQuery(this, soqlQuery, getPartnerService(), soslQueryAnalyzer);
            } catch (ConnectionException | SOQLParsingException e) {
                throw new SQLException(e);
            }
        }
        try {
            List<FieldDef> fieldDefinitions = getRootEntityFieldDefinitions();
            List<List> forceQueryResult = getPartnerService().query(soqlQuery, fieldDefinitions);
            if (!forceQueryResult.isEmpty()) {
                List<ColumnMap<String, Object>> maps = Collections.synchronizedList(new LinkedList<>());
                forceQueryResult.forEach(record -> maps.add(convertToColumnMap(record)));
                return new CachedResultSet(maps, getMetaData());
            } else {
                return new CachedResultSet(Collections.emptyList(), getMetaData());
            }
        } catch (ConnectionException | SOQLParsingException e) {
            throw new SQLException(e);
        }
    }

    private String prepareQuery() {
        logger.info("[PrepStat] prepareQuery IMPLEMENTED "+soqlQuery);
        soqlQueryOriginal = soqlQuery;
        soqlQuery = preprocessQuery(soqlQuery);
        return setParams(soqlQuery);
    }

    private String preprocessQuery(String soqlQuery) {
        // Preprocess sume sugar like Date Literals into SOQL format
        // {ts '2021-10-12 00:00:12Z'}
        // 2021-10-12T00:00:12Z
        if (soqlQuery == null) {
            return soqlQuery;
        }
        String soqlQueryProcessed = soqlQuery;
        if (soqlQuery.contains("{ts")) {
            soqlQueryProcessed = soqlQuery.trim().replaceAll("\\{ts\\s*'(\\d\\d\\d\\d-\\d\\d-\\d\\d)(T|\\s)(\\d\\d:\\d\\d:\\d\\d)(Z|[+-]\\d\\d\\d\\d|)'\\}", "$1T$3$4");
        }
        return soqlQueryProcessed;
    }

    private ColumnMap<String, Object> convertToColumnMap(List<ForceResultField> recordFields) {
        ColumnMap<String, Object> columnMap = new ColumnMap<>();
        // Flatten not only child records, but also Lists of Lists - for Relations - list of subrecords
        recordFields = flatten(recordFields);
        recordFields.stream()
                .map(field -> field == null ? new ForceResultField(null, null, null, null) : field)
                .forEach(field -> {
                    columnMap.put(field.getFullName(), field.getValue());
                });
        return columnMap;
    }

    protected String removeCacheHints(String query) {
        Matcher matcher = Pattern.compile(CACHE_HINT).matcher(query);
        if (matcher.matches()) {
            String hint = matcher.group(1);
            return query.replaceFirst(hint, "");
        } else {
            return query;
        }
    }

    protected CacheMode getCacheMode(String query) {
        Matcher matcher = Pattern.compile(CACHE_HINT).matcher(query);
        if (matcher.matches()) {
            String mode = matcher.group(2);
            return CacheMode.valueOf(mode.toUpperCase());
        } else {
            return CacheMode.NO_CACHE;
        }
    }

    private String getCacheKey() {
        String preparedQuery = prepareQuery();
        return cacheMode == CacheMode.GLOBAL
                ? preparedQuery
                : connection.getPartnerConnection().getSessionHeader().getSessionId() + preparedQuery;
    }

    public List<Object> getParameters() {
        logger.info("[PrepStat] getParameters IMPLEMENTED "+soqlQuery);
        int paramsCountInQuery = StringUtils.countMatches(soqlQuery, '?');
        logger.info("[PrepStat] getParameters   detected "+paramsCountInQuery+" parameters");
        logger.info("[PrepStat] getParameters   parameters provided "+parameters.size());
        if (parameters.size() < paramsCountInQuery) {
            parameters.addAll(Collections.nCopies(paramsCountInQuery - parameters.size(), null));
        }
        return parameters;
    }

    protected String setParams(String soql) {
        logger.info("[PrepStat] setParams IMPLEMENTED "+soql);
        String result = soql;
        for (Object param : getParameters()) {
            String paramRepresentation = convertToSoqlParam(param);
            result = result.replaceFirst("\\?", paramRepresentation);
        }
        return result;
    }

    private final static DateFormat SF_DATETIME_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
    private static Map<Class<?>, Function<Object, String>> paramConverters = new HashMap<>();

    static {
        paramConverters.put(String.class, ForcePreparedStatement::toSoqlStringParam);
        paramConverters.put(Object.class, ForcePreparedStatement::toSoqlStringParam);
        paramConverters.put(Boolean.class, Object::toString);
        paramConverters.put(Double.class, Object::toString);
        paramConverters.put(BigDecimal.class, Object::toString);
        paramConverters.put(Float.class, Object::toString);
        paramConverters.put(Integer.class, Object::toString);
        paramConverters.put(Long.class, Object::toString);
        paramConverters.put(Short.class, Object::toString);
        paramConverters.put(java.util.Date.class, SF_DATETIME_FORMATTER::format);
        paramConverters.put(Timestamp.class, SF_DATETIME_FORMATTER::format);
        paramConverters.put(null, p -> "NULL");
    }

    protected static String toSoqlStringParam(Object param) {
        return "'" + param.toString().replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'";
    }

    protected static String convertToSoqlParam(Object paramValue) {
        Class<?> paramClass = getParamClass(paramValue);
        // Convert the string date representation to SOQL
        // like {ts '2021-10-21 12:01:02Z'}
        // to 2021-10-21 12:01:02-0000
        if (java.lang.String.class.equals(paramClass) && ((String) paramValue).startsWith("{ts")) {
            paramValue = convertStringToDate((String)paramValue);
            paramClass = getParamClass(paramValue);
        }
        return paramConverters.get(paramClass).apply(paramValue);
    }

    private static java.util.Date convertStringToDate(String paramValue) {
        if (paramValue == null) {
            return null;
        }
        try {
            String paramValueCleared = paramValue.trim().replaceAll("^\\{ts\\s*'(.*)'\\}$", "$1").replaceAll("Z$", "+0000");
            java.util.Date parsed = SF_DATETIME_FORMATTER.parse(paramValueCleared);
            return parsed;
        } catch (ParseException e) {
            logger.log(Level.SEVERE, "Failed to convert value to date ["+paramValue+"]", e);
        }
        return null;
    }

    protected static Class<?> getParamClass(Object paramValue) {
        Class<?> paramClass = paramValue != null ? paramValue.getClass() : null;
        if (!paramConverters.containsKey(paramClass)) {
            paramClass = Object.class;
        }
        return paramClass;
    }

    public static ResultSetMetaData dummyMetaData(ColumnMap<String, Object> row) {
        if (row == null) {
            return null;
        }
        try {
            logger.info("[PrepStat] dummyMetaData IMPLEMENTED ");
            RowSetMetaDataImpl result = new RowSetMetaDataImpl();
            int columnsCount = row.size();
            result.setColumnCount(columnsCount);
            for (int i = 1; i <= columnsCount; i++) {
                String fieldName = row.getColumnNames().get(i-1);
                result.setAutoIncrement(i, false);
                result.setColumnName(i, fieldName);
                result.setColumnLabel(i, fieldName);
                Object value = row.getValues().get(i-1);
                String javaTypeName = value == null ? "string" : value.getClass().getName();
                ForceDatabaseMetaData.TypeInfo typeInfo = ForceDatabaseMetaData.lookupTypeInfoFromJavaType(javaTypeName);
                logger.info("[PrepStat] dummyMetaData ("+i+") "+fieldName+" : "+javaTypeName+" => "+typeInfo.sqlDataType);
                result.setColumnType(i, typeInfo.sqlDataType);
                result.setColumnTypeName(i, typeInfo.typeName);
                result.setPrecision(i, typeInfo.precision);
                result.setSchemaName(i, ForceDatabaseMetaData.DEFAULT_SCHEMA);
                result.setCatalogName(i, ForceDatabaseMetaData.DEFAULT_CATALOG);
                result.setTableName(i, null);
                result.setCaseSensitive(i, false);
            }
            return result;
        } catch (Exception e) {
            // Ignore for metadata - just return empty
            logger.log(Level.WARNING, "Failed to compile dummy metadata information", e);
            return null;
        }
    }

    private ResultSetMetaData loadMetaData() throws SQLException {
        try {
            logger.info("[PrepStat] loadMetaData IMPLEMENTED "+soqlQuery);
            if (metadata == null) {
                RowSetMetaDataImpl result = new RowSetMetaDataImpl();
                SoqlQueryAnalyzer queryAnalyzer = getSoqlQueryAnalyzer();
                List<FieldDef> resultFieldDefinitions = flatten(getRootEntityFieldDefinitions());
                int columnsCount = resultFieldDefinitions.size();
                result.setColumnCount(columnsCount);
                for (int i = 1; i <= columnsCount; i++) {
                    FieldDef field = resultFieldDefinitions.get(i - 1);
                    result.setAutoIncrement(i, false);
                    result.setColumnName(i, field.getName());
                    result.setColumnLabel(i, field.getAlias());
                    String forceTypeName = field.getType();
                    ForceDatabaseMetaData.TypeInfo typeInfo = ForceDatabaseMetaData.lookupTypeInfo(forceTypeName);
                    result.setColumnType(i, typeInfo.sqlDataType);
                    result.setColumnTypeName(i, typeInfo.typeName);
                    result.setPrecision(i, typeInfo.precision);
                    result.setSchemaName(i, ForceDatabaseMetaData.DEFAULT_SCHEMA);
                    result.setCatalogName(i, ForceDatabaseMetaData.DEFAULT_CATALOG);
                    result.setTableName(i, queryAnalyzer.getFromObjectName());
//                    result.setTableName(i, SOSL_QUERY_RESULT);
                    result.setCaseSensitive(i, false);
                }
                metadata = result;
            }
            return metadata;
        } catch (RuntimeException e) {
            throw new SQLException(e.getCause() != null ? e.getCause() : e);
        }
    }

    private <T> List<T> flatten(List<T> listWithLists) {
        logger.info("[PrepStat] flatten IMPLEMENTED "+soqlQuery);
        List<T> listPlain =  (List<T>) listWithLists.stream()
                .flatMap(
                        def -> def instanceof Collection
                        ? flatten((List<T>) def).stream() // MultiLevel flattening
                        : Stream.of(def)
                )
                .collect(Collectors.toList());
        return listPlain;
    }

    private List<FieldDef> fieldDefinitions;

    private List<FieldDef> getRootEntityFieldDefinitions() {
        logger.info("[PrepStat] getFieldDefinitions IMPLEMENTED "+soqlQuery);
        if (fieldDefinitions == null) {
            fieldDefinitions = getSoqlQueryAnalyzer().getFieldDefinitions();
            fieldDefinitions = flatten(fieldDefinitions);
            logger.info("[PrepStat] getFieldDefinitions:\n  "+
                    fieldDefinitions.stream().map( fd -> fd.getName()+":"+fd.getType()).collect(Collectors.joining("\n  ")));
        }
        return fieldDefinitions;
    }

    private SoslQueryAnalyzer soslQueryAnalyzer;
    private SoqlQueryAnalyzer soqlQueryAnalyzer;
    private InsertQueryAnalyzer insertQueryAnalyzer;
    private UpdateQueryAnalyzer updateQueryAnalyzer;
    private DeleteQueryAnalyzer deleteQueryAnalyzer;

    private SoqlQueryAnalyzer getSoqlQueryAnalyzer() {
        logger.info("[PrepStat] getSoqlQueryAnalyzer IMPLEMENTED "+soqlQuery);
        if (soqlQueryAnalyzer == null) {
            soqlQueryAnalyzer = new SoqlQueryAnalyzer(soqlQuery, (objName) -> {
                try {
                    return getPartnerService().describeSObject(objName);
                } catch (ConnectionException e) {
                    throw new RuntimeException(e);
                }
            }, connection.getCache());
            if (soqlQueryAnalyzer.isExpandedStarSyntaxForFields()) {
                this.soqlQuery = soqlQueryAnalyzer.getSoqlQuery();
                logger.info("[PrepStat] Expanded Star Syntax to "+soqlQuery);
            }
        }
        return soqlQueryAnalyzer;
    }

    private SoslQueryAnalyzer getSoslQueryAnalyzer() {
        logger.info("[PrepStat] getSoslQueryAnalyzer IMPLEMENTED "+soqlQuery);
        if (soslQueryAnalyzer == null) {
            soslQueryAnalyzer = new SoslQueryAnalyzer(soqlQuery, (objName) -> {
                try {
                    return getPartnerService().describeSObject(objName);
                } catch (ConnectionException e) {
                    throw new RuntimeException(e);
                }
            }, connection.getCache());
        }
        return soslQueryAnalyzer;
    }

    private InsertQueryAnalyzer getInsertQueryAnalyzer() {
        logger.info("[PrepStat] getInsertQueryAnalyzer IMPLEMENTED "+soqlQuery);
        if (insertQueryAnalyzer == null) {
            insertQueryAnalyzer = new InsertQueryAnalyzer(soqlQuery, (objName) -> {
                try {
                    return getPartnerService().describeSObject(objName);
                } catch (ConnectionException e) {
                    throw new RuntimeException(e);
                }
            }, connection.getCache(),
                    this::runResolveSubselect);
        }
        return insertQueryAnalyzer;
    }

    private UpdateQueryAnalyzer getUpdateQueryAnalyzer() {
        logger.info("[PrepStat] getUpdateQueryAnalyzer IMPLEMENTED "+soqlQuery);
        if (updateQueryAnalyzer == null) {
            updateQueryAnalyzer = new UpdateQueryAnalyzer(soqlQuery, (objName) -> {
                try {
                    return getPartnerService().describeSObject(objName);
                } catch (ConnectionException e) {
                    throw new RuntimeException(e);
                }
            }, connection.getCache(),
                    this::runResolveSubselect);
        }
        return updateQueryAnalyzer;
    }

    private DeleteQueryAnalyzer getDeleteQueryAnalyzer() {
        logger.info("[PrepStat] getDeleteQueryAnalyzer IMPLEMENTED "+soqlQuery);
        if (deleteQueryAnalyzer == null) {
            deleteQueryAnalyzer = new DeleteQueryAnalyzer(soqlQuery,
                    this::runResolveSubselect);
        }
        return deleteQueryAnalyzer;
    }

    private List<Map<String, Object>> runResolveSubselect(String soql) {
        List<Map<String, Object>> results = new ArrayList<>();
        logger.info("Resolving subselect \n"+soql);
        try {
            ForcePreparedStatement forcePreparedStatement = new ForcePreparedStatement(connection, soql);
            ResultSet resultSet = forcePreparedStatement.query();

            while(resultSet.next()) {
                // LinkedHashMap is needed to save the order of the fields
                Map<String, Object> record = new LinkedHashMap<>();
                results.add(record);
                for(int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
                     record.put(resultSet.getMetaData().getColumnName(i+1), resultSet.getString(i+1));
                }
            }
            logger.info("  "+results.size()+" records resolved with "+resultSet.getMetaData().getColumnCount()+" columns");
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to resolve sub-select \n"+soql, e);
            this.warnings.addSuppressed(new SQLWarning("Failed to resolve sub-select \n"+soql, e));
            throw new java.lang.IllegalArgumentException("Failed to resolve sub-select: "+e.getMessage());
        }

        return results;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return new ParameterMetadataImpl(parameters, soqlQuery);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        logger.info("[PrepStat] getMetaData IMPLEMENTED "+soqlQuery);
        return cacheMode == CacheMode.NO_CACHE
                ? loadMetaData()
                : metadataCache.computeIfAbsent(getCacheKey(), s -> {
            try {
                return loadMetaData();
            } catch (SQLException e) {
                rethrowAsNonChecked(e);
                return null;
            }
        });
    }

    private PartnerService getPartnerService() throws ConnectionException {
        logger.info("[PrepStat] getPartnerService IMPLEMENTED "+soqlQuery);
        if (partnerService == null) {
            logger.info("[PrepStat] getPartnerService creating service ");
            partnerService = new PartnerService(connection.getPartnerConnection());
        }
        return partnerService;
    }

    public boolean reconnect(String url, String userName, String userPass) throws ConnectionException {
        logger.info("[PrepStat] RECONNECT IMPLEMENTED newUserName="+userName + " url="+url);
        boolean updated = connection.updatePartnerConnection(url, userName, userPass);
        partnerService = null;
        return updated;
    }

    public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    public void setMaxRows(int max) throws SQLException {
        this.maxRows = max;
    }

    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    public void setArray(int i, Array x) throws SQLException {
        logger.info("[PrepStat] setArray NOT_IMPLEMENTED "+soqlQuery);
        throw new UnsupportedOperationException("The setArray is not implemented yet.");
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        logger.info("[PrepStat] setAsciiStream NOT_IMPLEMENTED "+soqlQuery);
        throw new UnsupportedOperationException("The setAsciiStream is not implemented yet.");
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException {
        addParameter(parameterIndex, x);
    }

    protected void addParameter(int parameterIndex, Object x) {
        logger.info("[PrepStat] addParameter "+parameterIndex+" IMPLEMENTED "+soqlQuery);
        parameterIndex--;
        if (parameters.size() < parameterIndex) {
            parameters.addAll(Collections.nCopies(parameterIndex - parameters.size(), null));
        }
        parameters.add(parameterIndex, x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        logger.info("[PrepStat] setBinaryStream NOT_IMPLEMENTED "+soqlQuery);
        throw new UnsupportedOperationException("The setBinaryStream is not implemented yet.");
    }

    public void setBlob(int i, Blob x) throws SQLException {
        logger.info("[PrepStat] setBlob NOT_IMPLEMENTED "+soqlQuery);
        throw new UnsupportedOperationException("The setBlob is not implemented yet.");
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        addParameter(parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        logger.info("[PrepStat] setByte NOT_IMPLEMENTED "+soqlQuery);
        throw new UnsupportedOperationException("The setByte is not implemented yet.");
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        logger.info("[PrepStat] setBytes NOT_IMPLEMENTED "+soqlQuery);
        throw new UnsupportedOperationException("The setBytes is not implemented yet.");
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
        logger.info("[PrepStat] setCharacterStream NOT_IMPLEMENTED "+soqlQuery);
        throw new UnsupportedOperationException("The setCharacterStream is not implemented yet.");
    }

    public void setClob(int i, Clob x) throws SQLException {
        logger.info("[PrepStat] setClob NOT_IMPLEMENTED "+soqlQuery);
        throw new UnsupportedOperationException("The setClob is not implemented yet.");
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        addParameter(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException {
        logger.info("[PrepStat] setDate NOT_IMPLEMENTED "+soqlQuery);
        throw new UnsupportedOperationException("The setDate is not implemented yet.");
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        addParameter(parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        addParameter(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        addParameter(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        addParameter(parameterIndex, x);
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        addParameter(parameterIndex, null);
    }

    public void setNull(int paramIndex, int sqlType, String typeName)
            throws SQLException {
        addParameter(paramIndex, null);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        logger.info("[PrepStat] setObject 1 NOT_IMPLEMENTED "+soqlQuery);
        throw new UnsupportedOperationException("The setObject 1 is not implemented yet.");
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException {
        logger.info("[PrepStat] setObject 2 NOT_IMPLEMENTED "+soqlQuery);
        throw new UnsupportedOperationException("The setObject 2 is not implemented yet.");
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType,
                          int scale) throws SQLException {
        logger.info("[PrepStat] setObject 3 NOT_IMPLEMENTED "+soqlQuery);
        throw new UnsupportedOperationException("The setObject 3  is not implemented yet.");
    }

    public void setRef(int i, Ref x) throws SQLException {
        String methodName = this.getClass().getSimpleName() + "." + new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        addParameter(parameterIndex, x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        addParameter(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        String methodName = this.getClass().getSimpleName() + "." + new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    public void setTime(int parameterIndex, Time x, Calendar cal)
            throws SQLException {
        String methodName = this.getClass().getSimpleName() + "." + new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");

    }

    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException {
        addParameter(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        String methodName = this.getClass().getSimpleName() + "." + new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");

    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        String methodName = this.getClass().getSimpleName() + "." + new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");

    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        String methodName = this.getClass().getSimpleName() + "." + new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    // Not required to implement below

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        logger.info("[PrepStat] executeQuery IMPLEMENTED "+sql);
        this.cacheMode = getCacheMode(sql);
        this.soqlQuery = removeCacheHints(sql);
        this.resultSet = executeQuery();
        return this.resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        logger.info("[PrepStat] executeUpdate IMPLEMENTED "+sql);
        this.cacheMode = getCacheMode(sql);
        this.soqlQuery = removeCacheHints(sql);

        this.updateCount = -1;
        this.updateCountReturned = false;
        this.resultSet = executeQuery();
        this.resultSetReturned = false;
        return this.updateCount;
    }

    @Override
    public void close() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void cancel() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        logger.info("[PrepStat] getWarnings IMPLEMENTED ");
        return resultSet != null ? resultSet.getWarnings() : warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        logger.info("[PrepStat] clearWarnings IMPLEMENTED ");
        if(resultSet != null) {
            resultSet.clearWarnings();
        }
        warnings = new SQLWarning();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        logger.info("[PrepStat] execute IMPLEMENTED "+sql);
        this.cacheMode = getCacheMode(sql);
        this.soqlQuery = removeCacheHints(sql);

        this.updateCount = -1;
        this.updateCountReturned = false;
        this.resultSet = executeQuery();
        this.resultSetReturned = false;
        boolean result = this.updateCount < 0;
        logger.info("[PrepStat] execute IMPLEMENTED ("+result+")"+sql);
        return result;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ResultSet toReturn = updateCount < 0 ? resultSet : null;
        if (this.resultSetReturned) {
            logger.info("[PrepStat] getResultSet IMPLEMENTED Already Returned " + soqlQuery + "\n " +
                    (resultSet == null ? " resultSet is NULL" : "resultSet is present") +
                    (toReturn == null ? " -> Not to be returned" : " -> Returning"));
            return null;
        }
        this.resultSetReturned = true;
        logger.info("[PrepStat] getResultSet IMPLEMENTED " + soqlQuery + "\n " +
                (resultSet == null ? " resultSet is NULL" : "resultSet is present") +
                (toReturn == null ? " -> Not to be returned" : " -> Returning"));
        return toReturn;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (this.updateCountReturned) {
            logger.info("[PrepStat] getUpdateCount Already Returned "+updateCount+" IMPLEMENTED "+soqlQuery);
            return -1;
        }
        logger.info("[PrepStat] getUpdateCount "+updateCount+" IMPLEMENTED "+soqlQuery);
        this.updateCountReturned = true;
        return updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (updateCount >=0 ) {
            logger.info("[PrepStat] getMoreResults IMPLEMENTED (false) updateCount="+updateCount+" sql="+soqlQuery);
            return false;
        }
        boolean more = resultSet != null && resultSet.next();
        logger.info("[PrepStat] getMoreResults IMPLEMENTED ("+more+") updateCount="+updateCount+" sql="+soqlQuery);
        return more;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getFetchDirection() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        logger.info("[PrepStat] getResultSetType NOT_IMPLEMENTED "+soqlQuery);
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        logger.info("[PrepStat] addBatch NOT_IMPLEMENTED "+sql);

    }

    @Override
    public void clearBatch() throws SQLException {
        logger.info("[PrepStat] clearBatch NOT_IMPLEMENTED "+soqlQuery);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        logger.info("[PrepStat] executeBatch NOT_IMPLEMENTED "+soqlQuery);
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        logger.info("[PrepStat] getConnection IMPLEMENTED "+soqlQuery);
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        logger.info("[PrepStat] getMoreResults NOT_IMPLEMENTED "+soqlQuery);
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        logger.info("[PrepStat] executeUpdate 1 NOT_IMPLEMENTED "+sql);
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        logger.info("[PrepStat] executeUpdate 2 NOT_IMPLEMENTED "+sql);
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        logger.info("[PrepStat] executeUpdate 3 NOT_IMPLEMENTED "+sql);
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        logger.info("[PrepStat] execute 1 NOT_IMPLEMENTED "+sql);
        return executeUpdate(sql) > 0;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        logger.info("[PrepStat] execute 2 NOT_IMPLEMENTED "+sql);
        return executeUpdate(sql) > 0;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        logger.info("[PrepStat] execute 3 NOT_IMPLEMENTED "+sql);
        return executeUpdate(sql) > 0;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isPoolable() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int executeUpdate() throws SQLException {
        logger.info("[PrepStat] executeUpdate 2 NOT_IMPLEMENTED "+soqlQuery);
        return executeUpdate(soqlQuery);
    }

    @Override
    public void clearParameters() throws SQLException {
        logger.info("[PrepStat] clearParameters 2 NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public boolean execute() throws SQLException {
        logger.info("[PrepStat] execute NOT_IMPLEMENTED "+soqlQuery);
        return executeUpdate(soqlQuery) > 0;
    }

    @Override
    public void addBatch() throws SQLException {
        logger.info("[PrepStat] addBatch NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        // TODO Auto-generated method stub
        logger.info("[PrepStat] setRowId NOT_IMPLEMENTED "+soqlQuery);

    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        logger.info("[PrepStat] setNString NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        logger.info("[PrepStat] setNCharacterStream NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        logger.info("[PrepStat] setNClob NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        logger.info("[PrepStat] setClob NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        logger.info("[PrepStat] setBlob NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        logger.info("[PrepStat] setNClob NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        logger.info("[PrepStat] setSQLXML NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        logger.info("[PrepStat] setAsciiStream NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        logger.info("[PrepStat] setBinaryStream NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        logger.info("[PrepStat] setCharacterStream NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        logger.info("[PrepStat] setAsciiStream NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        logger.info("[PrepStat] setBinaryStream NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        logger.info("[PrepStat] setCharacterStream NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        logger.info("[PrepStat] setNCharacterStream NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        logger.info("[PrepStat] setClob NOT_IMPLEMENTED "+soqlQuery);
        // TODO Auto-generated method stub

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        // TODO Auto-generated method stub
        logger.info("[PrepStat] setBlob NOT_IMPLEMENTED "+soqlQuery);

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        // TODO Auto-generated method stub
        logger.info("[PrepStat] setNClob NOT_IMPLEMENTED "+soqlQuery);

    }

    public void setUpdateCount(int updateCount) {
        this.updateCount = updateCount;
    }

    public void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }
}
