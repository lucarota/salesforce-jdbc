package it.rotaliano.jdbc.salesforce.statement;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for converting Java parameter values to their SOQL string representations.
 */
@Slf4j
public final class SoqlParameterConverter {

    public static final String ISO_DATETIME = "yyyy-MM-dd'T'HH:mm:ss'+00:00'";

    private static final Map<Class<?>, Function<Object, String>> PARAM_CONVERTERS = new HashMap<>();

    static {
        PARAM_CONVERTERS.put(String.class, SoqlParameterConverter::toSoqlStringParam);
        PARAM_CONVERTERS.put(Object.class, SoqlParameterConverter::toSoqlStringParam);
        PARAM_CONVERTERS.put(Boolean.class, Object::toString);
        PARAM_CONVERTERS.put(Double.class, Object::toString);
        PARAM_CONVERTERS.put(BigDecimal.class, Object::toString);
        PARAM_CONVERTERS.put(Float.class, Object::toString);
        PARAM_CONVERTERS.put(Integer.class, Object::toString);
        PARAM_CONVERTERS.put(Long.class, Object::toString);
        PARAM_CONVERTERS.put(Short.class, Object::toString);
        PARAM_CONVERTERS.put(java.util.Date.class, new SimpleDateFormat(ISO_DATETIME)::format);
        PARAM_CONVERTERS.put(Timestamp.class, new SimpleDateFormat(ISO_DATETIME)::format);
        PARAM_CONVERTERS.put(null, p -> "NULL");
    }

    private SoqlParameterConverter() {
    }

    static String toSoqlStringParam(Object param) {
        return "'" + param.toString()
            .replaceAll("'", "\\\\'")
            .replaceAll("\\\\", "\\\\\\\\") + "'";
    }

    public static String convertToSoqlParam(Object paramValue) {
        Class<?> paramClass = getParamClass(paramValue);
        if (paramValue instanceof String param && param.startsWith("{ts")) {
            paramValue = convertStringToDate(param);
        }
        return PARAM_CONVERTERS.get(paramClass).apply(paramValue);
    }

    private static java.util.Date convertStringToDate(String paramValue) {
        if (paramValue == null) {
            return null;
        }
        try {
            String paramValueCleared = paramValue.trim()
                    .replaceAll("^\\{ts\\s*'(.*)'}$", "$1")
                    .replaceAll("Z$", "+00:00");
            return new SimpleDateFormat(ISO_DATETIME).parse(paramValueCleared);
        } catch (ParseException e) {
            log.error("Failed to convert value to date [{}]", paramValue, e);
        }
        return null;
    }

    static Class<?> getParamClass(Object paramValue) {
        Class<?> paramClass = paramValue != null ? paramValue.getClass() : null;
        if (!PARAM_CONVERTERS.containsKey(paramClass)) {
            paramClass = Object.class;
        }
        return paramClass;
    }
}
