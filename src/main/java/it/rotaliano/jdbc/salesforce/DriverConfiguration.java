package it.rotaliano.jdbc.salesforce;

/**
 * Central configuration for the Salesforce JDBC driver.
 * All values can be overridden via system properties (e.g. {@code -Dsalesforce.jdbc.batchSize=200}).
 */
public class DriverConfiguration {

    /** Property prefix for all driver settings. */
    public static final String PROP_PREFIX = "salesforce.jdbc.";

    // Property key constants
    public static final String PROP_BATCH_SIZE        = PROP_PREFIX + "batchSize";
    public static final String PROP_API_VERSION       = PROP_PREFIX + "apiVersion";
    public static final String PROP_CONNECT_TIMEOUT   = PROP_PREFIX + "connectTimeout";
    public static final String PROP_READ_TIMEOUT      = PROP_PREFIX + "readTimeout";
    public static final String PROP_MAX_RETRIES       = PROP_PREFIX + "maxRetries";
    public static final String PROP_CACHE_SIZE        = PROP_PREFIX + "cacheSize";

    // Default values
    private static final int    DEFAULT_BATCH_SIZE        = 100;
    private static final String DEFAULT_API_VERSION       = "64";
    private static final long   DEFAULT_CONNECT_TIMEOUT   = 10_000L;
    private static final long   DEFAULT_READ_TIMEOUT      = 30_000L;
    private static final int    DEFAULT_MAX_RETRIES       = 5;
    private static final int    DEFAULT_CACHE_SIZE        = 100;

    private DriverConfiguration() {}

    /**
     * Maximum number of records per batch when creating/updating/deleting via Salesforce API.
     */
    public static int getBatchSize() {
        return getInt(PROP_BATCH_SIZE, DEFAULT_BATCH_SIZE);
    }

    /**
     * Salesforce API version used for SOAP connections (e.g. "61").
     */
    public static String getApiVersion() {
        return getString(PROP_API_VERSION, DEFAULT_API_VERSION);
    }

    /**
     * HTTP connect timeout in milliseconds for OAuth and API calls.
     */
    public static long getConnectTimeout() {
        return getLong(PROP_CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
    }

    /**
     * HTTP read timeout in milliseconds for OAuth and API calls.
     */
    public static long getReadTimeout() {
        return getLong(PROP_READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
    }

    /**
     * Maximum number of retry attempts for transient OAuth errors.
     */
    public static int getMaxRetries() {
        return getInt(PROP_MAX_RETRIES, DEFAULT_MAX_RETRIES);
    }

    /**
     * Maximum number of entries held in the query result cache (heap).
     */
    public static int getCacheSize() {
        return getInt(PROP_CACHE_SIZE, DEFAULT_CACHE_SIZE);
    }

    // --- helpers ---

    private static String getString(String key, String defaultValue) {
        String value = System.getProperty(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }

    private static int getInt(String key, int defaultValue) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static long getLong(String key, long defaultValue) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
