package it.rotaliano.jdbc.salesforce;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DriverConfigurationTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty(DriverConfiguration.PROP_BATCH_SIZE);
        System.clearProperty(DriverConfiguration.PROP_API_VERSION);
        System.clearProperty(DriverConfiguration.PROP_CONNECT_TIMEOUT);
        System.clearProperty(DriverConfiguration.PROP_READ_TIMEOUT);
        System.clearProperty(DriverConfiguration.PROP_MAX_RETRIES);
        System.clearProperty(DriverConfiguration.PROP_CACHE_SIZE);
    }

    @Nested
    @DisplayName("Default values")
    class DefaultValuesTests {

        @Test
        @DisplayName("batchSize default is 100")
        void testBatchSizeDefault() {
            assertEquals(100, DriverConfiguration.getBatchSize());
        }

        @Test
        @DisplayName("apiVersion default is 65")
        void testApiVersionDefault() {
            assertEquals("65", DriverConfiguration.getApiVersion());
        }

        @Test
        @DisplayName("connectTimeout default is 10000 ms")
        void testConnectTimeoutDefault() {
            assertEquals(10_000L, DriverConfiguration.getConnectTimeout());
        }

        @Test
        @DisplayName("readTimeout default is 30000 ms")
        void testReadTimeoutDefault() {
            assertEquals(30_000L, DriverConfiguration.getReadTimeout());
        }

        @Test
        @DisplayName("maxRetries default is 5")
        void testMaxRetriesDefault() {
            assertEquals(5, DriverConfiguration.getMaxRetries());
        }

        @Test
        @DisplayName("cacheSize default is 100")
        void testCacheSizeDefault() {
            assertEquals(100, DriverConfiguration.getCacheSize());
        }
    }

    @Nested
    @DisplayName("Override via system properties")
    class SystemPropertyOverrideTests {

        @Test
        @DisplayName("batchSize is overridable")
        void testBatchSizeOverride() {
            System.setProperty(DriverConfiguration.PROP_BATCH_SIZE, "200");
            assertEquals(200, DriverConfiguration.getBatchSize());
        }

        @Test
        @DisplayName("apiVersion is overridable")
        void testApiVersionOverride() {
            System.setProperty(DriverConfiguration.PROP_API_VERSION, "55");
            assertEquals("55", DriverConfiguration.getApiVersion());
        }

        @Test
        @DisplayName("connectTimeout is overridable")
        void testConnectTimeoutOverride() {
            System.setProperty(DriverConfiguration.PROP_CONNECT_TIMEOUT, "5000");
            assertEquals(5000L, DriverConfiguration.getConnectTimeout());
        }

        @Test
        @DisplayName("readTimeout is overridable")
        void testReadTimeoutOverride() {
            System.setProperty(DriverConfiguration.PROP_READ_TIMEOUT, "60000");
            assertEquals(60_000L, DriverConfiguration.getReadTimeout());
        }

        @Test
        @DisplayName("maxRetries is overridable")
        void testMaxRetriesOverride() {
            System.setProperty(DriverConfiguration.PROP_MAX_RETRIES, "3");
            assertEquals(3, DriverConfiguration.getMaxRetries());
        }

        @Test
        @DisplayName("cacheSize is overridable")
        void testCacheSizeOverride() {
            System.setProperty(DriverConfiguration.PROP_CACHE_SIZE, "500");
            assertEquals(500, DriverConfiguration.getCacheSize());
        }
    }

    @Nested
    @DisplayName("Invalid property values fall back to defaults")
    class InvalidValueFallbackTests {

        @Test
        @DisplayName("invalid batchSize falls back to default")
        void testBatchSizeInvalid() {
            System.setProperty(DriverConfiguration.PROP_BATCH_SIZE, "not-a-number");
            assertEquals(100, DriverConfiguration.getBatchSize());
        }

        @Test
        @DisplayName("empty apiVersion falls back to default")
        void testApiVersionEmpty() {
            System.setProperty(DriverConfiguration.PROP_API_VERSION, "   ");
            assertEquals("65", DriverConfiguration.getApiVersion());
        }

        @Test
        @DisplayName("invalid connectTimeout falls back to default")
        void testConnectTimeoutInvalid() {
            System.setProperty(DriverConfiguration.PROP_CONNECT_TIMEOUT, "abc");
            assertEquals(10_000L, DriverConfiguration.getConnectTimeout());
        }

        @Test
        @DisplayName("invalid maxRetries falls back to default")
        void testMaxRetriesInvalid() {
            System.setProperty(DriverConfiguration.PROP_MAX_RETRIES, "");
            assertEquals(5, DriverConfiguration.getMaxRetries());
        }

        @Test
        @DisplayName("invalid cacheSize falls back to default")
        void testCacheSizeInvalid() {
            System.setProperty(DriverConfiguration.PROP_CACHE_SIZE, "xyz");
            assertEquals(100, DriverConfiguration.getCacheSize());
        }
    }
}
