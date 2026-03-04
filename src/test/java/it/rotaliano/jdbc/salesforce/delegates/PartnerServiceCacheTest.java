package it.rotaliano.jdbc.salesforce.delegates;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PartnerServiceCacheTest {

    @Mock
    private PartnerConnection partnerConnection;

    @Mock
    private ConnectorConfig connectorConfig;

    private PartnerService partnerService;

    // Use unique orgIds to avoid cache collisions between tests
    private String testOrgId;
    private static int orgIdCounter = 0;

    @BeforeEach
    void setUp() {
        // Generate unique orgId for each test to avoid cache collision
        testOrgId = "testOrg_" + System.currentTimeMillis() + "_" + (++orgIdCounter);

        lenient().when(partnerConnection.getConfig()).thenReturn(connectorConfig);
        lenient().when(connectorConfig.getUsername()).thenReturn("test@example.com");

        partnerService = new PartnerService(partnerConnection, testOrgId);
    }

    @AfterEach
    void tearDown() {
        // Cleanup global cache after each test
        if (partnerService != null) {
            partnerService.cleanupGlobalCache();
        }
    }

    @Nested
    @DisplayName("describeSObject cache tests")
    class DescribeSObjectCacheTests {

        @Test
        @DisplayName("should cache describeSObject result")
        void testDescribeSObjectIsCached() throws ConnectionException {
            // Setup
            DescribeSObjectResult mockResult = createMockDescribeSObjectResult("Account");
            when(partnerConnection.describeSObject("Account")).thenReturn(mockResult);

            // First call - should hit the API
            DescribeSObjectResult result1 = partnerService.describeSObject("Account");
            assertNotNull(result1);
            assertEquals("Account", result1.getName());

            // Second call - should use cache
            DescribeSObjectResult result2 = partnerService.describeSObject("Account");
            assertNotNull(result2);
            assertEquals("Account", result2.getName());

            // Verify API was called only once
            verify(partnerConnection, times(1)).describeSObject("Account");
        }

        @Test
        @DisplayName("should cache different objects separately")
        void testDescribeSObjectCachesDifferentObjects() throws ConnectionException {
            // Setup
            DescribeSObjectResult accountResult = createMockDescribeSObjectResult("Account");
            DescribeSObjectResult contactResult = createMockDescribeSObjectResult("Contact");
            when(partnerConnection.describeSObject("Account")).thenReturn(accountResult);
            when(partnerConnection.describeSObject("Contact")).thenReturn(contactResult);

            // Call for Account
            DescribeSObjectResult result1 = partnerService.describeSObject("Account");
            assertEquals("Account", result1.getName());

            // Call for Contact
            DescribeSObjectResult result2 = partnerService.describeSObject("Contact");
            assertEquals("Contact", result2.getName());

            // Call Account again - should use cache
            DescribeSObjectResult result3 = partnerService.describeSObject("Account");
            assertEquals("Account", result3.getName());

            // Verify each was called only once
            verify(partnerConnection, times(1)).describeSObject("Account");
            verify(partnerConnection, times(1)).describeSObject("Contact");
        }

        @Test
        @DisplayName("should return null for invalid SObject without caching")
        void testDescribeSObjectInvalidReturnsNull() throws ConnectionException {
            // Setup - throw InvalidSObjectFault for invalid object
            when(partnerConnection.describeSObject("InvalidObject"))
                .thenThrow(new com.sforce.soap.partner.fault.InvalidSObjectFault());

            // Should return null for invalid object
            DescribeSObjectResult result = partnerService.describeSObject("InvalidObject");
            assertNull(result);
        }
    }

    @Nested
    @DisplayName("cleanupGlobalCache tests")
    class CleanupCacheTests {

        @Test
        @DisplayName("should clear cache when cleanupGlobalCache is called")
        void testCleanupGlobalCache() throws ConnectionException {
            // Setup
            DescribeSObjectResult mockResult = createMockDescribeSObjectResult("Account");
            when(partnerConnection.describeSObject("Account")).thenReturn(mockResult);

            // First call
            partnerService.describeSObject("Account");
            verify(partnerConnection, times(1)).describeSObject("Account");

            // Clear cache
            partnerService.cleanupGlobalCache();

            // Create new instance with same orgId to test cache was cleared
            partnerService = new PartnerService(partnerConnection, testOrgId);

            // Should call API again after cache clear
            partnerService.describeSObject("Account");
            verify(partnerConnection, times(2)).describeSObject("Account");
        }
    }

    @Nested
    @DisplayName("Multi-org cache isolation tests")
    class MultiOrgCacheTests {

        @Test
        @DisplayName("should isolate cache by orgId")
        void testCacheIsolationByOrgId() throws ConnectionException {
            // Setup
            DescribeSObjectResult accountResult = createMockDescribeSObjectResult("Account");
            when(partnerConnection.describeSObject("Account")).thenReturn(accountResult);

            String org1 = "uniqueOrg1_" + System.currentTimeMillis();
            String org2 = "uniqueOrg2_" + System.currentTimeMillis();

            // Create service for org1
            PartnerService serviceOrg1 = new PartnerService(partnerConnection, org1);
            DescribeSObjectResult result1 = serviceOrg1.describeSObject("Account");
            assertNotNull(result1);

            // Create service for org2
            PartnerService serviceOrg2 = new PartnerService(partnerConnection, org2);
            DescribeSObjectResult result2 = serviceOrg2.describeSObject("Account");
            assertNotNull(result2);

            // Both should have called the API since different orgIds
            verify(partnerConnection, times(2)).describeSObject("Account");

            // Cleanup
            serviceOrg1.cleanupGlobalCache();
        }

        @Test
        @DisplayName("should use username as connectionId when orgId is null")
        void testUsesUsernameWhenOrgIdNull() throws ConnectionException {
            // Setup with unique username
            String uniqueUsername = "user_" + System.currentTimeMillis() + "@test.com";
            when(connectorConfig.getUsername()).thenReturn(uniqueUsername);

            DescribeSObjectResult mockResult = createMockDescribeSObjectResult("Account");
            when(partnerConnection.describeSObject("Account")).thenReturn(mockResult);

            // Create service without orgId (uses username as key)
            PartnerService service = new PartnerService(partnerConnection, null);

            // First call
            service.describeSObject("Account");

            // Second call should use cache
            service.describeSObject("Account");

            // API should be called only once
            verify(partnerConnection, times(1)).describeSObject("Account");

            // Cleanup
            service.cleanupGlobalCache();
        }
    }

    @Nested
    @DisplayName("Thread safety concern documentation")
    class ThreadSafetyTests {

        @Test
        @DisplayName("static cache is shared between instances - potential issue")
        void testStaticCacheIsShared() throws ConnectionException {
            // This test documents the current behavior that static caches are shared
            // which could cause issues in multi-threaded environments

            DescribeSObjectResult mockResult = createMockDescribeSObjectResult("SharedObject");
            when(partnerConnection.describeSObject("SharedObject")).thenReturn(mockResult);

            // Two instances with same orgId share the same cache
            String sharedOrgId = "sharedOrg_" + System.currentTimeMillis();
            PartnerService service1 = new PartnerService(partnerConnection, sharedOrgId);
            PartnerService service2 = new PartnerService(partnerConnection, sharedOrgId);

            // First instance caches the result
            service1.describeSObject("SharedObject");

            // Second instance gets cached result without calling API
            service2.describeSObject("SharedObject");

            // API was called only once because cache is shared
            verify(partnerConnection, times(1)).describeSObject("SharedObject");

            // Cleanup
            service1.cleanupGlobalCache();
        }
    }

    private DescribeSObjectResult createMockDescribeSObjectResult(String name) {
        DescribeSObjectResult result = new DescribeSObjectResult();
        result.setName(name);
        result.setQueryable(true);

        // Add minimal fields
        Field idField = new Field();
        idField.setName("Id");
        idField.setType(FieldType.id);
        idField.setLength(18);

        Field nameField = new Field();
        nameField.setName("Name");
        nameField.setType(FieldType.string);
        nameField.setLength(255);

        result.setFields(new Field[]{idField, nameField});
        return result;
    }
}
