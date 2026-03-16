package it.rotaliano.jdbc.salesforce;

import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.ws.ConnectionException;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.security.NoTypePermission;
import com.thoughtworks.xstream.security.NullPermission;
import com.thoughtworks.xstream.security.PrimitiveTypePermission;
import it.rotaliano.jdbc.salesforce.delegates.ForceResultField;
import it.rotaliano.jdbc.salesforce.delegates.PartnerService;
import it.rotaliano.jdbc.salesforce.utils.FieldDefTree;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFixtureUtils {

    private static final Logger log = LoggerFactory.getLogger(TestFixtureUtils.class);
    private static final Path FIXTURES_BASE = Paths.get("src/test/resources/fixtures/connectivity");

    public static XStream createXStream() {
        XStream xstream = new XStream();
        xstream.addPermission(NoTypePermission.NONE);
        xstream.addPermission(NullPermission.NULL);
        xstream.addPermission(PrimitiveTypePermission.PRIMITIVES);
        xstream.allowTypeHierarchy(Collection.class);
        xstream.addImmutableType(com.sforce.soap.partner.SoapType.class, true);
        xstream.addImmutableType(com.sforce.soap.partner.FieldType.class, true);
        xstream.allowTypesByRegExp(new String[]{".*"});
        return xstream;
    }

    public static Path describeDir(String testName) {
        return FIXTURES_BASE.resolve(testName).resolve("describe");
    }

    public static Path queryResultPath(String testName) {
        return FIXTURES_BASE.resolve(testName).resolve("query_result.xml");
    }

    public static void saveDescribe(String testName, String sObjectType, DescribeSObjectResult result) {
        Path dir = describeDir(testName);
        try {
            Files.createDirectories(dir);
            String xml = createXStream().toXML(result);
            Files.writeString(dir.resolve(sObjectType + ".xml"), xml,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save describe fixture for " + sObjectType, e);
        }
    }

    public static DescribeSObjectResult loadDescribe(String testName, String sObjectType) {
        Path file = describeDir(testName).resolve(sObjectType + ".xml");
        try {
            String xml = Files.readString(file);
            return (DescribeSObjectResult) createXStream().fromXML(xml);
        } catch (IOException e) {
            throw new RuntimeException("Fixture not found: " + file + ". Run the live recorder test first.", e);
        }
    }

    public static void saveQueryResult(String testName, List<List<ForceResultField>> result) {
        Path file = queryResultPath(testName);
        try {
            Files.createDirectories(file.getParent());
            String xml = createXStream().toXML(result);
            Files.writeString(file, xml,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save query result fixture for " + testName, e);
        }
    }

    @SuppressWarnings("unchecked")
    public static List<List<ForceResultField>> loadQueryResult(String testName) {
        Path file = queryResultPath(testName);
        try {
            String xml = Files.readString(file);
            return (List<List<ForceResultField>>) createXStream().fromXML(xml);
        } catch (IOException e) {
            throw new RuntimeException("Fixture not found: " + file + ". Run the live recorder test first.", e);
        }
    }

    public static boolean fixturesExist(String testName) {
        return Files.isDirectory(describeDir(testName)) && Files.exists(queryResultPath(testName));
    }

    /**
     * PartnerService subclass that loads describe results and query results from fixture files.
     * Used for offline testing without a live Salesforce connection.
     */
    public static class FileBackedPartnerService extends PartnerService {

        private final String testName;

        public FileBackedPartnerService(String testName) {
            super(null, "test-offline-" + testName);
            this.testName = testName;
        }

        @Override
        public DescribeSObjectResult describeSObject(String sObjectType) {
            return loadDescribe(testName, sObjectType);
        }

        @Override
        public List<List<ForceResultField>> query(String soql, FieldDefTree expectedSchema)
            throws ConnectionException {
            return loadQueryResult(testName);
        }

        @Override
        public Map.Entry<List<List<ForceResultField>>, String> queryStart(String soql,
            FieldDefTree expectedSchema) throws ConnectionException {
            // Return full result in a single batch with null locator (= done)
            return new java.util.AbstractMap.SimpleEntry<>(loadQueryResult(testName), null);
        }

        @Override
        public Map.Entry<List<List<ForceResultField>>, String> queryMore(String queryLocator,
            FieldDefTree expectedSchema) throws ConnectionException {
            return new java.util.AbstractMap.SimpleEntry<>(Collections.emptyList(), null);
        }
    }

    /**
     * PartnerService subclass that delegates to a real PartnerService but records
     * describeSObject() and query() results to fixture files.
     * Used during live test runs to capture data for offline testing.
     *
     * <p>For non-cached queries the driver uses the streaming path (queryStart/queryMore).
     * This implementation intercepts queryStart() by calling delegate.query() to fetch
     * the full result in one shot, saves it, and returns everything as a single batch.
     */
    public static class RecordingPartnerService extends PartnerService {

        private final PartnerService delegate;
        private final String testName;

        public RecordingPartnerService(PartnerService delegate, String testName) {
            super(null, "test-recording-" + testName);
            this.delegate = delegate;
            this.testName = testName;
        }

        @Override
        public DescribeSObjectResult describeSObject(String sObjectType) {
            DescribeSObjectResult result = delegate.describeSObject(sObjectType);
            if (result != null) {
                saveDescribe(testName, sObjectType, result);
                log.info("[Recorder] Saved describe for {} in test {}", sObjectType, testName);
            }
            return result;
        }

        @Override
        public List<List<ForceResultField>> query(String soql, FieldDefTree expectedSchema)
            throws ConnectionException {
            List<List<ForceResultField>> result = delegate.query(soql, expectedSchema);
            saveQueryResult(testName, result);
            log.info("[Recorder] Saved query result via query() for test {} ({} rows)", testName, result.size());
            return result;
        }

        @Override
        public Map.Entry<List<List<ForceResultField>>, String> queryStart(String soql,
            FieldDefTree expectedSchema) throws ConnectionException {
            // Fetch the full result via delegate.query() and save it.
            // Return everything as a single batch (null locator = done).
            List<List<ForceResultField>> fullResult = delegate.query(soql, expectedSchema);
            saveQueryResult(testName, fullResult);
            log.info("[Recorder] Saved query result via queryStart() for test {} ({} rows)", testName, fullResult.size());
            return new java.util.AbstractMap.SimpleEntry<>(fullResult, null);
        }

        @Override
        public Map.Entry<List<List<ForceResultField>>, String> queryMore(String queryLocator,
            FieldDefTree expectedSchema) throws ConnectionException {
            return delegate.queryMore(queryLocator, expectedSchema);
        }
    }
}
