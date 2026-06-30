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
import it.rotaliano.jdbc.salesforce.utils.TreeNode;
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

    public static String sha256(String base) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(base.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception ex) {
            return String.valueOf(base.hashCode());
        }
    }

    public static Path queryResultPath(String testName) {
        return queryResultPath(testName, null);
    }

    public static Path queryResultPath(String testName, String soql) {
        if (soql == null) {
            return FIXTURES_BASE.resolve(testName).resolve("query_result.xml");
        }
        String normalized = soql.trim().replaceAll("\\s+", " ").toLowerCase();
        return FIXTURES_BASE.resolve(testName).resolve("query_" + sha256(normalized) + ".xml");
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

    public static void saveQueryResult(String testName, String soql, List<List<ForceResultField>> result) {
        Path file = queryResultPath(testName, soql);
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
    public static List<List<ForceResultField>> loadQueryResult(String testName, String soql) {
        Path file = queryResultPath(testName, soql);
        if (!Files.exists(file)) {
            file = queryResultPath(testName, null);
        }
        try {
            String xml = Files.readString(file);
            return (List<List<ForceResultField>>) createXStream().fromXML(xml);
        } catch (IOException e) {
            throw new RuntimeException("Fixture not found: " + file + ". Run the live recorder test first.", e);
        }
    }

    public static boolean fixturesExist(String testName) {
        return Files.isDirectory(describeDir(testName)) &&
            (Files.exists(queryResultPath(testName, null)) || Files.isDirectory(FIXTURES_BASE.resolve(testName)));
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

        private List<List<ForceResultField>> getExpandedQueryResult(String soql, FieldDefTree expectedSchema) {
            List<List<ForceResultField>> rawRows = loadQueryResult(testName, soql);
            if (expectedSchema == null) {
                return rawRows;
            }
            java.util.List<TreeNode<ForceResultField>> treeRows = new java.util.ArrayList<>();
            for (java.util.List<ForceResultField> row : rawRows) {
                TreeNode<ForceResultField> rowNode = new TreeNode<>();
                for (ForceResultField f : row) {
                    rowNode.addChild(f);
                }
                treeRows.add(rowNode);
            }
            return FieldDefTree.expand(treeRows, expectedSchema);
        }

        @Override
        public List<List<ForceResultField>> query(String soql, FieldDefTree expectedSchema)
            throws ConnectionException {
            return getExpandedQueryResult(soql, expectedSchema);
        }

        @Override
        public Map.Entry<List<List<ForceResultField>>, String> queryStart(String soql,
            FieldDefTree expectedSchema) throws ConnectionException {
            return new java.util.AbstractMap.SimpleEntry<>(getExpandedQueryResult(soql, expectedSchema), null);
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
            saveQueryResult(testName, soql, result);
            log.info("[Recorder] Saved query result via query() for test {} ({} rows)", testName, result.size());
            return result;
        }

        @Override
        public Map.Entry<List<List<ForceResultField>>, String> queryStart(String soql,
            FieldDefTree expectedSchema) throws ConnectionException {
            List<List<ForceResultField>> fullResult = delegate.query(soql, expectedSchema);
            saveQueryResult(testName, soql, fullResult);
            log.info("[Recorder] Saved query result via queryStart() for test {} ({} rows)", testName, fullResult.size());
            return new java.util.AbstractMap.SimpleEntry<>(fullResult, null);
        }

        @Override
        public Map.Entry<List<List<ForceResultField>>, String> queryMore(String queryLocator,
            FieldDefTree expectedSchema) throws ConnectionException {
            return delegate.queryMore(queryLocator, expectedSchema);
        }

        @Override
        public com.sforce.soap.partner.SaveResult[] createRecords(String entityName, List<Map<String, Object>> recordsDefinitions)
            throws ConnectionException {
            return delegate.createRecords(entityName, recordsDefinitions);
        }

        @Override
        public com.sforce.soap.partner.SaveResult[] saveRecords(String entityName, List<Map<String, Object>> recordsDefinitions)
            throws ConnectionException {
            return delegate.saveRecords(entityName, recordsDefinitions);
        }

        @Override
        public com.sforce.soap.partner.DeleteResult[] deleteRecords(Collection<String> recordsIds) throws ConnectionException {
            return delegate.deleteRecords(recordsIds);
        }
    }
}
