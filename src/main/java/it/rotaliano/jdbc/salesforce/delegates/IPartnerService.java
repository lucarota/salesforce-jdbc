package it.rotaliano.jdbc.salesforce.delegates;

import it.rotaliano.jdbc.salesforce.metadata.Table;
import it.rotaliano.jdbc.salesforce.utils.FieldDefTree;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.ws.ConnectionException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Interface for Salesforce Partner API operations.
 *
 * <p>This interface abstracts the Salesforce Partner API interactions,
 * allowing for easier testing through mocking and enabling alternative
 * implementations if needed.
 */
public interface IPartnerService {

    /**
     * Gets all queryable tables (SObjects) from Salesforce.
     *
     * @return list of tables with their metadata
     * @throws ConnectionException if connection to Salesforce fails
     */
    List<Table> getTables() throws ConnectionException;

    /**
     * Gets tables matching the specified pattern.
     *
     * @param tablePattern SQL LIKE pattern for table names
     * @return list of matching tables
     * @throws ConnectionException if connection to Salesforce fails
     */
    List<Table> getTables(String tablePattern) throws ConnectionException;

    /**
     * Describes a specific SObject type.
     *
     * @param sObjectType the SObject type name (e.g., "Account", "Contact")
     * @return the SObject description, or null if not found
     */
    DescribeSObjectResult describeSObject(String sObjectType);

    /**
     * Clears all cached schema information.
     */
    void cleanupGlobalCache();

    /**
     * Executes a SOQL query and returns all results.
     *
     * @param soql the SOQL query string
     * @param expectedSchema the expected field structure for result mapping
     * @return list of rows, each row is a list of field values
     * @throws ConnectionException if query execution fails
     */
    List<List<ForceResultField>> query(String soql, FieldDefTree expectedSchema) throws ConnectionException;

    /**
     * Starts a SOQL query and returns the first batch of results.
     *
     * @param soql the SOQL query string
     * @param expectedSchema the expected field structure for result mapping
     * @return entry containing results and query locator (null if done)
     * @throws ConnectionException if query execution fails
     */
    Map.Entry<List<List<ForceResultField>>, String> queryStart(String soql, FieldDefTree expectedSchema)
        throws ConnectionException;

    /**
     * Continues a query using the query locator from a previous call.
     *
     * @param queryLocator the locator from queryStart or previous queryMore
     * @param expectedSchema the expected field structure for result mapping
     * @return entry containing results and next query locator (null if done)
     * @throws ConnectionException if query execution fails
     */
    Map.Entry<List<List<ForceResultField>>, String> queryMore(String queryLocator, FieldDefTree expectedSchema)
        throws ConnectionException;

    /**
     * Creates new records in Salesforce.
     *
     * @param entityName the SObject type name
     * @param recordsDefinitions list of field-value maps for each record
     * @return array of save results with success/failure info
     * @throws ConnectionException if record creation fails
     */
    SaveResult[] createRecords(String entityName, List<Map<String, Object>> recordsDefinitions)
        throws ConnectionException;

    /**
     * Updates existing records in Salesforce.
     *
     * @param entityName the SObject type name
     * @param recordsDefinitions list of field-value maps (must include Id)
     * @return array of save results with success/failure info
     * @throws ConnectionException if record update fails
     */
    SaveResult[] saveRecords(String entityName, List<Map<String, Object>> recordsDefinitions)
        throws ConnectionException;

    /**
     * Deletes records from Salesforce.
     *
     * @param recordsIds collection of record IDs to delete
     * @return array of delete results with success/failure info
     * @throws ConnectionException if record deletion fails
     */
    DeleteResult[] deleteRecords(Collection<String> recordsIds) throws ConnectionException;
}
