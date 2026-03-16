package it.rotaliano.jdbc.salesforce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import it.rotaliano.jdbc.salesforce.connection.ForceConnection;
import it.rotaliano.jdbc.salesforce.delegates.PartnerService;
import it.rotaliano.jdbc.salesforce.statement.ForcePreparedStatement;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DBTablePrinter;

class ForceDriverConnectivityTest {

    private static final Logger log = LoggerFactory.getLogger(ForceDriverConnectivityTest.class);
    private static final String CREDENTIALS_FILE = ".credentials";

    private final String url;
    private final String userUAT;
    private final String passUAT;
    private final String userSIT;
    private final String passSIT;
    private final String userBugFix;
    private final String passBugFix;

    ForceDriverConnectivityTest() {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(CREDENTIALS_FILE)) {
            props.load(fis);
        } catch (IOException e) {
            log.warn("Could not load {}: {}. Live tests will fail.", CREDENTIALS_FILE, e.getMessage());
        }
        url = props.getProperty("url", "");
        userUAT = props.getProperty("userUAT", "");
        passUAT = props.getProperty("passUAT", "");
        userSIT = props.getProperty("userSIT", "");
        passSIT = props.getProperty("passSIT", "");
        userBugFix = props.getProperty("userBugFix", "");
        passBugFix = props.getProperty("passBugFix", "");
    }

    // ==================== RECORDING INFRASTRUCTURE ====================

    /**
     * Installs a RecordingPartnerService on the given live Connection.
     * Intercepts describeSObject() and query() calls, saving results to fixture files.
     */
    private void installRecordingPartnerService(Connection conn, String testName) {
        try {
            ForceConnection forceConn = (ForceConnection) conn;
            PartnerService original = forceConn.getPartnerService();
            PartnerService recording = new TestFixtureUtils.RecordingPartnerService(original, testName);

            Field psField = ForceConnection.class.getDeclaredField("partnerService");
            psField.setAccessible(true);
            psField.set(forceConn, recording);
        } catch (Exception e) {
            throw new RuntimeException("Failed to install recording PartnerService", e);
        }
    }

    // ==================== OFFLINE INFRASTRUCTURE ====================

    /**
     * Creates an offline ForceConnection backed by fixture files.
     * Uses FileBackedPartnerService that loads saved DescribeSObjectResult and query results.
     */
    private ForceConnection createOfflineConnection(String testName) {
        PartnerService fileBacked = new TestFixtureUtils.FileBackedPartnerService(testName);
        ConnectorConfig config = new ConnectorConfig();
        config.setManualLogin(true);
        config.setServiceEndpoint("https://offline.test/services/Soap/u/61.0");
        config.setAuthEndpoint("https://offline.test/services/Soap/u/61.0");
        config.setSessionId("offline-session");
        try {
            PartnerConnection pc = Connector.newConnection(config);
            return new ForceConnection(pc, fileBacked);
        } catch (ConnectionException e) {
            throw new RuntimeException("Failed to create offline PartnerConnection", e);
        }
    }

    // ==================== LIVE TESTS (run manually to record fixtures) ====================

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void selectWithSubquery_record() throws SQLException {
        String query = "CACHE GLOBAL SELECT Id,Account.Name,(SELECT Id, Name FROM Account.Contacts LIMIT 5) FROM Account where id= '0015E00000z3YaoQAE'";

        Connection con = DriverManager.getConnection(url, userUAT, passUAT);
        installRecordingPartnerService(con, "selectWithSubquery");
        ForcePreparedStatement select = (ForcePreparedStatement) con.prepareStatement(query);

        ResultSet result = select.executeQuery();

        DBTablePrinter.printResultSet(result);
        result.beforeFirst();
        int rowCount = 0;
        while (result.next()) {
            rowCount++;
        }
        assertEquals(5, rowCount);
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void selectWithParametersAndNestedSubquery_record() throws JSQLParserException, SQLException {
        String query = """
            select id, Zuora__Zuora_Id__c firstPdfId, Zuora__ZuoraId__c secondPdfId, name name, Zuora__InvoiceDate__c invoiceDate,
                Zuora__DueDate__c debitDate, Zuora__Balance2__c balance,
                Zuora__TotalAmount__c totalAmount, CurrencyIsoCode CurrencyIsoCode,
                ( SELECT Zuora__Payment__r.Zuora__Effective_Date__c effectiveDate, zuora__paymentpart__c.Zuora__Payment__r.Zuora__GatewayResponse__c zuoraGateway,
                zuora__paymentpart__c.Zuora__Payment__r.Zuora__Status__c paymentStatus FROM Zuora__Payment_Parts__r )
              from zuora__zinvoice__c
              where Available_On_CP__c != 'Hide'
              and zuora__account__c = ? order by name
              limit ? offset ?
            """;

        Connection con = DriverManager.getConnection(url, userBugFix, passBugFix);
        installRecordingPartnerService(con, "selectWithParametersAndNestedSubquery");
        PreparedStatement ps = con.prepareStatement(query);
        ps.setString(1, "0011l00001NOh7GAAT");
        ps.setInt(2, 20);
        ps.setInt(3, 0);

        ResultSet result = ps.executeQuery();
        DBTablePrinter.printResultSet(result);
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void selectWithTwoLevelRelation_record() throws JSQLParserException, SQLException {
        String query = """
            select
            	Id,
            	Zuora_Quote__r.zqu__Status__c as quoteStatus,
            	Zuora_Quote__c as ZuoraQuoteId,
            	RecordTypeId,
            	Zuora_Quote__r.Customer_Subscription__r.oss_technical_account_id__c as technicalAccountId,
            	Zuora_Quote__r.Customer_Subscription__r.Name as SubscriptionCode
            from
            	order
            where
            	Zuora_Quote__r.Customer_Subscription__r.oss_technical_account_id__c = '154290'
            	and RecordTypeId in ('0122o000000NxGPAA0')
            	and order_id__c = ?
            limit 1
            """;

        Connection con = DriverManager.getConnection(url, userBugFix, passBugFix);
        installRecordingPartnerService(con, "selectWithTwoLevelRelation");
        PreparedStatement ps = con.prepareStatement(query);
        ps.setString(1, "97ffe7cb");

        ResultSet result = ps.executeQuery();
        DBTablePrinter.printResultSet(result);
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void selectWithRelationTraversalOnBeam_record() throws JSQLParserException, SQLException {
        String query = """
            select Beam__r.Id as id, Beam__r.Name as name, Beam__r.Satellite__r.Name as satelliteName,
                                     Beam__r.Beam_Yearly_Service_Availability__c as beamYearlyServiceAvailability, Beam__r.Beam_Monthly_Service_Availability__c as beamMonthlyServiceAvailability,
                                     Beam__r.status__c as status, Beam__r.SKL_Start_Date__c as sklStartDate, Beam__r.SKL_End_Date__c as sklEndDate
                                     from Subscription_Beam__c
                                     where Account__r.OSS_Customer_Account_ID__c = ?
            """;

        Connection con = DriverManager.getConnection(url, userBugFix, passBugFix);
        installRecordingPartnerService(con, "selectWithRelationTraversalOnBeam");
        PreparedStatement ps = con.prepareStatement(query);
        ps.setString(1, "93694");

        ResultSet result = ps.executeQuery();
        DBTablePrinter.printResultSet(result);
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void selectWithMultiObjectRelation_record() throws JSQLParserException, SQLException {
        String query = """
            select
              Zuora__Subscription__r.Partner_Subscription__r.Zuora__Account__r.OSS_Customer_Account_ID__c as partnerId,
              Zuora__Subscription__r.Partner_Subscription__r.oss_technical_service_id__c as serviceId,
              Zuora__Subscription__r.OSS_Technical_Account_ID__c as subscriptionId,
              Zuora__Subscription__r.Zuora__Account__r.OSS_Customer_Account_ID__c as customerId,
              Product_Rate_Plan__r.zqu__Product__r.ProductCode as productCode,
              Product_Rate_Plan__r.zqu__Product__r.Product_Type_Id__c as planId
              from zuora__subscriptionrateplan__c
              where Zuora__Subscription__r.OSS_Technical_Account_ID__c = '152846'
              and Zuora__Subscription__r.Partner_Subscription__r.oss_technical_service_id__c = ?
              and Zuora__Subscription__r.Id != NULL
              and Product_Rate_Plan__r.Id != NULL
              and Product_Rate_Plan__r.zqu__Product__r.Id != NULL
            """;

        Connection con = DriverManager.getConnection(url, userSIT, passSIT);
        installRecordingPartnerService(con, "selectWithMultiObjectRelation");
        PreparedStatement ps = con.prepareStatement(query);
        ps.setString(1, "200135");

        ResultSet result = ps.executeQuery();
        DBTablePrinter.printResultSet(result);
        result.beforeFirst();
        while (result.next()) {
            assertEquals("402", result.getString("partnerId"));
            assertEquals("200135", result.getString("serviceId"));
            assertEquals("152846", result.getString("subscriptionId"));
            assertEquals("4169", result.getString("customerId"));
            assertEquals("1247", result.getString("productCode"));
            assertEquals("c0d34e55", result.getString("planId"));
        }
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void selectWithSelfPrefixedRelation_record() throws JSQLParserException, SQLException {
        String query = """
                select Id, Name, Status, CreatedDate, OrderNumber, Order_Id__c,
                Activation_Key__c , Zuora_Quote__r.zqu__Status__c,
                Zuora_Quote__c , RecordTypeId, Terminal_Class__c,
                Zuora_Quote__r.Customer_Subscription__r.OSS_Technical_Account_ID__c,
                Order.Zuora_Quote__r.Customer_Subscription__r.Name
                from Order
                -- where Zuora_Quote__r.zqu__Status__c != null and Terminal_Class__c != null
                limit 2
            """;

        Connection con = DriverManager.getConnection(url, userSIT, passSIT);
        installRecordingPartnerService(con, "selectWithSelfPrefixedRelation");
        ForcePreparedStatement select = (ForcePreparedStatement) con.prepareStatement(query);

        ResultSet result = select.executeQuery();
        DBTablePrinter.printResultSet(result);
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void selectWithNestedSubqueryColumnTypes_record() throws JSQLParserException, SQLException {
        String query = """
            select
            	id,
            	(
            	select
            		Zuora__Payment__r.Zuora__Effective_Date__c effectiveDate,
            		zuora__paymentpart__c.Zuora__Payment__r.Zuora__GatewayResponse__c zuoraGateway,
            		zuora__paymentpart__c.Zuora__Payment__r.Zuora__Status__c paymentStatus
            	from
            		Zuora__Payment_Parts__r)
            from
            	zuora__zinvoice__c
            where
            	Available_On_CP__c != 'Hide'
            	and zuora__account__c = ?
            limit 20 offset 0
            """;

        Connection con = DriverManager.getConnection(url, userBugFix, passBugFix);
        installRecordingPartnerService(con, "selectWithNestedSubqueryColumnTypes");
        PreparedStatement ps = con.prepareStatement(query);
        ps.setString(1, "0011l00001NVdGUAA1");

        ResultSet result = ps.executeQuery();
        DBTablePrinter.printResultSet(result);
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void selectStar_record() throws JSQLParserException, SQLException {
        String query = """
                select * from zuora__subscription__c
            """;

        Connection con = DriverManager.getConnection(url, userBugFix, passBugFix);
        installRecordingPartnerService(con, "selectStar");
        ForcePreparedStatement select = (ForcePreparedStatement) con.prepareStatement(query);

        ResultSet result = select.executeQuery();
        DBTablePrinter.printResultSet(result);
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void selectWithLiterals_record() throws JSQLParserException, SQLException {
        String query = """
                select 2, true from account a
            """;

        Connection con = DriverManager.getConnection(url, userBugFix, passBugFix);
        installRecordingPartnerService(con, "selectWithLiterals");
        ForcePreparedStatement select = (ForcePreparedStatement) con.prepareStatement(query);

        ResultSet result = select.executeQuery();
        DBTablePrinter.printResultSet(result);
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void selectWithEscapedQuoteInWhere_record() throws JSQLParserException, SQLException {
        String query = """
            select id, Account_Type__c as AccountType, ParentId, BillingEntity__c as BillingEntityId, installation_county__c as installationCountry,\s
            OSS_Customer_Account_ID__c as CustomerAccountId, Email_address__c from Account
            where Email_address__c = 'D\\'angeloo@yopmail.com'
        """;

        Connection con = DriverManager.getConnection(url, userSIT, passSIT);
        installRecordingPartnerService(con, "selectWithEscapedQuoteInWhere");
        ForcePreparedStatement select = (ForcePreparedStatement) con.prepareStatement(query);

        ResultSet result = select.executeQuery();
        DBTablePrinter.printResultSet(result);
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void selectWithEscapedQuoteInParameter_record() throws JSQLParserException, SQLException {
        String query = """
            select id, Account_Type__c as AccountType, ParentId, BillingEntity__c as BillingEntityId, installation_county__c as installationCountry,\s
            OSS_Customer_Account_ID__c as CustomerAccountId, Email_address__c from Account
            where Email_address__c = ?
            """;

        Connection con = DriverManager.getConnection(url, userSIT, passSIT);
        installRecordingPartnerService(con, "selectWithEscapedQuoteInParameter");
        PreparedStatement ps = con.prepareStatement(query);
        ps.setString(1, "D'angeloo@yopmail.com");

        ResultSet result = ps.executeQuery();
        DBTablePrinter.printResultSet(result);
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void selectWithDateParameter_record() throws JSQLParserException, SQLException {
        String query = """
            select
                Id,
                Name,
                Status,
                CreatedDate,
                OrderNumber,
                Order_Id__c,
                Activation_Key__c,
                Zuora_Quote__r.zqu__Status__c,
                Zuora_Quote__c,
                RecordTypeId,
                Terminal_Class__c,
                Zuora_Quote__r.Customer_Subscription__r.OSS_Technical_Account_ID__c,
                Zuora_Quote__r.Customer_Subscription__r.Name
            from order
            where CreatedDate > ?
            and Zuora_Quote__r.Customer_Subscription__r.oss_technical_account_id__c != null
            and RecordTypeId = '0122o000000NxGPAA0' limit 10
        """;

        LocalDate todayMinusOneMonth = new Date().toInstant()
            .atZone(ZoneId.systemDefault())
            .toLocalDate()
            .minusMonths(1);

        Connection con = DriverManager.getConnection(url, userBugFix, passBugFix);
        installRecordingPartnerService(con, "selectWithDateParameter");
        PreparedStatement ps = con.prepareStatement(query);
        ps.setObject(1, Date.from(todayMinusOneMonth.atStartOfDay(ZoneId.systemDefault()).toInstant()));

        ResultSet result = ps.executeQuery();
        DBTablePrinter.printResultSet(result);
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void selectWithTimestampFilter_record() throws JSQLParserException, SQLException {
        String query = """
    select Beam__r.
    Id as id, Beam__r.Name as name, Beam__r.Satellite__r.Name as satelliteName,
    Beam__r.Beam_Yearly_Service_Availability__c as beamYearlyServiceAvailability, Beam__r.Beam_Monthly_Service_Availability__c as beamMonthlyServiceAvailability,
    Beam__r.status__c as status, SKL_Start_Date__c as sklStartDate, SKL_End_Date__c as sklEndDate, SKL_NotificationType__c as NotificationType, SKL_EventID__c as EventID
    from Beam_Notification__c
    where Beam__c IN (select Beam__c from Subscription_Beam__c where Account__r.OSS_Customer_Account_ID__c = '27859')
    and lastModifiedDate > '{ts 2025-07-25 00:00:00Z}'
    and SKL_EventID__c != null  order by Beam__c""";

        Connection con = DriverManager.getConnection(url, userSIT, passSIT);
        installRecordingPartnerService(con, "selectWithTimestampFilter");
        ForcePreparedStatement select = (ForcePreparedStatement) con.prepareStatement(query);

        ResultSet result = select.executeQuery();
        DBTablePrinter.printResultSet(result);
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void selectWithAggregateMax_record() throws JSQLParserException, SQLException {
        String query = """
    SELECT max(createdDate)
    FROM Beam_Notification__c""";

        Connection con = DriverManager.getConnection(url, userSIT, passSIT);
        installRecordingPartnerService(con, "selectWithAggregateMax");
        ForcePreparedStatement select = (ForcePreparedStatement) con.prepareStatement(query);

        ResultSet result = select.executeQuery();
        DBTablePrinter.printResultSet(result);
    }

    // ==================== DML LIVE TESTS (cannot run offline) ====================

    @Test
    @Disabled("Live test - DML, cannot run offline")
    void updateContactFields() throws SQLException {
        String query = "update Contact set Firstname = 'Tester1', Marketing_Opt_in_SMS__c = true\n"
            + "WHERE Id =  '0031l00000cS7aYAAS'";

        Connection con = DriverManager.getConnection(url, userBugFix, passBugFix);
        Statement update = con.createStatement();
        int result = update.executeUpdate(query);
        assertEquals(1, result);
    }

    @Test
    @Disabled("Live test - DML, cannot run offline")
    void updateFieldsToNull() throws SQLException {
        String query = "UPDATE Asset SET Latitude__c=NULL, Longitude__c=NULL WHERE Id='02i1l00000CifsmAAB'";

        Connection con = DriverManager.getConnection(url, userSIT, passSIT);
        Statement update = con.createStatement();
        int result = update.executeUpdate(query);
        assertEquals(1, result);
    }

    @Test
    @Disabled("Live test - DML, cannot run offline")
    void updateWithPreparedStatement() throws SQLException {
        String query = "update Asset set Locking_Status__c = ? where Terminal_Account_ID__c = ?";

        Connection con = DriverManager.getConnection(url, userSIT, passSIT);
        PreparedStatement ps = con.prepareStatement(query);
        ps.setBoolean(1, false);
        ps.setString(2, "151013");
        int result = ps.executeUpdate();
        assertEquals(1, result);
    }

    @Test
    @Disabled("Live test - DML, cannot run offline")
    void insertAndRetrieveGeneratedKey() throws SQLException {
        String query = "INSERT INTO Asset (Name) VALUES (?)";

        Connection con = DriverManager.getConnection(url, userSIT, passSIT);
        PreparedStatement ps = con.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        ps.setString(1, "LR - Insert Test");
        ps.executeUpdate();

        ResultSet keys = ps.getGeneratedKeys();
        if (keys.next()) {
            log.info("key= {}", keys.getString(1));
        }
    }

    // ==================== METADATA LIVE TESTS ====================

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void describeSObjectAndSaveXml_record() throws ConnectionException, IOException {
        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(userSIT);
        partnerConfig.setPassword(passSIT);
        partnerConfig.setAuthEndpoint("https://test.salesforce.com/services/Soap/u/v61.0");
        PartnerConnection connection = Connector.newConnection(partnerConfig);
        connection.getConfig()
            .setServiceEndpoint(
                "https://eutelsat-bip-prod--bugfix.sandbox.my.salesforce.com/services/Soap/u/61.0/00D1l0000000RSj");

        DescribeSObjectResult[] records = connection.describeSObjects(new String[]{"Zuora__Subscription__c"});

        String xml = TestFixtureUtils.createXStream().toXML(records);
        Files.writeString(Paths.get("src/test/resources/Zuora__Subscription__c_description.xml"),
            xml, StandardOpenOption.CREATE);
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void loadAllTableMetadata_record() throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, userSIT, passSIT)) {
            DatabaseMetaData meta = conn.getMetaData();

            List<String> tables = List.of("Account", "Asset", "attachment", "Beam__c", "Case",
                "CommissionCalculationHistory__c", "Contact", "ContentDocumentLink", "ContentVersion",
                "Country_List__c", "folder__c", "Kafka_Job__c", "Lead", "MultiCastInfo__c", "Order", "product2",
                "RecordType", "Sales_Incentive_History__c", "service_region__c", "Subscription_Future_Change__c",
                "Wallet__c", "Wallet_Transaction__c", "Work_Info__c", "zqu__productoption__c",
                "zqu__ProductRatePlan__c", "zqu__quote__c", "zqu__QuoteRatePlan__c", "zqu__QuoteRatePlanCharge__c",
                "zuora__customeraccount__c", "zuora__payment__c", "zuora__paymentpart__c", "Zuora__Subscription__c",
                "Zuora_Subscription_History__c", "Zuora__SubscriptionRatePlan__c", "zuora__zinvoice__c");

            StopWatch sw = new StopWatch();
            sw.start();
            for (String table : tables) {
                log.info("Cached metadata for table: {}", table);
                ResultSet cols = meta.getColumns(null, null, table, null);
                while (cols.next()) {
                    if (log.isTraceEnabled()) {
                        log.trace("col: {}", cols.getString("COLUMN_NAME"));
                    }
                }
            }
            sw.stop();
            log.info("All tables cached: {}ms", sw.getTime());
        }
    }

    @Test
    @Disabled("Live test - run manually to record fixtures")
    void metadata_record() throws SQLException {
        Connection con = DriverManager.getConnection(url, userSIT, passSIT);
        ResultSet order = con.getMetaData().getIndexInfo(null, null, "product2", false, false);
        DBTablePrinter.printResultSet(order);
    }

    // ==================== OFFLINE TESTS (run without Salesforce) ====================

    @Nested
    class OfflineTests {

        private void assumeFixturesExist(String testName) {
            Assumptions.assumeTrue(TestFixtureUtils.fixturesExist(testName),
                "Fixtures not found for '" + testName + "'. Run the corresponding _record test against live Salesforce first.");
        }

        @Test
        void selectWithSubqueryReturnsExpectedRowCount() throws SQLException {
            assumeFixturesExist("selectWithSubquery");
            String query = "CACHE GLOBAL SELECT Id,Account.Name,(SELECT Id, Name FROM Account.Contacts LIMIT 5) FROM Account where id= '0015E00000z3YaoQAE'";

            ForceConnection conn = createOfflineConnection("selectWithSubquery");
            ForcePreparedStatement select = (ForcePreparedStatement) conn.prepareStatement(query);

            ResultSet result = select.executeQuery();
            assertNotNull(result, "ResultSet should not be null");

            ResultSetMetaData rsmd = result.getMetaData();
            assertTrue(rsmd.getColumnCount() >= 3, "Should have at least Id, Name, and subquery columns");

            result.beforeFirst();
            int rowCount = 0;
            while (result.next()) {
                rowCount++;
            }
            assertEquals(5, rowCount, "Subquery should return 5 contacts");
        }

        @Test
        void selectWithParametersAndNestedSubquery() throws SQLException {
            assumeFixturesExist("selectWithParametersAndNestedSubquery");
            String query = """
                select id, Zuora__Zuora_Id__c firstPdfId, Zuora__ZuoraId__c secondPdfId, name name, Zuora__InvoiceDate__c invoiceDate,
                    Zuora__DueDate__c debitDate, Zuora__Balance2__c balance,
                    Zuora__TotalAmount__c totalAmount, CurrencyIsoCode CurrencyIsoCode,
                    ( SELECT Zuora__Payment__r.Zuora__Effective_Date__c effectiveDate, zuora__paymentpart__c.Zuora__Payment__r.Zuora__GatewayResponse__c zuoraGateway,
                    zuora__paymentpart__c.Zuora__Payment__r.Zuora__Status__c paymentStatus FROM Zuora__Payment_Parts__r )
                  from zuora__zinvoice__c
                  where Available_On_CP__c != 'Hide'
                  and zuora__account__c = ? order by name
                  limit ? offset ?
                """;

            ForceConnection conn = createOfflineConnection("selectWithParametersAndNestedSubquery");
            PreparedStatement ps = conn.prepareStatement(query);
            ps.setString(1, "0011l00001NOh7GAAT");
            ps.setInt(2, 20);
            ps.setInt(3, 0);

            ResultSet result = ps.executeQuery();
            assertNotNull(result, "ResultSet should not be null");

            ResultSetMetaData rsmd = result.getMetaData();
            assertEquals(12, rsmd.getColumnCount(), "Should have 9 main fields + 3 subquery fields");
            assertEquals("firstPdfId", rsmd.getColumnLabel(2));
            assertEquals("secondPdfId", rsmd.getColumnLabel(3));
            assertEquals("invoiceDate", rsmd.getColumnLabel(5));
        }

        @Test
        void selectWithTwoLevelRelationResolvesFields() throws SQLException {
            assumeFixturesExist("selectWithTwoLevelRelation");
            String query = """
                select
                	Id,
                	Zuora_Quote__r.zqu__Status__c as quoteStatus,
                	Zuora_Quote__c as ZuoraQuoteId,
                	RecordTypeId,
                	Zuora_Quote__r.Customer_Subscription__r.oss_technical_account_id__c as technicalAccountId,
                	Zuora_Quote__r.Customer_Subscription__r.Name as SubscriptionCode
                from
                	order
                where
                	Zuora_Quote__r.Customer_Subscription__r.oss_technical_account_id__c = '154290'
                	and RecordTypeId in ('0122o000000NxGPAA0')
                	and order_id__c = ?
                limit 1
                """;

            ForceConnection conn = createOfflineConnection("selectWithTwoLevelRelation");
            PreparedStatement ps = conn.prepareStatement(query);
            ps.setString(1, "97ffe7cb");

            ResultSet result = ps.executeQuery();
            assertNotNull(result, "ResultSet should not be null");

            ResultSetMetaData rsmd = result.getMetaData();
            assertEquals(6, rsmd.getColumnCount(), "Should resolve all 6 fields including nested relations");
            assertEquals("quoteStatus", rsmd.getColumnLabel(2));
            assertEquals("ZuoraQuoteId", rsmd.getColumnLabel(3));
            assertEquals("technicalAccountId", rsmd.getColumnLabel(5));
            assertEquals("SubscriptionCode", rsmd.getColumnLabel(6));
        }

        @Test
        void selectWithMultiObjectRelationReturnsCorrectValues() throws SQLException {
            assumeFixturesExist("selectWithMultiObjectRelation");
            String query = """
                select
                  Zuora__Subscription__r.Partner_Subscription__r.Zuora__Account__r.OSS_Customer_Account_ID__c as partnerId,
                  Zuora__Subscription__r.Partner_Subscription__r.oss_technical_service_id__c as serviceId,
                  Zuora__Subscription__r.OSS_Technical_Account_ID__c as subscriptionId,
                  Zuora__Subscription__r.Zuora__Account__r.OSS_Customer_Account_ID__c as customerId,
                  Product_Rate_Plan__r.zqu__Product__r.ProductCode as productCode,
                  Product_Rate_Plan__r.zqu__Product__r.Product_Type_Id__c as planId
                  from zuora__subscriptionrateplan__c
                  where Zuora__Subscription__r.OSS_Technical_Account_ID__c = '152846'
                  and Zuora__Subscription__r.Partner_Subscription__r.oss_technical_service_id__c = ?
                  and Zuora__Subscription__r.Id != NULL
                  and Product_Rate_Plan__r.Id != NULL
                  and Product_Rate_Plan__r.zqu__Product__r.Id != NULL
                """;

            ForceConnection conn = createOfflineConnection("selectWithMultiObjectRelation");
            PreparedStatement ps = conn.prepareStatement(query);
            ps.setString(1, "200135");

            ResultSet result = ps.executeQuery();
            assertNotNull(result, "ResultSet should not be null");

            assertEquals(6, result.getMetaData().getColumnCount());

            int rowCount = 0;
            result.beforeFirst();
            while (result.next()) {
                assertEquals("402", result.getString("partnerId"));
                assertEquals("200135", result.getString("serviceId"));
                assertEquals("152846", result.getString("subscriptionId"));
                assertEquals("4169", result.getString("customerId"));
                assertEquals("1247", result.getString("productCode"));
                assertEquals("c0d34e55", result.getString("planId"));
                rowCount++;
            }
            assertTrue(rowCount > 0, "Should return at least one row");
        }

        @Test
        void selectWithSelfPrefixedRelationResolvesAllColumns() throws SQLException {
            assumeFixturesExist("selectWithSelfPrefixedRelation");
            String query = """
                    select Id, Name, Status, CreatedDate, OrderNumber, Order_Id__c,
                    Activation_Key__c , Zuora_Quote__r.zqu__Status__c,
                    Zuora_Quote__c , RecordTypeId, Terminal_Class__c,
                    Zuora_Quote__r.Customer_Subscription__r.OSS_Technical_Account_ID__c,
                    Order.Zuora_Quote__r.Customer_Subscription__r.Name
                    from Order
                    -- where Zuora_Quote__r.zqu__Status__c != null and Terminal_Class__c != null
                    limit 2
                """;

            ForceConnection conn = createOfflineConnection("selectWithSelfPrefixedRelation");
            ForcePreparedStatement select = (ForcePreparedStatement) conn.prepareStatement(query);

            ResultSet result = select.executeQuery();
            assertNotNull(result, "ResultSet should not be null");

            ResultSetMetaData rsmd = result.getMetaData();
            assertEquals(13, rsmd.getColumnCount(), "Should resolve all 13 columns including self-prefixed relations");
        }

        @Test
        void selectWithNestedSubqueryResolvesCorrectColumnTypes() throws SQLException {
            assumeFixturesExist("selectWithNestedSubqueryColumnTypes");
            String query = """
                select
                	id,
                	(
                	select
                		Zuora__Payment__r.Zuora__Effective_Date__c effectiveDate,
                		zuora__paymentpart__c.Zuora__Payment__r.Zuora__GatewayResponse__c zuoraGateway,
                		zuora__paymentpart__c.Zuora__Payment__r.Zuora__Status__c paymentStatus
                	from
                		Zuora__Payment_Parts__r)
                from
                	zuora__zinvoice__c
                where
                	Available_On_CP__c != 'Hide'
                	and zuora__account__c = ?
                limit 20 offset 0
                """;

            ForceConnection conn = createOfflineConnection("selectWithNestedSubqueryColumnTypes");
            PreparedStatement ps = conn.prepareStatement(query);
            ps.setString(1, "0011l00001NVdGUAA1");

            ResultSet result = ps.executeQuery();
            assertNotNull(result, "ResultSet should not be null");

            ResultSetMetaData rsmd = result.getMetaData();
            assertEquals(4, rsmd.getColumnCount(), "Should have id + 3 subquery fields");

            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                String label = rsmd.getColumnLabel(i);
                assertNotNull(label, "Column label should not be null at index " + i);
                assertFalse(label.isBlank(), "Column label should not be blank at index " + i);
                int colType = rsmd.getColumnType(i);
                assertTrue(colType != 0, "Column type should not be 0 for " + label);
            }
        }

        @Test
        void selectWithEscapedQuoteInParameter() throws SQLException {
            assumeFixturesExist("selectWithEscapedQuoteInParameter");
            String query = """
                select id, Account_Type__c as AccountType, ParentId, BillingEntity__c as BillingEntityId, installation_county__c as installationCountry,\s
                OSS_Customer_Account_ID__c as CustomerAccountId, Email_address__c from Account
                where Email_address__c = ?
                """;

            ForceConnection conn = createOfflineConnection("selectWithEscapedQuoteInParameter");
            PreparedStatement ps = conn.prepareStatement(query);
            ps.setString(1, "D'angeloo@yopmail.com");

            ResultSet result = ps.executeQuery();
            assertNotNull(result, "ResultSet should not be null");

            ResultSetMetaData rsmd = result.getMetaData();
            assertEquals(7, rsmd.getColumnCount(), "Should resolve all 7 columns with aliases");
            assertEquals("AccountType", rsmd.getColumnLabel(2));
            assertEquals("BillingEntityId", rsmd.getColumnLabel(4));
            assertEquals("installationCountry", rsmd.getColumnLabel(5));
            assertEquals("CustomerAccountId", rsmd.getColumnLabel(6));
        }

        @Test
        void selectWithAggregateFunction() throws SQLException {
            assumeFixturesExist("selectWithAggregateMax");
            String query = """
                SELECT max(createdDate)
                FROM Beam_Notification__c""";

            ForceConnection conn = createOfflineConnection("selectWithAggregateMax");
            ForcePreparedStatement select = (ForcePreparedStatement) conn.prepareStatement(query);

            ResultSet result = select.executeQuery();
            assertNotNull(result, "ResultSet should not be null");

            ResultSetMetaData rsmd = result.getMetaData();
            assertEquals(1, rsmd.getColumnCount(), "Aggregate query should return exactly 1 column");
        }
    }
}
