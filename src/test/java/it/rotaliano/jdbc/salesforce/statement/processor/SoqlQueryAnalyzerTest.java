package it.rotaliano.jdbc.salesforce.statement.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import it.rotaliano.jdbc.salesforce.delegates.PartnerService;
import it.rotaliano.jdbc.salesforce.statement.FieldDef;
import it.rotaliano.jdbc.salesforce.utils.FieldDefTree;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.security.NoTypePermission;
import com.thoughtworks.xstream.security.NullPermission;
import com.thoughtworks.xstream.security.PrimitiveTypePermission;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class SoqlQueryAnalyzerTest {

    private final PartnerService partnerService;

    SoqlQueryAnalyzerTest() {
        partnerService = mock(PartnerService.class);
        when(partnerService.describeSObject(eq("Account"))).thenReturn(describeSObject("Account"));
        when(partnerService.describeSObject(eq("Contact"))).thenReturn(describeSObject("Contact"));
        when(partnerService.describeSObject(eq("User"))).thenReturn(describeSObject("User"));
        when(partnerService.describeSObject(eq("Order"))).thenReturn(describeSObject("Order"));
        when(partnerService.describeSObject(eq("zqu__Quote__c"))).thenReturn(describeSObject("zqu__Quote__c"));
        when(partnerService.describeSObject(eq("Zuora__Subscription__c"))).thenReturn(describeSObject("Zuora__Subscription__c"));
    }

    @Test
    public void testGetFieldNames_SimpleQuery() {
        String soql = " select Id ,Name \r\nfrom Account\r\n where something = 'nothing' ";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);

        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<String> expected = Arrays.asList("Id", "Name");
        List<String> actual = listFlatFieldNames(analyzer);

        assertEquals(expected, actual);
    }

    private List<String> listFlatFieldNames(SoqlQueryAnalyzer analyzer) {
        return analyzer.getFieldDefinitions().flatten().stream()
            .map(FieldDef::getName)
            .collect(Collectors.toList());
    }

    private List<String> listFlatFieldAliases(SoqlQueryAnalyzer analyzer) {
        return analyzer.getFieldDefinitions().flatten().stream()
            .map(FieldDef::getAlias)
            .collect(Collectors.toList());
    }

    @Test
    public void testGetFieldNames_SelectWithReferences() {
        String soql = " select Id , Account.Name \r\nfrom Contact\r\n where something = 'nothing' ";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<String> expected = Arrays.asList("Id", "Name");
        List<String> actual = listFlatFieldNames(analyzer);

        assertEquals(expected, actual);
    }

    @Test
    public void testGetFieldNames_SelectWithAggregateAliased() {
        String soql = " select Id , Account.Name, count(id) aggrAlias1\r\nfrom Contact\r\n where something = 'nothing' ";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);
        // Just Name is confusing - see the case
        List<String> expected = Arrays.asList("Id", "Name", "aggrAlias1");
        List<String> actual = listFlatFieldNames(analyzer);

        assertEquals(expected, actual);
    }

    @Test
    public void testGetFieldNames_SubSelectWithSameFields() {
        String soql = " select Id, Account.Name, Owner.Id, Owner.Name from Account ";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);
        // Just Name is confusing - see the case
        List<String> expected = Arrays.asList("Id", "Name", "Id", "Name");
        List<String> actual = listFlatFieldNames(analyzer);

        assertEquals(expected, actual);
    }

    @Test
    public void testGetFieldNames_SubSelectWithSameFieldAliases() {
        String soql = " select Id, Account.Name, Owner.Id, Owner.Name from Account ";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);
        // Just Name is confusing - see the case
        List<String> expected = Arrays.asList("Id", "Name", "Owner.Id", "Owner.Name");
        List<String> actual = listFlatFieldAliases(analyzer);

        assertEquals(expected, actual);
    }

    @Test
    public void testGetFieldNames_SelectWithAggregate() {
        String soql = " select Id , Account.Name, count(id)\r\nfrom Contact\r\n where something = 'nothing' ";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);
        List<String> expected = Arrays.asList("Id", "Name", "count");
        List<String> actual = listFlatFieldNames(analyzer);

        assertEquals(expected, actual);
    }

    @Test
    public void testGetFromObjectName() {
        String soql = " select Id , Account.Name \r\nfrom Contact\r\n where something = 'nothing' ";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);
        String expected = "Contact";
        String actual = analyzer.getFromObjectName();

        assertEquals(expected, actual);
    }

    @Test
    public void testGetSimpleFieldDefinitions() {
        String soql = "SELECT Id, Name FROM Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(2, actual.size());
        assertEquals("Id", actual.get(0).getName());
        assertEquals("id", actual.get(0).getType());

        assertEquals("Name", actual.get(1).getName());
        assertEquals("string", actual.get(1).getType());

        System.out.println(analyzer.getSoqlQueryString());
    }

    @Test
    public void testGetReferenceFieldDefinitions() {
        String soql = "SELECT Account.Name FROM Contact";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(1, actual.size());
        assertEquals("Name", actual.get(0).getName());
        assertEquals("string", actual.get(0).getType());
    }

    @Test
    public void testGetAggregateFieldDefinition() {
        String soql = "SELECT MIN(Name) FROM Contact";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(1, actual.size());
        assertEquals("MIN", actual.get(0).getAlias());
        assertEquals("string", actual.get(0).getType());
    }

    @Test
    public void testGetAggregateFieldDefinitionWithoutParameter() {
        String soql = "SELECT Count() FROM Contact";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(1, actual.size());
        assertEquals("Count", actual.get(0).getName());
        assertEquals("int", actual.get(0).getType());
    }

    @Test
    public void testGetSimpleFieldWithQualifier() {
        String soql = "SELECT Contact.Id FROM Contact";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(1, actual.size());
        assertEquals("Id", actual.get(0).getName());
        assertEquals("id", actual.get(0).getType());
    }

    @Test
    public void testGetNamedAggregateFieldDefinitions() {
        String soql = "SELECT count(Name) nameCount FROM Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();

        assertEquals(1, actual.size());
        assertEquals("nameCount", actual.get(0).getName());
        assertEquals("int", actual.get(0).getType());
    }

    private DescribeSObjectResult describeSObject(String sObjectType) {
        try {
            String xml = new String(Files.readAllBytes(Paths.get(
                "src/test/resources/" + sObjectType + "_description.xml")));
            XStream xstream = new XStream();

            // clear out existing permissions and set own ones
            xstream.addPermission(NoTypePermission.NONE);
            // allow some basics
            xstream.addPermission(NullPermission.NULL);
            xstream.addPermission(PrimitiveTypePermission.PRIMITIVES);
            xstream.allowTypeHierarchy(Collection.class);

            xstream.addImmutableType(com.sforce.soap.partner.SoapType.class, true);
            xstream.addImmutableType(com.sforce.soap.partner.FieldType.class, true);
            xstream.allowTypesByRegExp(new String[]{".*"});
            Object result = xstream.fromXML(xml);
            if (result instanceof DescribeSObjectResult[]) {
                return ((DescribeSObjectResult[]) result)[0];
            }
            return (DescribeSObjectResult) result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testFetchFieldDefinitions_WithIncludedSelect() {
        String soql = "SELECT Name, (SELECT Id, max(LastName) maxLastName FROM Contacts), Id FROM Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();

        assertEquals(4, actual.size());
        FieldDef fieldDef = actual.get(0);
        assertEquals("Name", fieldDef.getName());
        assertEquals("string", fieldDef.getType());

        fieldDef = actual.get(1);
        assertEquals("Id", fieldDef.getName());
        assertEquals("id", fieldDef.getType());

        fieldDef = actual.get(2);
        assertEquals("maxLastName", fieldDef.getAlias());
        assertEquals("string", fieldDef.getType());

        fieldDef = actual.get(3);
        assertEquals("Id", fieldDef.getName());
        assertEquals("id", fieldDef.getType());
    }

    @Test
    public void testFetchFieldDefinitions_Star() {
        String soql = "SELECT * FROM Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();

        assertEquals(66, actual.size());
        FieldDef fieldDef = actual.get(0);
        assertEquals("Id", fieldDef.getName());
        assertEquals("id", fieldDef.getType());
    }

    @Test
    public void testIncludes() {
        String soql = "SELECT Account.Name FROM Contact where option includes ('option 1')";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(1, actual.size());
    }

    @Test
    public void testRelationFieldDefinitions() {
        String soql = """
            select Id, Name, Status, CreatedDate, OrderNumber, Order_Id__c,
              Activation_Key__c , Zuora_Quote__r.zqu__Status__c,
              Zuora_Quote__c , RecordTypeId, Terminal_Class__c,
              Zuora_Quote__r.Customer_Subscription__r.OSS_Technical_Account_ID__c,
              Order.Zuora_Quote__r.Customer_Subscription__r.Name
            from Order
        """;
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        FieldDefTree fieldDefinitions = analyzer.getFieldDefinitions();
        System.out.println(fieldDefinitions.toTree());
        List<FieldDef> actual = fieldDefinitions.flatten();

        assertEquals(13, actual.size());
        FieldDef fieldDef = actual.get(0);
        assertEquals("Id", fieldDef.getName());
        assertEquals("id", fieldDef.getType());

        fieldDef = actual.get(1);
        assertEquals("Name", fieldDef.getName());
        assertEquals("string", fieldDef.getType());

        fieldDef = actual.get(2);
        assertEquals("Status", fieldDef.getAlias());
        assertEquals("picklist", fieldDef.getType());

        fieldDef = actual.get(3);
        assertEquals("CreatedDate", fieldDef.getName());
        assertEquals("datetime", fieldDef.getType());

        fieldDef = actual.get(4);
        assertEquals("OrderNumber", fieldDef.getName());
        assertEquals("string", fieldDef.getType());

        fieldDef = actual.get(5);
        assertEquals("Order_Id__c", fieldDef.getName());
        assertEquals("string", fieldDef.getType());

        fieldDef = actual.get(6);
        assertEquals("Activation_Key__c", fieldDef.getName());
        assertEquals("string", fieldDef.getType());

        fieldDef = actual.get(7);
        assertEquals("zqu__Status__c", fieldDef.getName());
        assertEquals("picklist", fieldDef.getType());

        fieldDef = actual.get(8);
        assertEquals("OSS_Technical_Account_ID__c", fieldDef.getName());
        assertEquals("string", fieldDef.getType());

        fieldDef = actual.get(9);
        assertEquals("Name", fieldDef.getName());
        assertEquals("string", fieldDef.getType());

        fieldDef = actual.get(10);
        assertEquals("Zuora_Quote__c", fieldDef.getName());
        assertEquals("reference", fieldDef.getType());

        fieldDef = actual.get(11);
        assertEquals("RecordTypeId", fieldDef.getName());
        assertEquals("reference", fieldDef.getType());

        fieldDef = actual.get(12);
        assertEquals("Terminal_Class__c", fieldDef.getName());
        assertEquals("picklist", fieldDef.getType());

    }

    @Test
    public void testAggregateFunctions_SUM() {
        String soql = "SELECT SUM(NumberOfEmployees) FROM Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(1, actual.size());
        assertEquals("SUM", actual.get(0).getAlias());
        assertEquals("_int", actual.get(0).getType());
    }

    @Test
    public void testAggregateFunctions_AVG() {
        String soql = "SELECT AVG(NumberOfEmployees) FROM Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(1, actual.size());
        assertEquals("AVG", actual.get(0).getAlias());
        assertEquals("_int", actual.get(0).getType());
    }

    @Test
    public void testAggregateFunctions_MAX() {
        String soql = "SELECT MAX(CreatedDate) FROM Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(1, actual.size());
        assertEquals("MAX", actual.get(0).getAlias());
        assertEquals("datetime", actual.get(0).getType());
    }

    @Test
    public void testDateFunctions_CALENDAR_MONTH() {
        String soql = "SELECT CALENDAR_MONTH(CreatedDate) FROM Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(1, actual.size());
        assertEquals("CALENDAR_MONTH", actual.get(0).getAlias());
        assertEquals("int", actual.get(0).getType());
    }

    @Test
    public void testDateFunctions_DAY_IN_MONTH() {
        String soql = "SELECT DAY_IN_MONTH(CreatedDate) FROM Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(1, actual.size());
        assertEquals("DAY_IN_MONTH", actual.get(0).getAlias());
        assertEquals("int", actual.get(0).getType());
    }

    @Test
    public void testDateFunctions_FISCAL_QUARTER() {
        String soql = "SELECT FISCAL_QUARTER(CreatedDate) FROM Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(1, actual.size());
        assertEquals("FISCAL_QUARTER", actual.get(0).getAlias());
        assertEquals("int", actual.get(0).getType());
    }

    @Test
    public void testDateFunctions_HOUR_IN_DAY() {
        String soql = "SELECT HOUR_IN_DAY(CreatedDate) FROM Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(1, actual.size());
        assertEquals("HOUR_IN_DAY", actual.get(0).getAlias());
        assertEquals("int", actual.get(0).getType());
    }

    @Test
    public void testAggregateFunctions_COUNT_DISTINCT() {
        String soql = "SELECT COUNT_DISTINCT(Name) FROM Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(1, actual.size());
        assertEquals("COUNT_DISTINCT", actual.get(0).getAlias());
        assertEquals("int", actual.get(0).getType());
    }

    @Test
    public void testCoalesceInWhere_BasicComparison() {
        String soql = "SELECT Name FROM Account WHERE COALESCE(Phone, Fax) = '12345'";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        String result = analyzer.getSoqlQueryString();
        assertTrue(result.contains("WHERE ((Phone = '12345') OR (Phone = NULL AND Fax = '12345'))"), 
            "Query should be rewritten: " + result);
    }

    @Test
    public void testCoalesceInWhere_WithLiteralTrue() {
        String soql = "SELECT Name FROM Account WHERE COALESCE(Phone, Fax, '12345') = '12345'";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        String result = analyzer.getSoqlQueryString();
        assertTrue(result.contains("WHERE ((Phone = '12345') OR (Phone = NULL AND Fax = '12345') OR (Phone = NULL AND Fax = NULL))"), 
            "Query should be rewritten: " + result);
    }

    @Test
    public void testCoalesceInWhere_WithLiteralFalse() {
        String soql = "SELECT Name FROM Account WHERE COALESCE(Phone, Fax, 'N/A') = '12345'";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        String result = analyzer.getSoqlQueryString();
        assertTrue(result.contains("WHERE ((Phone = '12345') OR (Phone = NULL AND Fax = '12345'))"), 
            "Query should be rewritten: " + result);
        assertFalse(result.contains("N/A"), "Falsy constant branch should be omitted: " + result);
    }

    @Test
    public void testCoalesceInWhere_IsNull() {
        String soql = "SELECT Name FROM Account WHERE COALESCE(Phone, Fax) IS NULL";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        String result = analyzer.getSoqlQueryString();
        assertTrue(result.contains("WHERE (Phone = NULL AND Fax = NULL)"), 
            "Query should be rewritten: " + result);
    }

    @Test
    public void testCoalesceInWhere_IsNotNull() {
        String soql = "SELECT Name FROM Account WHERE COALESCE(Phone, Fax) IS NOT NULL";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        String result = analyzer.getSoqlQueryString();
        assertTrue(result.contains("WHERE (Phone <> NULL OR Fax <> NULL)"), 
            "Query should be rewritten: " + result);
    }

    @Test
    public void testCoalesceInWhere_WithParameters() {
        String soql = "SELECT Name FROM Account WHERE COALESCE(Phone, Fax, '12345') = ?";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        // Test with parameter matching the literal '12345'
        String resultTrue = analyzer.getSoqlQueryString(List.of("12345"));
        assertTrue(resultTrue.contains("WHERE ((Phone = ?) OR (Phone = NULL AND Fax = ?) OR (Phone = NULL AND Fax = NULL))"), 
            "Query should include the true literal branch: " + resultTrue);

        // Test with parameter NOT matching the literal '12345' (e.g. '99999')
        String resultFalse = analyzer.getSoqlQueryString(List.of("99999"));
        assertTrue(resultFalse.contains("WHERE ((Phone = ?) OR (Phone = NULL AND Fax = ?))"), 
            "Query should omit the falsy literal branch: " + resultFalse);
        assertFalse(resultFalse.contains("NULL AND NULL"), "Should not contain null-fallback branch: " + resultFalse);
    }

    @Test
    public void testStandardIsNullInWhere() {
        String soql = "SELECT Name FROM Account WHERE Phone IS NULL";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        String result = analyzer.getSoqlQueryString();
        assertTrue(result.contains("WHERE Phone = NULL"), 
            "Query should rewrite IS NULL to = NULL: " + result);
    }

    @Test
    public void testStandardIsNotNullInWhere() {
        String soql = "SELECT Name FROM Account WHERE Phone IS NOT NULL";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        String result = analyzer.getSoqlQueryString();
        assertTrue(result.contains("WHERE Phone <> NULL"), 
            "Query should rewrite IS NOT NULL to <> NULL: " + result);
    }

    @Test
    public void testCaseExpressionInSelect() {
        String soql = "SELECT CASE WHEN Amount > 1000 THEN 'BIG' ELSE 'SMALL' END AS val FROM Opportunity";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        String result = analyzer.getSoqlQueryString();
        assertTrue(result.contains("SELECT Amount FROM Opportunity"), 
            "Query should rewrite CASE select to retrieve physical columns: " + result);
    }

    @Test
    public void testEmulatedFunctionInSelect() {
        String soql = "SELECT UPPER(Name) FROM Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        String result = analyzer.getSoqlQueryString();
        assertTrue(result.contains("SELECT Name FROM Account"),
            "Query should rewrite UPPER(Name) to Name: " + result);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(2, actual.size());
        assertEquals("UPPER", actual.get(0).getAlias());
        assertEquals("string", actual.get(0).getType());
        assertEquals("Name", actual.get(1).getName());
        assertEquals("string", actual.get(1).getType());
    }

    @Test
    public void testTrimFunctionInSelect() {
        String soql = "SELECT TRIM(Name) FROM Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        String result = analyzer.getSoqlQueryString();
        assertTrue(result.contains("SELECT Name FROM Account"),
            "Query should rewrite TRIM(Name) to Name: " + result);

        List<FieldDef> actual = analyzer.getFieldDefinitions().flatten();
        assertEquals(2, actual.size());
        assertEquals("trim", actual.get(0).getAlias());
        assertEquals("string", actual.get(0).getType());
        assertEquals("Name", actual.get(1).getName());
        assertEquals("string", actual.get(1).getType());
    }

    @Test
    public void testEmulatedFunctionInWhere() {
        String soql = "SELECT Id FROM Account WHERE UPPER(Name) = 'TEST'";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, partnerService);
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(queryAnalyzer);

        String result = analyzer.getSoqlQueryString();
        assertFalse(result.toUpperCase().contains("WHERE"), "Query should clear WHERE clause: " + result);
        assertTrue(result.contains("Name"), "Query should add Name to SELECT list: " + result);
        assertTrue(result.contains("Id"), "Query should keep Id in SELECT list: " + result);

        it.rotaliano.jdbc.salesforce.expression.Expression clientSideExpr = analyzer.getClientSideWhereExpression();
        
        it.rotaliano.jdbc.salesforce.expression.RowContext trueContext = columnName -> {
            if ("Name".equalsIgnoreCase(columnName)) return "test";
            return null;
        };
        assertEquals(true, clientSideExpr.evaluate(trueContext));

        it.rotaliano.jdbc.salesforce.expression.RowContext falseContext = columnName -> {
            if ("Name".equalsIgnoreCase(columnName)) return "other";
            return null;
        };
        assertEquals(false, clientSideExpr.evaluate(falseContext));
    }
}
