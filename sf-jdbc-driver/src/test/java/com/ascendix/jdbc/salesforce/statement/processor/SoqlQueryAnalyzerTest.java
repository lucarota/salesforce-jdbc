package com.ascendix.jdbc.salesforce.statement.processor;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.statement.FieldDef;
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
import org.junit.Test;

public class SoqlQueryAnalyzerTest {

    private final PartnerService partnerService;

    public SoqlQueryAnalyzerTest() {
        partnerService = mock(PartnerService.class);
        when(partnerService.describeSObject(eq("Account"))).thenReturn(describeSObject("Account"));
        when(partnerService.describeSObject(eq("Contact"))).thenReturn(describeSObject("Contact"));
        when(partnerService.describeSObject(eq("User"))).thenReturn(describeSObject("User"));
    }

    @Test
    public void testGetFieldNames_SimpleQuery() {
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(" select Id ,Name \r\nfrom Account\r\n where something = 'nothing' ",
            partnerService);
        List<String> expected = Arrays.asList("Id", "Name");
        List<String> actual = listFlatFieldNames(analyzer);

        assertEquals(expected, actual);
    }

    private List<String> listFlatFieldNames(SoqlQueryAnalyzer analyzer) {
        return analyzer.getFieldDefinitions().stream()
                .map(FieldDef::getName)
                .collect(Collectors.toList());
    }

    private List<String> listFlatFieldAliases(SoqlQueryAnalyzer analyzer) {
        return analyzer.getFieldDefinitions().stream()
                .map(FieldDef::getAlias)
                .collect(Collectors.toList());
    }

    @Test
    public void testGetFieldNames_SelectWithReferences() {
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(" select Id , Account.Name \r\nfrom Contact\r\n where something = 'nothing' ",
            partnerService);
        List<String> expected = Arrays.asList("Id", "Name");
        List<String> actual = listFlatFieldNames(analyzer);

        assertEquals(expected, actual);
    }

    @Test
    public void testGetFieldNames_SelectWithAggregateAliased() {
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(" select Id , Account.Name, count(id) aggrAlias1\r\nfrom Contact\r\n where something = 'nothing' ",
            partnerService);
        // Just Name is confusing - see the case
        List<String> expected = Arrays.asList("Id", "Name", "aggrAlias1");
        List<String> actual = listFlatFieldNames(analyzer);

        assertEquals(expected, actual);
    }

    @Test
    public void testGetFieldNames_SubSelectWithSameFields() {
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(" select Id, Account.Name, Owner.Id, Owner.Name from Account ",
            partnerService);
        // Just Name is confusing - see the case
        List<String> expected = Arrays.asList("Id", "Name", "Id", "Name");
        List<String> actual = listFlatFieldNames(analyzer);

        assertEquals(expected, actual);
    }

    @Test
    public void testGetFieldNames_SubSelectWithSameFieldAliases() {
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(" select Id, Account.Name, Owner.Id, Owner.Name from Account ",
            partnerService);
        // Just Name is confusing - see the case
        List<String> expected = Arrays.asList("Id", "Name", "Owner.Id", "Owner.Name");
        List<String> actual = listFlatFieldAliases(analyzer);

        assertEquals(expected, actual);
    }

    @Test
    public void testGetFieldNames_SelectWithAggregate() {
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(" select Id , Account.Name, count(id)\r\nfrom Contact\r\n where something = 'nothing' ",
            partnerService);
        List<String> expected = Arrays.asList("Id", "Name", "count");
        List<String> actual = listFlatFieldNames(analyzer);

        assertEquals(expected, actual);
    }

    @Test
    public void testGetFromObjectName() {
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer(" select Id , Account.Name \r\nfrom Contact\r\n where something = 'nothing' ",
            partnerService);
        String expected = "Contact";
        String actual = analyzer.getFromObjectName();

        assertEquals(expected, actual);
    }

    @Test
    public void testGetSimpleFieldDefinitions() {
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer("SELECT Id, Name FROM Account", partnerService);

        List<FieldDef> actual = analyzer.getFieldDefinitions();
        assertEquals(2, actual.size());
        assertEquals("Id", actual.get(0).getName());
        assertEquals("id", actual.get(0).getType());

        assertEquals("Name", actual.get(1).getName());
        assertEquals("string", actual.get(1).getType());

        System.out.println(analyzer.getSoqlQuery());
    }

    @Test
    public void testGetReferenceFieldDefinitions() {
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer("SELECT Account.Name FROM Contact", partnerService);

        List<FieldDef> actual = analyzer.getFieldDefinitions();
        assertEquals(1, actual.size());
        assertEquals("Name", actual.get(0).getName());
        assertEquals("string", actual.get(0).getType());
    }

    @Test
    public void testGetAggregateFieldDefinition() {
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer("SELECT MIN(Name) FROM Contact", partnerService);

        List<FieldDef> actual = analyzer.getFieldDefinitions();
        assertEquals(1, actual.size());
        assertEquals("MIN", actual.get(0).getAlias());
        assertEquals("string", actual.get(0).getType());
    }

    @Test
    public void testGetAggregateFieldDefinitionWithoutParameter() {
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer("SELECT Count() FROM Contact", partnerService);

        List<FieldDef> actual = analyzer.getFieldDefinitions();
        assertEquals(1, actual.size());
        assertEquals("Count", actual.get(0).getName());
        assertEquals("int", actual.get(0).getType());
    }

    @Test
    public void testGetSimpleFieldWithQualifier() {
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer("SELECT Contact.Id FROM Contact", partnerService);

        List<FieldDef> actual = analyzer.getFieldDefinitions();
        assertEquals(1, actual.size());
        assertEquals("Id", actual.get(0).getName());
        assertEquals("id", actual.get(0).getType());
    }

    @Test
    public void testGetNamedAggregateFieldDefinitions() {
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer("SELECT count(Name) nameCount FROM Account",
            partnerService);

        List<FieldDef> actual = analyzer.getFieldDefinitions();

        assertEquals(1, actual.size());
        assertEquals("nameCount", actual.get(0).getName());
        assertEquals("int", actual.get(0).getType());
    }

    private DescribeSObjectResult describeSObject(String sObjectType) {
        try {
            String xml = new String(Files.readAllBytes(Paths.get("src/test/resources/" + sObjectType + "_desription.xml")));
            XStream xstream = new XStream();

            // clear out existing permissions and set own ones
            xstream.addPermission(NoTypePermission.NONE);
            // allow some basics
            xstream.addPermission(NullPermission.NULL);
            xstream.addPermission(PrimitiveTypePermission.PRIMITIVES);
            xstream.allowTypeHierarchy(Collection.class);

            xstream.addImmutableType(com.sforce.soap.partner.SoapType.class, true);
            xstream.addImmutableType(com.sforce.soap.partner.FieldType.class, true);
            xstream.allowTypesByRegExp(new String[] { ".*" });
            return (DescribeSObjectResult) xstream.fromXML(xml);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testFetchFieldDefinitions_WithIncludedSelect() {
        SoqlQueryAnalyzer analyzer = new SoqlQueryAnalyzer("SELECT Name, (SELECT Id, max(LastName) maxLastName FROM Contacts), Id FROM Account",
            partnerService);

        List<FieldDef> actual = analyzer.getFieldDefinitions();

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

}
