package com.ascendix.jdbc.salesforce.statement.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.statement.processor.utils.RecordFieldsBuilder;
import com.sforce.soap.partner.DescribeSObjectResult;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class UpdateQueryAnalyzerTest {

    UpdateQueryAnalyzerTest() {
        PartnerService partnerService = mock(PartnerService.class);
        when(partnerService.describeSObject("Account")).thenReturn(describeSObject("Account"));
    }

    private DescribeSObjectResult describeSObject(String objName) {
        DescribeSObjectResult result = new DescribeSObjectResult();
        result.setName(objName);
        return result;
    }

    @Test
    void testProcessUpdate_One_ById() {
        String soql = "Update Account set Name ='FirstAccount_new' where Id='001xx000003GeY0AAK'";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);
        UpdateQueryAnalyzer analyzer = new UpdateQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        // Verify we have exactly one rec to save
        assertEquals(1, analyzer.getRecords(List.of()).size());
        Map<String, Object> rec = analyzer.getRecords(List.of()).get(0);
        // Verify the fields count for the first rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "Id"), rec.keySet());
        assertEquals("FirstAccount_new", rec.get("Name"));
        assertEquals("001xx000003GeY0AAK", rec.get("Id"));
    }

    @Test
    void testProcessUpdate_One_ByName() {
        String soql = "Update Account set Name ='NEW_AccountName' where Name='FirstAccount_new'";

        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, (subSoql, parameters) -> {
            if ("SELECT Id FROM Account WHERE Name = 'FirstAccount_new'".equals(subSoql)) {
                return Arrays.asList(
                    RecordFieldsBuilder.id("005xx1111111111111"),
                    RecordFieldsBuilder.id("005xx2222222222222"),
                    RecordFieldsBuilder.id("005xx3333333333333")
                );
            }
            return List.of();
        }, null);

        UpdateQueryAnalyzer analyzer = new UpdateQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        // Verify we have exactly three rec to save
        assertEquals(3, analyzer.getRecords(List.of()).size());

        Map<String, Object> rec = analyzer.getRecords(List.of()).get(0);
        // Verify the fields count for the first rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "Id"), rec.keySet());
        assertEquals("NEW_AccountName", rec.get("Name"));
        assertEquals("005xx1111111111111", rec.get("Id"));

        rec = analyzer.getRecords(List.of()).get(1);
        // Verify the fields count for the first rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "Id"), rec.keySet());
        assertEquals("NEW_AccountName", rec.get("Name"));
        assertEquals("005xx2222222222222", rec.get("Id"));

        rec = analyzer.getRecords(List.of()).get(2);
        // Verify the fields count for the first rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "Id"), rec.keySet());
        assertEquals("NEW_AccountName", rec.get("Name"));
        assertEquals("005xx3333333333333", rec.get("Id"));
    }

    @Test
    void testProcessUpdate_One_ByName_CALC() {
        String soql = "Update Account set Name=Name+'-' where Name='FirstAccount_new'";

        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, (subSoql, parameters) -> {
            if ("SELECT Id, Name FROM Account WHERE Name = 'FirstAccount_new'".equals(subSoql)) {
                return Arrays.asList(
                    RecordFieldsBuilder.setId("005xx1111111111111").set("Name", "Acc_01").build(),
                    RecordFieldsBuilder.setId("005xx2222222222222").set("Name", "Acc_02").build(),
                    RecordFieldsBuilder.setId("005xx3333333333333").set("Name", "Acc_03").build()
                );
            }
            return List.of();
        }, null);

        UpdateQueryAnalyzer analyzer = new UpdateQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        // Verify we have exactly three rec to save
        assertEquals(3, analyzer.getRecords(List.of()).size());

        Map<String, Object> rec = analyzer.getRecords(List.of()).get(0);
        // Verify the fields count for the first rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "Id"), rec.keySet());
        assertEquals("Acc_01-", rec.get("Name"));
        assertEquals("005xx1111111111111", rec.get("Id"));

        rec = analyzer.getRecords(List.of()).get(1);
        // Verify the fields count for the first rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "Id"), rec.keySet());
        assertEquals("Acc_02-", rec.get("Name"));
        assertEquals("005xx2222222222222", rec.get("Id"));

        rec = analyzer.getRecords(List.of()).get(2);
        // Verify the fields count for the first rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "Id"), rec.keySet());
        assertEquals("Acc_03-", rec.get("Name"));
        assertEquals("005xx3333333333333", rec.get("Id"));
    }

    @Test
    void testProcessUpdate_One_NoId() {
        String soql = "Update Account set Name = 'FirstAccount_new'";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, (subSoql, parameters) -> {
            if ("SELECT Id FROM Account".equals(subSoql)) {
                return Arrays.asList(
                    RecordFieldsBuilder.id("005xx1111111111111"),
                    RecordFieldsBuilder.id("005xx2222222222222"),
                    RecordFieldsBuilder.id("005xx3333333333333")
                );
            }
            return List.of();
        }, null);
        UpdateQueryAnalyzer analyzer = new UpdateQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        // Verify we have three records to save
        assertEquals(3, analyzer.getRecords(List.of()).size());
        Map<String, Object> rec = analyzer.getRecords(List.of()).get(0);
        // Verify the fields count for the first rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "Id"), rec.keySet());
        assertEquals("FirstAccount_new", rec.get("Name"));
        assertEquals("005xx1111111111111", rec.get("Id"));
    }

    @Test
    void testProcessUpdate_One_ByIdInverseWhere() {
        String soql = "Update Account set Name ='FirstAccount_new' where '001xx000003GeY0AAK'=Id";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);
        UpdateQueryAnalyzer analyzer = new UpdateQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        // Verify we have exactly one rec to save
        assertEquals(1, analyzer.getRecords(List.of()).size());
        Map<String, Object> rec = analyzer.getRecords(List.of()).get(0);
        // Verify the fields count for the first rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "Id"), rec.keySet());
        assertEquals("FirstAccount_new", rec.get("Name"));
        assertEquals("001xx000003GeY0AAK", rec.get("Id"));
    }

    @Test
    void testProcessUpdate_One_ByIdJdbcParam() {
        String soql = "Update Account set Name ='FirstAccount_new' where id=?";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);
        UpdateQueryAnalyzer analyzer = new UpdateQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        // Verify we have exactly one rec to save
        final List<Object> params = List.of("001xx000003GeY0AAK");
        assertEquals(1, analyzer.getRecords(params).size());
        Map<String, Object> rec = analyzer.getRecords(params).get(0);
        // Verify the fields count for the first rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "Id"), rec.keySet());
        assertEquals("FirstAccount_new", rec.get("Name"));
        assertEquals("001xx000003GeY0AAK", rec.get("Id"));
    }

    @Test
    void testProcessUpdate_One_ByIdEquals() {
        String soql = "Update Account set Name ='FirstAccount_new' where id=ownerId";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, (subSoql, parameters) -> {
            if ("SELECT Id FROM Account WHERE id = ownerId".equals(subSoql)) {
                return Arrays.asList(
                    RecordFieldsBuilder.id("005xx1111111111111"),
                    RecordFieldsBuilder.id("005xx2222222222222"),
                    RecordFieldsBuilder.id("005xx3333333333333")
                );
            }
            return List.of();
        }, null);
        UpdateQueryAnalyzer analyzer = new UpdateQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        // Verify we have three records to save
        assertEquals(3, analyzer.getRecords(List.of()).size());
        Map<String, Object> rec = analyzer.getRecords(List.of()).get(0);
        // Verify the fields count for the first rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "Id"), rec.keySet());
        assertEquals("FirstAccount_new", rec.get("Name"));
        assertEquals("005xx1111111111111", rec.get("Id"));
    }

    @Test
    void testProcessUpdate_One_All() {
        String soql = "Update Account set Name ='FirstAccount_new' where 1=1";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, (subSoql, parameters) -> {
            if ("SELECT Id FROM Account WHERE 1 = 1".equals(subSoql)) {
                return Arrays.asList(
                    RecordFieldsBuilder.id("005xx1111111111111"),
                    RecordFieldsBuilder.id("005xx2222222222222"),
                    RecordFieldsBuilder.id("005xx3333333333333")
                );
            }
            return List.of();
        }, null);
        UpdateQueryAnalyzer analyzer = new UpdateQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        // Verify we have three records to save
        assertEquals(3, analyzer.getRecords(List.of()).size());
        Map<String, Object> rec = analyzer.getRecords(List.of()).get(0);
        // Verify the fields count for the first rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "Id"), rec.keySet());
        assertEquals("FirstAccount_new", rec.get("Name"));
        assertEquals("005xx1111111111111", rec.get("Id"));
    }
}
