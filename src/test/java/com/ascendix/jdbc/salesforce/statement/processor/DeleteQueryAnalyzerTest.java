package com.ascendix.jdbc.salesforce.statement.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.statement.processor.utils.RecordFieldsBuilder;
import com.sforce.soap.partner.DescribeSObjectResult;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeleteQueryAnalyzerTest {

    DeleteQueryAnalyzerTest() {
        PartnerService partnerService = mock(PartnerService.class);
        when(partnerService.describeSObject("Account")).thenReturn(describeSObject("Account"));
    }

    private DescribeSObjectResult describeSObject(String objName) {
        DescribeSObjectResult result = new DescribeSObjectResult();
        result.setName(objName);
        return result;
    }

    @Test
    void testProcessDelete_One_ById() {
        String soql = "Delete from Account where Id='001xx000003GeY0AAK'";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);
        DeleteQueryAnalyzer analyzer = new DeleteQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        List<String> rec = analyzer.getRecords(List.of());
        // Verify we have exactly one rec to delete
        assertEquals(1, rec.size());
        // Verify the fields' id for the first rec
        assertEquals("001xx000003GeY0AAK", rec.get(0));
    }

    @Test
    void testProcessDelete_One_ByName() {
        String soql = "Delete from Account where Name='FirstAccount_new'";

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

        DeleteQueryAnalyzer analyzer = new DeleteQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        List<String> rec = analyzer.getRecords(List.of());
        // Verify we have exactly three rec to delete
        assertEquals(3, rec.size());
        // Verify the fields' id
        assertEquals("005xx1111111111111", rec.get(0));
        assertEquals("005xx2222222222222", rec.get(1));
        assertEquals("005xx3333333333333", rec.get(2));
    }


    @Test
    void testProcessDelete_One_NoId() {
        String soql = "Delete from Account";
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
        DeleteQueryAnalyzer analyzer = new DeleteQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        List<String> rec = analyzer.getRecords(List.of());
        // Verify we have three records to save
        assertEquals(3, rec.size());
        // Verify the fields' id
        assertEquals("005xx1111111111111", rec.get(0));
        assertEquals("005xx2222222222222", rec.get(1));
        assertEquals("005xx3333333333333", rec.get(2));
    }

    @Test
    void testProcessDelete_One_ByIdInverseWhere() {
        String soql = "Delete from Account where '001xx000003GeY0AAK'=Id";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);
        DeleteQueryAnalyzer analyzer = new DeleteQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        List<String> rec = analyzer.getRecords(List.of());
        // Verify we have exactly one rec to save
        assertEquals(1, rec.size());
        // Verify the fields' id
        assertEquals("001xx000003GeY0AAK", rec.get(0));
    }

    @Test
    void testProcessDelete_One_ByIdJdbcParam() {
        String soql = "Delete from Account where id=?";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);
        DeleteQueryAnalyzer analyzer = new DeleteQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        final List<Object> params = List.of("001xx000003GeY0AAK");
        List<String> rec = analyzer.getRecords(params);
        // Verify we have exactly one rec to save
        assertEquals(1, rec.size());
        // Verify the fields' id
        assertEquals("001xx000003GeY0AAK", rec.get(0));
    }

    @Test
    void testProcessDelete_One_ByIdEquals() {
        String soql = "Delete from Account where id=ownerId";
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
        DeleteQueryAnalyzer analyzer = new DeleteQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        List<String> rec = analyzer.getRecords(List.of());
        // Verify we have three records to save
        assertEquals(3, rec.size());
        // Verify the fields' id
        assertEquals("005xx1111111111111", rec.get(0));
        assertEquals("005xx2222222222222", rec.get(1));
        assertEquals("005xx3333333333333", rec.get(2));
    }

    @Test
    void testProcessDelete_One_All() {
        String soql = "Delete from Account where 1=1";
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
        DeleteQueryAnalyzer analyzer = new DeleteQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        List<String> rec = analyzer.getRecords(List.of());
        // Verify we have three records to save
        assertEquals(3, rec.size());
        // Verify the fields' id
        assertEquals("005xx1111111111111", rec.get(0));
        assertEquals("005xx2222222222222", rec.get(1));
        assertEquals("005xx3333333333333", rec.get(2));
    }
}
