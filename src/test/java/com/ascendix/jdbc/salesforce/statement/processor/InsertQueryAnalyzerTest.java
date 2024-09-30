package com.ascendix.jdbc.salesforce.statement.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class InsertQueryAnalyzerTest {

    @Test
    public void testProcessInsert_ValuesOne() {
        String soql = "insert into Account(Name, OwnerId, Title) values ('FirstAccount', '005xx1231231233123', Null)";

        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);
        InsertQueryAnalyzer analyzer = new InsertQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        // Verify we have exactly one rec to save
        assertEquals(1, analyzer.getRecords(List.of()).size());
        Map<String, Object> rec = analyzer.getRecords(List.of()).get(0);
        // Verify the fields count for the first rec
        assertEquals(3, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "OwnerId", "Title"), rec.keySet());
        assertEquals("FirstAccount", rec.get("Name"));
        assertEquals("005xx1231231233123", rec.get("OwnerId"));
        assertTrue(rec.containsKey("Title"));
        assertNull(rec.get("Title"));
    }

    @Test
    public void testProcessInsert_ValuesOneSubSelect() {
        String soql = "insert into Account(Name, OwnerId) values ('FirstAccount', " +
                " (SELECT Id from User where Name='CollectionOwner-f CollectionOwner-l' LIMIT 1) " +
                ")";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, (subSoql, parameter) -> {
            if ("SELECT Id FROM User WHERE Name = 'CollectionOwner-f CollectionOwner-l' LIMIT 1".equals(subSoql)) {
                Map<String, Object> rec = new HashMap<>();
                rec.put("id", "005xx1231231233123");
                return List.of(rec);
            }
            return List.of();
        }, null);
        InsertQueryAnalyzer analyzer = new InsertQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        // Verify we have exactly one rec to save
        assertEquals(1, analyzer.getRecords(List.of()).size());
        Map<String, Object> rec = analyzer.getRecords(List.of()).get(0);
        // Verify the fields count for the first rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "OwnerId"), rec.keySet());
        assertEquals("FirstAccount", rec.get("Name"));
        assertEquals("005xx1231231233123", rec.get("OwnerId"));
    }

    @Test
    public void testProcessInsert_ValuesTwo() {
        String soql = "insert into Account(Name, OwnerId) values ('FirstAccount', '005xx1111111111111'),  ('SecondAccount', '005xx2222222222222')";

        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);
        InsertQueryAnalyzer analyzer = new InsertQueryAnalyzer(queryAnalyzer);

        assertEquals("Account", analyzer.getFromObjectName());

        // Verify we have exactly one rec to save
        assertEquals(2, analyzer.getRecords(List.of()).size());

        // Verify the first rec
        Map<String, Object> rec = analyzer.getRecords(List.of()).get(0);
        // Verify the fields count for the first rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "OwnerId"), rec.keySet());
        assertEquals("FirstAccount", rec.get("Name"));
        assertEquals("005xx1111111111111", rec.get("OwnerId"));

        // Verify the second rec
        rec = analyzer.getRecords(List.of()).get(1);
        // Verify the fields count for the second rec
        assertEquals(2, rec.size());
        // Verify the fields' names for the first rec
        assertEquals(Set.of("Name", "OwnerId"), rec.keySet());
        assertEquals("SecondAccount", rec.get("Name"));
        assertEquals("005xx2222222222222", rec.get("OwnerId"));
    }
}
