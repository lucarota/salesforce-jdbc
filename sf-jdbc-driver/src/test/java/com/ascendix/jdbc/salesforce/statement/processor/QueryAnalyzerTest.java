package com.ascendix.jdbc.salesforce.statement.processor;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class QueryAnalyzerTest {

    @Test
    public void testIsInsertQuery() {
        String soql = "insert into Account(Name, OwnerId) values ('FirstAccount', '005xx1231231233123')";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);

        assertTrue(queryAnalyzer.analyse(soql, StatementTypeEnum.INSERT));
    }

    @Test
    public void testIsUpdateQuery() {
        String soql = "Update Account set Name ='FirstAccount_new' where Id='001xx000003GeY0AAK'";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);

        assertTrue(queryAnalyzer.analyse(soql, StatementTypeEnum.UPDATE));
    }

    @Test
    public void testIsSelectQuery() {
        String soql = "Select Id from Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);

        assertTrue(queryAnalyzer.analyse(soql, StatementTypeEnum.SELECT));
    }

    @Test
    public void testIsDeleteQuery() {
        String soql = "Delete from Account where id='001xx000003GeY0AAK'";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);

        assertTrue(queryAnalyzer.analyse(soql, StatementTypeEnum.DELETE));
    }
}
