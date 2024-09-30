package com.ascendix.jdbc.salesforce.statement.processor;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class QueryAnalyzerTest {

    @Test
    void testIsInsertQuery() {
        String soql = "insert into Account(Name, OwnerId) values ('FirstAccount', '005xx1231231233123')";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);

        assertTrue(queryAnalyzer.analyse(soql, StatementTypeEnum.INSERT));
    }

    @Test
    void testIsUpdateQuery() {
        String soql = "Update Account set Name ='FirstAccount_new' where Id='001xx000003GeY0AAK'";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);

        assertTrue(queryAnalyzer.analyse(soql, StatementTypeEnum.UPDATE));
    }

    @Test
    void testIsSelectQuery() {
        String soql = "Select Id from Account";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);

        assertTrue(queryAnalyzer.analyse(soql, StatementTypeEnum.SELECT));
    }

    @Test
    void testIsDeleteQuery() {
        String soql = "Delete from Account where id='001xx000003GeY0AAK'";
        final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(soql, null, null);

        assertTrue(queryAnalyzer.analyse(soql, StatementTypeEnum.DELETE));
    }
}
