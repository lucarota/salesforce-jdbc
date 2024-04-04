package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.statement.FieldDef;
import com.ascendix.jdbc.salesforce.statement.processor.utils.SelectSpecVisitor;
import java.util.ArrayList;
import java.util.List;

import com.ascendix.jdbc.salesforce.utils.FieldDefTree;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.statement.select.PlainSelect;

@Slf4j
public class SoqlQueryAnalyzer {

    private final QueryAnalyzer queryAnalyzer;
    private FieldDefTree fieldDefinitions;

    public SoqlQueryAnalyzer(QueryAnalyzer queryAnalyzer) {
        this.queryAnalyzer = queryAnalyzer;
    }

    public String getSoqlQuery() {
        return queryAnalyzer.getQueryData().toString();
    }

    public FieldDefTree getFieldDefinitions() {
        if (fieldDefinitions == null) {
            fieldDefinitions = new FieldDefTree();
            String rootEntityName = queryAnalyzer.getFromObjectName();
            SelectSpecVisitor visitor = new SelectSpecVisitor(rootEntityName, fieldDefinitions, queryAnalyzer.getPartnerService());
            PlainSelect query = (PlainSelect) queryAnalyzer.getQueryData();
            query.getSelectItems().forEach(spec -> spec.accept(visitor));
        }
        return fieldDefinitions;
    }

    public boolean isExpandedStarSyntaxForFields() {
        return queryAnalyzer.isExpandedStarSyntaxForFields();
    }

    public String getFromObjectName() {
        return queryAnalyzer.getFromObjectName();
    }
}
