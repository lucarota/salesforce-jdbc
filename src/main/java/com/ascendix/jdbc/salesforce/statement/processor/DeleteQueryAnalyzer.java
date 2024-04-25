package com.ascendix.jdbc.salesforce.statement.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.SelectUtils;

@Slf4j
public class DeleteQueryAnalyzer {

    private final QueryAnalyzer queryAnalyzer;
    private List<String> records;

    public DeleteQueryAnalyzer(QueryAnalyzer queryAnalyzer) {
        this.queryAnalyzer = queryAnalyzer;
    }

    public List<String> getRecords(List<Object> parameters) {
        if (queryAnalyzer.getQueryData() != null && records == null) {
            records = new ArrayList<>();
            final Delete query = (Delete) queryAnalyzer.getQueryData();
            String id = queryAnalyzer.checkIsDirectIdWhere(query.getWhere(), parameters);
            if (id != null) {
                records.add(id);

                return records;
            }
            // otherwise we need to fetch all the Entity Ids applicable to this WHERE condition and then build a records using these Ids to delete
            if (queryAnalyzer.getSubSelectResolver() != null) {
                try {
                    Select select = SelectUtils.buildSelectFromTableAndExpressions(query.getTable(),"Id");
                    select.getPlainSelect().setWhere(query.getWhere());

                    List<Map<String, Object>> subRecords = queryAnalyzer.getSubSelectResolver().apply(select.toString());

                    for (Map<String, Object> subRecord : subRecords) {
                        // this subRecord is LinkedHashMap - so the order of fields is determined by soql
                        records.add((String) subRecord.get("Id"));
                    }
                } catch (JSQLParserException e) {
                    log.error(
                        "Failed request to fetch the applicable entities: error in columns to fetch",
                        e);
                }
            } else {
                log.error(
                    "Failed request to fetch the applicable entities: subSelectResolver not defined");
            }
        }
        return records;
    }

    public String getFromObjectName() {
        return queryAnalyzer.getFromObjectName();
    }
}
