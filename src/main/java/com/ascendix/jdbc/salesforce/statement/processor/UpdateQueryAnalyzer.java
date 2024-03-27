package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.statement.processor.utils.ColumnsFinderVisitor;
import com.ascendix.jdbc.salesforce.statement.processor.utils.UpdateRecordVisitor;
import com.ascendix.jdbc.salesforce.statement.processor.utils.ValueToStringVisitor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import net.sf.jsqlparser.util.SelectUtils;

@Slf4j
public class UpdateQueryAnalyzer {

    private final QueryAnalyzer queryAnalyzer;
    private List<Map<String, Object>> records;

    public UpdateQueryAnalyzer(QueryAnalyzer queryAnalyzer) {
        this.queryAnalyzer = queryAnalyzer;
    }

    public List<Map<String, Object>> getRecords() {
        if (queryAnalyzer.getQueryData() != null && records == null) {
            records = new ArrayList<>();
            final Function<String, List<Map<String, Object>>> subSelectResolver = queryAnalyzer.getSubSelectResolver();

            final Update query = (Update) queryAnalyzer.getQueryData();
            String id = queryAnalyzer.checkIsDirectIdWhere(query.getWhere());
            List<UpdateSet> updateSets = query.getUpdateSets();
            if (id != null) {
                Set<String> columnsToFetch = new HashSet<>();
                boolean isFunctionFound = findSubFields(columnsToFetch, updateSets);
                if (columnsToFetch.isEmpty() && !isFunctionFound) {
                    // means there is no calculations in the new field values
                    Map<String, Object> rec = new HashMap<>();
                    records.add(rec);
                    rec.put("Id", id);

                    for (UpdateSet updateSet : updateSets) {
                        List<Column> columns = updateSet.getColumns();
                        for (Column column : columns) {
                            updateSet.getValues().accept(new ValueToStringVisitor(rec, column.getColumnName(),
                                    subSelectResolver));
                        }

                    }
                    return records;
                }
            }
            // otherwise we need to fetch all the Entity Ids applicable to this WHERE condition and then build a records using these Ids with fields to update
            if (subSelectResolver != null) {
                try {
                    Set<String> columnsToFetch = new HashSet<>();
                    findSubFields(columnsToFetch, updateSets);
                    columnsToFetch.add("Id");
                    Select select = SelectUtils.buildSelectFromTableAndExpressions(query.getTable(),
                        columnsToFetch.toArray(new String[]{}));
                    select.getPlainSelect().setWhere(query.getWhere());

                    List<Map<String, Object>> subRecords = subSelectResolver.apply(select.toString());

                    for (Map<String, Object> subRecord : subRecords) {
                        // this subRecord is LinkedHashMap - so the order of fields is determined by soql
                        Map<String, Object> rec = new HashMap<>();
                        records.add(rec);
                        rec.put("Id", subRecord.get("Id"));

                        // Iterating over the received entities and adding fields to update
                        for (UpdateSet updateSet : updateSets) {
                            List<Column> columns = updateSet.getColumns();
                            for (int i = 0; i < columns.size(); i++) {
                                Expression expr = updateSet.getValue(i);
                                expr.accept(
                                    new UpdateRecordVisitor(rec, subRecord, columns.get(i).getColumnName())
                                );
                            }
                        }

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

    private boolean findSubFields(Set<String> columns, List<UpdateSet> updateSets) {
        ColumnsFinderVisitor visitor = new ColumnsFinderVisitor(columns);
        for (UpdateSet updateSet : updateSets) {
            updateSet.getValues().forEach(expr -> expr.accept(visitor));
        }

        return visitor.isFunctionFound();
    }

    public String getFromObjectName() {
        return queryAnalyzer.getFromObjectName();
    }
}
