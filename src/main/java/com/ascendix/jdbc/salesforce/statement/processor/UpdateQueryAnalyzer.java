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
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.JdbcParameter;
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

    public List<Map<String, Object>> getRecords(List<Object> parameters) {
        if (queryAnalyzer.getQueryData() != null && records == null) {
            records = new ArrayList<>();
            final BiFunction<String, List<Object>, List<Map<String, Object>>> subSelectResolver = queryAnalyzer.getSubSelectResolver();

            final Update query = (Update) queryAnalyzer.getQueryData();
            String id = queryAnalyzer.getIdValue(query.getWhere(), parameters);
            List<UpdateSet> updateSets = query.getUpdateSets();
            if (id != null) {
                Set<String> columnsToFetch = new HashSet<>();
                boolean isFunctionFound = findSubFields(columnsToFetch, updateSets);
                if (columnsToFetch.isEmpty() && !isFunctionFound) {
                    populateRecordsById(parameters, id, updateSets, subSelectResolver);
                }
            }
            // otherwise we need to fetch all the Entity Ids applicable to this WHERE condition and then build a records using these Ids with fields to update
            if (subSelectResolver != null) {
                populateRecordsBySubSelect(parameters, updateSets, query, subSelectResolver);
            } else {
                log.error("Failed request to fetch the applicable entities: subSelectResolver not defined");
            }
        }
        return records;
    }

    private void populateRecordsBySubSelect(final List<Object> parameters, final List<UpdateSet> updateSets,
        final Update query, final BiFunction<String, List<Object>, List<Map<String, Object>>> subSelectResolver) {
        try {
            Set<String> columnsToFetch = new HashSet<>();
            findSubFields(columnsToFetch, updateSets);
            columnsToFetch.add("Id");
            Select select = SelectUtils.buildSelectFromTableAndExpressions(query.getTable(),
                columnsToFetch.toArray(new String[]{}));

            select.getPlainSelect().setWhere(query.getWhere());
            List<Object> whereParameters = getWhereParameters(parameters, query);
            List<Map<String, Object>> subRecords = subSelectResolver.apply(select.toString(), whereParameters);

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
                        expr.accept(new UpdateRecordVisitor(rec, subRecord, columns.get(i).getColumnName(), parameters));
                    }
                }
            }
        } catch (JSQLParserException e) {
            log.error("Failed request to fetch the applicable entities: error in columns to fetch", e);
        }
    }

    private static List<Object> getWhereParameters(final List<Object> parameters, final Update query) {
        List<Object> whereParameters = new ArrayList<>();
        query.getWhere().accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(JdbcParameter parameter) {
                int idx = parameter.getIndex() - 1;
                Object o = parameters.get(idx);
                whereParameters.add(o);
            }
        });
        return whereParameters;
    }

    private void populateRecordsById(final List<Object> parameters, final String id,
        final List<UpdateSet> updateSets,

        final BiFunction<String, List<Object>, List<Map<String, Object>>> subSelectResolver) {
        // means there is no calculations in the new field values
        Map<String, Object> rec = new HashMap<>();
        records.add(rec);
        rec.put("Id", id);

        for (UpdateSet updateSet : updateSets) {
            List<Column> columns = updateSet.getColumns();
            for (Column column : columns) {
                updateSet.getValues().accept(new ValueToStringVisitor(rec, column.getColumnName(), parameters,
                    subSelectResolver));
            }
        }
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
