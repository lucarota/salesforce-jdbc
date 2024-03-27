package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.statement.processor.utils.InsertValuesVisitor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.Values;

@Slf4j
public class InsertQueryAnalyzer {

    private final QueryAnalyzer queryAnalyzer;
    private List<Map<String, Object>> records;

    public InsertQueryAnalyzer(QueryAnalyzer queryAnalyzer) {
        this.queryAnalyzer = queryAnalyzer;
    }

    public List<Map<String, Object>> getRecords() {
        if (queryAnalyzer.getQueryData() != null && records == null) {
            records = new ArrayList<>();
            final Function<String, List<Map<String, Object>>> subSelectResolver = queryAnalyzer.getSubSelectResolver();
            final Insert query = (Insert) queryAnalyzer.getQueryData();
            Select select = query.getSelect();
            if (select instanceof Values) {
                query.getValues().accept(new InsertValuesVisitor(query.getColumns(), records, subSelectResolver));
            } else if (select != null && subSelectResolver != null) {
                log.info("Insert/Update has a sub-select: {}", select);
                List<Map<String, Object>> subRecords = subSelectResolver.apply(
                        select.getPlainSelect().toString());
                log.info("Insert/Update fetched {} records from a sub-select: {}", subRecords.size(), select);
                for (Map<String, Object> subRecord : subRecords) {
                    // this subRecord is LinkedHashMap - so the order of fields is determined by soql
                    Map<String, Object> rec = new HashMap<>();
                    records.add(rec);
                    int fieldIndex = 0;
                    for (final Map.Entry<String, Object> fieldEntry : subRecord.entrySet()) {
                        String insertFieldName = query.getColumns().get(fieldIndex).getColumnName();
                        Object subSelectFieldValue = fieldEntry.getValue();
                        rec.put(insertFieldName, subSelectFieldValue);

                        fieldIndex++;
                    }
                }
            }
        }
        return records;
    }

    public String getFromObjectName() {
        return queryAnalyzer.getFromObjectName();
    }
}
