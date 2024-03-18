package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.statement.ForcePreparedStatement;
import com.ascendix.jdbc.salesforce.statement.processor.utils.ColumnsFinderVisitor;
import com.ascendix.jdbc.salesforce.statement.processor.utils.ValueToStringVisitor;
import com.sforce.soap.partner.DescribeSObjectResult;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import net.sf.jsqlparser.util.SelectUtils;

import java.sql.ResultSet;
import java.util.*;
import java.util.function.Function;

@Slf4j
public class SoslQueryProcessor {

    private String soql;
    private final Function<String, DescribeSObjectResult> objectDescriptor;
    private final Map<String, DescribeSObjectResult> describedObjectsCache;
    private final Function<String, List<Map<String, Object>>> subSelectResolver;
    private Update queryData;
    private List<Map<String, Object>> records;

    public SoslQueryProcessor(String soql, Function<String, DescribeSObjectResult> objectDescriptor) {
        this(soql, objectDescriptor, new HashMap<>(), null);
    }

    public SoslQueryProcessor(String soql,
        Function<String, DescribeSObjectResult> objectDescriptor,
        Map<String, DescribeSObjectResult> describedObjectsCache,
        Function<String, List<Map<String, Object>>> subSelectResolver) {
        this.soql = soql;
        this.objectDescriptor = objectDescriptor;
        this.describedObjectsCache = describedObjectsCache;
        this.subSelectResolver = subSelectResolver;
    }

    public static boolean isSoslQuery(String soqlQuery, SoslQueryAnalyzer queryAnalyzer) {
        if (soqlQuery == null || soqlQuery.trim().isEmpty()) {
            return false;
        }
        soqlQuery = soqlQuery.trim();

        return queryAnalyzer.analyse(soqlQuery);
    }

    public static ResultSet processQuery(ForcePreparedStatement forcePreparedStatement, String soqlQuery,
        PartnerService partnerService, SoslQueryAnalyzer soslQueryAnalyzer) {
        return null;
    }

    public boolean analyse(String soql) {
        if (soql == null || soql.trim().isEmpty()) {
            return false;
        }
        this.soql = soql;
        return getQueryData() != null;
    }

    protected String getFromObjectName() {
        return queryData.getTable().getName();
    }

    private Update getQueryData() {
        if (queryData == null) {
            try {
                Statement statement = CCJSqlParserUtil.parse(soql);
                if (statement instanceof Update) {
                    queryData = (Update) statement;
                }
            } catch (JSQLParserException e) {
                log.error("Failed request to create entities with error: {}", e.getMessage(), e);
            }
        }
        return queryData;
    }

    public List<Map<String, Object>> getRecords() {
        if (queryData != null && records == null) {
            records = new ArrayList<>();

            String id = checkIsDirectIdWhere();
            List<UpdateSet> updateSets = getQueryData().getUpdateSets();
            if (id != null) {
                Set<String> columnsToFetch = new HashSet<>();
                boolean isFunctionFound = findSubFields(columnsToFetch, updateSets);
                if (columnsToFetch.isEmpty() && !isFunctionFound) {
                    // means there is no calculations in the new field values
                    Map<String, Object> record = new HashMap<>();
                    records.add(record);
                    record.put("Id", id);

                    for (UpdateSet updateSet : updateSets) {
                        List<Column> columns = updateSet.getColumns();
                        for (Column column : columns) {
                            updateSet.getValues().accept(new ValueToStringVisitor(record,column.getColumnName(),
                                    subSelectResolver)
                            );
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
                    Select select = SelectUtils.buildSelectFromTableAndExpressions(getQueryData().getTable(),
                        columnsToFetch.toArray(new String[]{}));
                    select.getPlainSelect().setWhere(getQueryData().getWhere());

                    List<Map<String, Object>> subRecords = subSelectResolver.apply(select.toString());

                    for (Map<String, Object> subRecord : subRecords) {
                        // this subRecord is LinkedHashMap - so the order of fields is determined by soql
                        Map<String, Object> record = new HashMap<>();
                        records.add(record);
                        record.put("Id", subRecord.get("Id"));

                        for (UpdateSet updateSet : updateSets) {
                            List<Column> columns = updateSet.getColumns();
                            for (Column column : columns) {
                                updateSet.getValues().accept(new ValueToStringVisitor(record,column.getColumnName(),
                                        subSelectResolver)
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

    /**
     * Checks if this update is using WHERE Id='001xx010201' notation and no other criteria
     */
    private String checkIsDirectIdWhere() {
        if (queryData.getWhere() != null && queryData.getWhere() instanceof final EqualsTo whereRoot) {
            // direct ID comparison like Id='001xx192918212'
            if (whereRoot.getLeftExpression() instanceof final Column col
                && whereRoot.getRightExpression() instanceof StringValue) {
                if ("id".equalsIgnoreCase(col.getColumnName())) {
                    StringValue idValue = (StringValue) whereRoot.getRightExpression();
                    return idValue.getValue();
                }
            }
            // direct ID comparison like '001xx192918212'=Id
            if (whereRoot.getLeftExpression() instanceof StringValue
                && whereRoot.getRightExpression() instanceof final Column col) {
                if ("id".equalsIgnoreCase(col.getColumnName())) {
                    StringValue idValue = (StringValue) whereRoot.getLeftExpression();
                    return idValue.getValue();
                }
            }
        }
        return null;
    }
}
