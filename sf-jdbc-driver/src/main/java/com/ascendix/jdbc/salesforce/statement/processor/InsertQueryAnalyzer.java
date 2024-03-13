package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.statement.processor.utils.ValueToStringVisitor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.SubSelect;

@Slf4j
public class InsertQueryAnalyzer {

    private String soql;
    private final Function<String, List<Map<String, Object>>> subSelectResolver;
    private Insert queryData;
    private List<Map<String, Object>> records;

    public InsertQueryAnalyzer(String soql, Function<String, List<Map<String, Object>>> subSelectResolver) {
        this.soql = soql;
        this.subSelectResolver = subSelectResolver;
    }

    public boolean analyse(String soql) {
        if (soql == null || soql.trim().isEmpty()) {
            return false;
        }
        this.soql = soql;
        return getQueryData(true) != null;
    }

    public class InsertItemsListVisitor implements ItemsListVisitor {

        List<Column> columns;
        List<Map<String, Object>> records;

        public InsertItemsListVisitor(List<Column> columns, List<Map<String, Object>> records) {
            this.columns = columns;
            this.records = records;
        }

        @Override
        public void visit(SubSelect subSelect) {
            log.warn("SubSelect Visitor");
        }

        @Override
        public void visit(ExpressionList expressionList) {
            log.trace("Expression Visitor");
            HashMap<String, Object> fieldValues = new HashMap<>();
            records.add(fieldValues);

            for (int i = 0; i < columns.size(); i++) {
                expressionList.getExpressions().get(i).accept(
                    new ValueToStringVisitor(
                        fieldValues,
                        columns.get(i).getColumnName(),
                        subSelectResolver)
                );
            }
        }

        @Override
        public void visit(NamedExpressionList namedExpressionList) {
            log.warn("NamedExpression Visitor");
        }

        @Override
        public void visit(MultiExpressionList multiExprList) {
            log.trace("MultiExpression Visitor");
            multiExprList.getExpressionLists().forEach(expressions -> {
                expressions.accept(new InsertItemsListVisitor(columns, records));
            });
        }
    }

    protected String getFromObjectName() {
        return queryData.getTable().getName();
    }

    private Insert getQueryData() {
        return getQueryData(false);
    }

    private Insert getQueryData(boolean silentMode) {
        if (queryData == null) {
            try {
                Statement statement = CCJSqlParserUtil.parse(soql);
                if (statement instanceof Insert) {
                    queryData = (Insert) statement;
                }
            } catch (JSQLParserException e) {
                if (!silentMode) {
                    log.error("Failed request to create entities with error: {}", e.getMessage(), e);
                }
            }
        }
        return queryData;
    }

    public List<Map<String, Object>> getRecords() {
        if (queryData != null && records == null) {
            records = new ArrayList<>();
            if (getQueryData().isUseValues()) {
                getQueryData().getItemsList().accept(new InsertItemsListVisitor(getQueryData().getColumns(), records));
            } else {
                if (getQueryData().getSelect() != null) {
                    if (subSelectResolver != null) {
                        log.info("Insert/Update has a sub-select: {}", getQueryData().getSelect().toString());
                        List<Map<String, Object>> subRecords = subSelectResolver.apply(getQueryData().getSelect()
                            .toString());
                        log.info("Insert/Update fetched {} records from a sub-select: {}", subRecords.size(),
                            getQueryData().getSelect().toString());
                        for (Map<String, Object> subRecord : subRecords) {
                            // this subRecord is LinkedHashMap - so the order of fields is determined by soql
                            Map<String, Object> record = new HashMap<>();
                            records.add(record);
                            int fieldIndex = 0;
                            for (final Map.Entry<String, Object> fieldEntry : subRecord.entrySet()) {
                                String insertFieldName = queryData.getColumns().get(fieldIndex).getColumnName();
                                Object subSelectFieldValue = fieldEntry.getValue();
                                record.put(insertFieldName, subSelectFieldValue);

                                fieldIndex++;
                            }
                        }
                    }
                }
            }
        }
        return records;
    }
}
