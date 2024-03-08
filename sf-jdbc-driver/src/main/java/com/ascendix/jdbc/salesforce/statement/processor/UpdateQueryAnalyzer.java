package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.ForceDriver;
import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.statement.processor.utils.ColumnsFinderVisitor;
import com.ascendix.jdbc.salesforce.statement.processor.utils.UpdateRecordVisitor;
import com.ascendix.jdbc.salesforce.statement.processor.utils.ValueToStringVisitor;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.SelectUtils;

public class UpdateQueryAnalyzer {

    private static final Logger logger = Logger.getLogger(ForceDriver.SF_JDBC_DRIVER_NAME);

    private String soql;
    private final Function<String, List<Map<String, Object>>> subSelectResolver;
    private Update queryData;
    private List<Map<String, Object>> records;

    public UpdateQueryAnalyzer(String soql, PartnerService partnerService,
        Function<String, List<Map<String, Object>>> subSelectResolver) {
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

    public class UpdateItemsListVisitor implements ItemsListVisitor {

        List<Column> columns;
        List<Map<String, Object>> records;

        public UpdateItemsListVisitor(List<Column> columns, List<Map<String, Object>> records) {
            this.columns = columns;
            this.records = records;
        }

        @Override
        public void visit(SubSelect subSelect) {
            System.out.println("SubSelect Visitor");
        }

        @Override
        public void visit(ExpressionList expressionList) {
            System.out.println("Expression Visitor");
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
            System.out.println("NamedExpression Visitor");
        }

        @Override
        public void visit(MultiExpressionList multiExprList) {
            System.out.println("MultiExpression Visitor");
            multiExprList.getExpressionLists().forEach(expressions -> {
                expressions.accept(new UpdateItemsListVisitor(columns, records));
            });
        }
    }

    private Field findField(String name, DescribeSObjectResult objectDesc, Function<Field, String> nameFetcher) {
        return Arrays.stream(objectDesc.getFields())
            .filter(field -> name.equalsIgnoreCase(nameFetcher.apply(field)))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(
                "Unknown field name \"" + name + "\" in object \"" + objectDesc.getName() + "\""));
    }

    protected String getFromObjectName() {
        return queryData.getTable().getName();
    }

    private Update getQueryData() {
        return getQueryData(false);
    }

    private Update getQueryData(boolean silentMode) {
        if (queryData == null) {
            try {
                Statement statement = CCJSqlParserUtil.parse(soql);
                if (statement instanceof Update) {
                    queryData = (Update) statement;
                }
            } catch (JSQLParserException e) {
                if (!silentMode) {
                    logger.log(Level.SEVERE, "Failed request to create entities with error: " + e.getMessage(), e);
                }
            }
        }
        return queryData;
    }

    public List<Map<String, Object>> getRecords() {
        if (queryData != null && records == null) {
            records = new ArrayList<>();

            String id = checkIsDirectIdWhere();
            if (id != null) {
                Set<String> columnsToFetch = new HashSet<>();
                boolean isFunctionFound = findSubFields(columnsToFetch, getQueryData().getExpressions());
                if (columnsToFetch.isEmpty() && !isFunctionFound) {
                    // means there is no calculations in the new field values
                    Map<String, Object> record = new HashMap<>();
                    records.add(record);
                    record.put("Id", id);

                    List<Column> columns = getQueryData().getColumns();
                    for (int i = 0; i < columns.size(); i++) {
                        getQueryData().getExpressions().get(i).accept(
                            new ValueToStringVisitor(
                                record,
                                columns.get(i).getColumnName(),
                                subSelectResolver)
                        );
                    }
                    return records;
                }
            }
            // otherwise we need to fetch all the Entity Ids applicable to this WHERE condition and then build a records using these Ids with fields to update
            if (subSelectResolver != null) {
                try {
                    Set<String> columnsToFetch = new HashSet<>();
                    findSubFields(columnsToFetch, getQueryData().getExpressions());
                    columnsToFetch.add("Id");
                    Select select = SelectUtils.buildSelectFromTableAndExpressions(getQueryData().getTable(),
                        columnsToFetch.toArray(new String[]{}));
                    ((PlainSelect) select.getSelectBody()).setWhere(getQueryData().getWhere());

                    List<Map<String, Object>> subRecords = subSelectResolver.apply(select.toString());

                    for (Map<String, Object> subRecord : subRecords) {
                        // this subRecord is LinkedHashMap - so the order of fields is determined by soql
                        Map<String, Object> record = new HashMap<>();
                        records.add(record);
                        record.put("Id", subRecord.get("Id"));

                        List<Column> columns = getQueryData().getColumns();
                        // Iterating over the received entities and adding fields to update
                        for (int i = 0; i < columns.size(); i++) {
                            Expression expr = getQueryData().getExpressions().get(i);
                            expr.accept(
                                new UpdateRecordVisitor(
                                    getQueryData(),
                                    record,
                                    subRecord,
                                    columns.get(i).getColumnName(),
                                    subSelectResolver)
                            );
                        }
                    }
                } catch (JSQLParserException e) {
                    logger.log(Level.SEVERE,
                        "Failed request to fetch the applicable entities: error in columns to fetch",
                        e);
                }
            } else {
                logger.log(Level.SEVERE,
                    "Failed request to fetch the applicable entities: subSelectResolver not defined");
            }
        }
        return records;
    }

    private Set<String> findFields(List<Expression> expressions) {
        Set<String> columns = new HashSet<>();
        findSubFields(columns, expressions);
        return columns;
    }

    private boolean findSubFields(Set<String> columns, List<Expression> expressions) {
        ColumnsFinderVisitor visitor = new ColumnsFinderVisitor(columns);
        expressions.forEach(expr -> expr.accept(visitor));
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
