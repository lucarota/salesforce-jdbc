package com.ascendix.jdbc.salesforce.statement.processor;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.SelectUtils;

import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.ascendix.jdbc.salesforce.statement.processor.InsertQueryProcessor.SF_JDBC_DRIVER_NAME;

public class DeleteQueryAnalyzer {

    private static final Logger logger = Logger.getLogger(SF_JDBC_DRIVER_NAME);

    private String soql;
    private Function<String, List<Map<String, Object>>> subSelectResolver;
    private Delete queryData;
    private List<String> records;

    public DeleteQueryAnalyzer(String soql,
                               Function<String, List<Map<String, Object>>> subSelectResolver) {
        this.soql = soql;
        this.subSelectResolver = subSelectResolver;
    }

    public boolean analyse(String soql) {
        if (soql == null || soql.trim().length() == 0) {
            return false;
        }
        this.soql = soql;
        return getQueryData(true) != null;
    }

    protected String getFromObjectName() {
        return queryData.getTable().getName();
    }

    private Delete getQueryData() {
        return getQueryData(false);
    }

    private Delete getQueryData(boolean silentMode) {
        if (queryData == null) {
            try {
                Statement statement = CCJSqlParserUtil.parse(soql);
                if (statement instanceof Delete) {
                    queryData = (Delete) statement;
                }
            } catch (JSQLParserException e) {
                if (!silentMode) {
                    logger.log(Level.SEVERE, "Failed request to create entities with error: " + e.getMessage(), e);
                }
            }
        }
        return queryData;
    }

    public List<String> getRecords() {
        if (queryData != null && records == null) {
            records = new ArrayList<>();

            String id = checkIsDirectIdWhere();
            if (id != null) {
                records.add(id);

                return records;
            }
            // otherwise we need to fetch all the Entity Ids applicable to this WHERE condition and then build a records using these Ids to delete
            if (subSelectResolver != null) {
                try {
                    Select select = SelectUtils.buildSelectFromTableAndExpressions(getQueryData().getTable(), new String[]{"Id"});
                    ((PlainSelect)select.getSelectBody()).setWhere(getQueryData().getWhere());

                    List<Map<String, Object>> subRecords = subSelectResolver.apply(select.toString());

                    for (Map<String, Object> subRecord: subRecords) {
                        // this subRecord is LinkedHashMap - so the order of fields is determined by soql
                        records.add((String) subRecord.get("Id"));
                    }
                } catch (JSQLParserException e) {
                    logger.log(Level.SEVERE,"Failed request to fetch the applicable entities: error in columns to fetch", e);
                }

            } else {
                logger.log(Level.SEVERE,"Failed request to fetch the applicable entities: subSelectResolver not defined");
            }

        }
        return records;
    }

    /** Checks if this update is using WHERE Id='001xx010201' notation and no other criteria */
    private String checkIsDirectIdWhere() {
        if (queryData.getWhere() != null && queryData.getWhere() instanceof EqualsTo) {
            EqualsTo whereRoot = (EqualsTo) queryData.getWhere();
            // direct ID comparison like Id='001xx192918212'
            if (whereRoot.getLeftExpression() instanceof Column && whereRoot.getRightExpression() instanceof StringValue) {
                Column col = (Column)whereRoot.getLeftExpression();
                if ("id".equalsIgnoreCase(col.getColumnName())) {
                    StringValue idValue = (StringValue) whereRoot.getRightExpression();
                    return idValue.getValue();
                }
            }
            // direct ID comparison like '001xx192918212'=Id
            if (whereRoot.getLeftExpression() instanceof StringValue && whereRoot.getRightExpression() instanceof Column) {
                Column col = (Column)whereRoot.getRightExpression();
                if ("id".equalsIgnoreCase(col.getColumnName())) {
                    StringValue idValue = (StringValue) whereRoot.getLeftExpression();
                    return idValue.getValue();
                }
            }
        }
        return null;
    }
}
