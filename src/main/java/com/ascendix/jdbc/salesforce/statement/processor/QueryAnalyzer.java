package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.sforce.soap.partner.DescribeSObjectResult;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

@Slf4j
@Getter
public class QueryAnalyzer {

    private String soql;
    private final Function<String, List<Map<String, Object>>> subSelectResolver;
    private Statement queryData;
    private final PartnerService partnerService;
    private boolean expandedStarSyntaxForFields = false;

    public QueryAnalyzer(String soql,
        Function<String, List<Map<String, Object>>> subSelectResolver, PartnerService partnerService) {
        this.soql = soql;
        this.subSelectResolver = subSelectResolver;
        this.partnerService = partnerService;
        this.queryData = getQueryData(true);
    }

    public boolean analyse(String soql, StatementTypeEnum desiredType) {
        if (soql == null || soql.trim().isEmpty()) {
            return false;
        }
        this.soql = soql;
        return switch (getType()) {
            case INSERT -> desiredType == StatementTypeEnum.INSERT;
            case UPDATE -> desiredType == StatementTypeEnum.UPDATE;
            case DELETE -> desiredType == StatementTypeEnum.DELETE;
            case SELECT -> desiredType == StatementTypeEnum.SELECT;
            case SEARCH, UNDEFINED -> false;
        };
    }

    public String getFromObjectName() {
        return switch (getType()) {
            case INSERT -> ((Insert)queryData).getTable().getName();
            case UPDATE -> ((Update)queryData).getTable().getName();
            case DELETE -> ((Delete)queryData).getTable().getName();
            case SELECT -> ((PlainSelect)queryData).getFromItem().toString();
            case SEARCH, UNDEFINED -> "";
        };
    }

    protected Statement getQueryData() {
        return getQueryData(false);
    }

    protected Statement getQueryData(boolean silentMode) {
        if (queryData == null) {
            try {
                queryData = CCJSqlParserUtil.parse(soql);
                if (queryData instanceof PlainSelect select) {
                    if ("*".equals(select.getSelectItem(0).toString())) {
                        select.getSelectItems().clear();
                        this.expandedStarSyntaxForFields = true;
                        DescribeSObjectResult describeSObjectResult = describeObject(select.getFromItem().toString());
                        Arrays.stream(describeSObjectResult.getFields())
                            .forEach(f -> select.addSelectItem(new Column(null, f.getName())));
                    }
                    this.soql = select.toString();
                    return select;
                }
            } catch (JSQLParserException e) {
                if (!silentMode) {
                    log.error("Failed request to create entities with error: {}", e.getMessage(), e);
                }
            }
        }
        return queryData;
    }


    private DescribeSObjectResult describeObject(String fromObjectName) {
        return partnerService.describeSObject(fromObjectName);
    }

    public StatementTypeEnum getType() {
        if (queryData instanceof Delete) return StatementTypeEnum.DELETE;
        if (queryData instanceof Insert) return StatementTypeEnum.INSERT;
        if (queryData instanceof Update) return StatementTypeEnum.UPDATE;
        if (queryData instanceof Select) return StatementTypeEnum.SELECT;
        return StatementTypeEnum.UNDEFINED;
    }

    /**
     * Checks if this update is using WHERE Id='001xx010201' notation and no other criteria
     */
    protected String checkIsDirectIdWhere(Expression where, List<Object> parameters) {
        if (where instanceof final EqualsTo whereRoot) {
            // direct ID comparison like Id='001xx192918212'
            if (whereRoot.getLeftExpression() instanceof final Column col
                && "id".equalsIgnoreCase(col.getColumnName())) {
                if (whereRoot.getRightExpression() instanceof StringValue value) {
                    return value.getValue();
                }
                if (whereRoot.getRightExpression() instanceof JdbcParameter param) {
                    return parameters.get(param.getIndex() - 1).toString();
                }
            }

            // direct ID comparison like '001xx192918212'=Id
            if (whereRoot.getRightExpression() instanceof final Column col
                && "id".equalsIgnoreCase(col.getColumnName())) {
                if (whereRoot.getLeftExpression() instanceof StringValue value) {
                    return value.getValue();
                }
                if (whereRoot.getLeftExpression() instanceof JdbcParameter param) {
                    return parameters.get(param.getIndex() - 1).toString();
                }
            }
        }
        return null;
    }
}
