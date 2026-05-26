package it.rotaliano.jdbc.salesforce.statement.processor;

import it.rotaliano.jdbc.salesforce.statement.processor.utils.SelectSpecVisitor;
import it.rotaliano.jdbc.salesforce.utils.FieldDefTree;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitorAdapter;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import java.util.ArrayList;

@Slf4j
public class SoqlQueryAnalyzer {

    private final QueryAnalyzer queryAnalyzer;
    private FieldDefTree fieldDefinitions;

    public SoqlQueryAnalyzer(QueryAnalyzer queryAnalyzer) {
        this.queryAnalyzer = queryAnalyzer;
    }

    public String getSoqlQueryString() {
        try {
            Statement stmt = net.sf.jsqlparser.parser.CCJSqlParserUtil.parse(queryAnalyzer.getSoql());
            if (stmt instanceof PlainSelect select) {
                rewriteCoalesceInSelect(select);
                return select.toString();
            }
        } catch (Exception e) {
            log.warn("Failed to rewrite COALESCE in SOQL query, using original query", e);
        }
        return queryAnalyzer.getQueryData().toString();
    }

    private void rewriteCoalesceInSelect(PlainSelect select) {
        List<SelectItem<?>> newSelectItems = new ArrayList<>();
        for (SelectItem<?> item : select.getSelectItems()) {
            Expression expr = item.getExpression();
            if (expr instanceof Function func && "coalesce".equalsIgnoreCase(func.getName())) {
                if (func.getParameters() != null) {
                    for (Expression param : func.getParameters()) {
                        List<Column> cols = new ArrayList<>();
                        findColumns(param, cols);
                        for (Column col : cols) {
                            newSelectItems.add(new SelectItem<>(col));
                        }
                    }
                }
            } else {
                newSelectItems.add(item);
            }
        }
        select.setSelectItems(newSelectItems);
    }

    private void findColumns(Expression expr, List<Column> result) {
        if (expr instanceof Column col) {
            result.add(col);
        } else if (expr instanceof BinaryExpression binary) {
            findColumns(binary.getLeftExpression(), result);
            findColumns(binary.getRightExpression(), result);
        } else if (expr instanceof Function func) {
            if (func.getParameters() != null) {
                for (Expression param : func.getParameters()) {
                    findColumns(param, result);
                }
            }
        } else if (expr instanceof ParenthesedExpressionList parenthesis) {
            for (Object innerExpr : parenthesis) {
                findColumns((Expression) innerExpr, result);
            }
        }
    }

    public Statement getSoqlQuery() {
        return queryAnalyzer.getQueryData();
    }

    public FieldDefTree getFieldDefinitions() {
        if (fieldDefinitions == null) {
            fieldDefinitions = new FieldDefTree();
            String rootEntityName = queryAnalyzer.getFromObjectName();
            SelectSpecVisitor visitor = new SelectSpecVisitor(rootEntityName,
                fieldDefinitions,
                queryAnalyzer.getPartnerService());
            PlainSelect query = (PlainSelect) queryAnalyzer.getQueryData();
            final List<SelectItem<?>> selectItems = query.getSelectItems();
            replaceOrderByAlias(query, selectItems);
            selectItems.forEach(spec -> spec.accept(visitor, null));
        }
        return fieldDefinitions;
    }

    private void replaceOrderByAlias(final PlainSelect query, final List<SelectItem<?>> selectItems) {
        if (query.getOrderByElements() != null) {
            query.getOrderByElements().forEach(orderByElement -> orderByElement.accept(new OrderByVisitorAdapter<>() {
                @Override
                public <S> Expression visit(OrderByElement orderBy, S context) {
                    if (orderBy.getExpression() instanceof Column orderCol) {
                        selectItems.forEach(item -> {
                            if (item.getAlias() != null && item.getAlias()
                                    .getName()
                                    .equals(orderCol.getColumnName()) && item.getExpression() instanceof Column c) {
                                orderCol.setColumnName(c.getColumnName());
                            }
                        });
                    }
                    return null;
                }
            }, null));
        }
    }

    public boolean isExpandedStarSyntaxForFields() {
        return queryAnalyzer.isExpandedStarSyntaxForFields();
    }

    public String getFromObjectName() {
        return queryAnalyzer.getFromObjectName();
    }
}
