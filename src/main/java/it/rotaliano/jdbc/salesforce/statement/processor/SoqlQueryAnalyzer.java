package it.rotaliano.jdbc.salesforce.statement.processor;

import it.rotaliano.jdbc.salesforce.statement.processor.utils.SelectSpecVisitor;
import it.rotaliano.jdbc.salesforce.utils.FieldDefTree;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitorAdapter;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;


@Slf4j
public class SoqlQueryAnalyzer {

    private final QueryAnalyzer queryAnalyzer;
    private FieldDefTree fieldDefinitions;
    private it.rotaliano.jdbc.salesforce.expression.Expression clientSideWhereExpression;

    public SoqlQueryAnalyzer(QueryAnalyzer queryAnalyzer) {
        this.queryAnalyzer = queryAnalyzer;
    }

    public it.rotaliano.jdbc.salesforce.expression.Expression getClientSideWhereExpression() {
        return clientSideWhereExpression;
    }

    public String getSoqlQueryString() {
        return getSoqlQueryString(new ArrayList<>());
    }

    public String getSoqlQueryString(List<Object> parameters) {
        try {
            Statement stmt = net.sf.jsqlparser.parser.CCJSqlParserUtil.parse(queryAnalyzer.getSoql());
            if (stmt instanceof PlainSelect select) {
                rewriteSelectItems(select);
                if (select.getWhere() != null && containsEmulatedFunctions(select.getWhere())) {
                    // Extract columns from WHERE clause to select list
                    List<Column> cols = new ArrayList<>();
                    findColumns(select.getWhere(), cols);
                    for (Column col : cols) {
                        // Make sure they are not already in SELECT
                        boolean exists = select.getSelectItems().stream()
                            .anyMatch(item -> item.getExpression().toString().equalsIgnoreCase(col.toString()));
                        if (!exists) {
                            select.addSelectItem(col);
                        }
                    }
                    // Build client-side WHERE expression
                    this.clientSideWhereExpression = it.rotaliano.jdbc.salesforce.expression.AstBuilder.build(select.getWhere());
                    // Clear from SOQL
                    select.setWhere(null);
                } else {
                    rewriteCoalesceInWhere(select, parameters);
                }
                return select.toString();
            }
        } catch (Exception e) {
            log.warn("Failed to rewrite query, using original", e);
        }
        return queryAnalyzer.getQueryData().toString();
    }

    private void rewriteCoalesceInWhere(PlainSelect select, List<Object> parameters) {
        if (select.getWhere() != null) {
            select.setWhere(rewriteExpression(select.getWhere(), parameters));
        }
    }

    private Expression rewriteExpression(Expression expr, List<Object> parameters) {
        if (expr == null) {
            return null;
        }

        if (expr instanceof AndExpression and) {
            and.setLeftExpression(rewriteExpression(and.getLeftExpression(), parameters));
            and.setRightExpression(rewriteExpression(and.getRightExpression(), parameters));
            return and;
        }

        if (expr instanceof OrExpression or) {
            or.setLeftExpression(rewriteExpression(or.getLeftExpression(), parameters));
            or.setRightExpression(rewriteExpression(or.getRightExpression(), parameters));
            return or;
        }

        if (expr instanceof ParenthesedExpressionList parenthesis) {
            parenthesis.replaceAll(o -> rewriteExpression((Expression) o, parameters));
            return parenthesis;
        }

        if (expr instanceof IsNullExpression isNull) {
            Expression left = rewriteExpression(isNull.getLeftExpression(), parameters);
            if (left instanceof Function func && "coalesce".equalsIgnoreCase(func.getName())) {
                return expandCoalesceNullity(func, isNull.isNot());
            }
            if (isNull.isNot()) {
                return new NotEqualsTo(left, new NullValue());
            } else {
                return new EqualsTo(left, new NullValue());
            }
        }

        if (expr instanceof InExpression in) {
            in.setLeftExpression(rewriteExpression(in.getLeftExpression(), parameters));
            in.setRightExpression(rewriteExpression(in.getRightExpression(), parameters));
            return in;
        }

        if (expr instanceof NotExpression not) {
            not.setExpression(rewriteExpression(not.getExpression(), parameters));
            return not;
        }

        if (expr instanceof BinaryExpression binary) {
            Expression left = binary.getLeftExpression();
            Expression right = binary.getRightExpression();

            // Nullity comparisons with NullValue
            if (right instanceof NullValue) {
                if (left instanceof Function func && "coalesce".equalsIgnoreCase(func.getName())) {
                    if (binary instanceof EqualsTo) {
                        return expandCoalesceNullity(func, false);
                    }
                    if (binary instanceof NotEqualsTo) {
                        return expandCoalesceNullity(func, true);
                    }
                }
            }
            if (left instanceof NullValue) {
                if (right instanceof Function func && "coalesce".equalsIgnoreCase(func.getName())) {
                    if (binary instanceof EqualsTo) {
                        return expandCoalesceNullity(func, false);
                    }
                    if (binary instanceof NotEqualsTo) {
                        return expandCoalesceNullity(func, true);
                    }
                }
            }

            // General binary comparisons
            if (left instanceof Function func && "coalesce".equalsIgnoreCase(func.getName())) {
                return expandCoalesceBinary(func, right, binary, true, parameters);
            }
            if (right instanceof Function func && "coalesce".equalsIgnoreCase(func.getName())) {
                return expandCoalesceBinary(func, left, binary, false, parameters);
            }

            // Recurse left and right for other binary expressions
            binary.setLeftExpression(rewriteExpression(left, parameters));
            binary.setRightExpression(rewriteExpression(right, parameters));
            return binary;
        }

        return expr;
    }

    private Expression expandCoalesceNullity(Function func, boolean isNot) {
        if (func.getParameters() == null || func.getParameters().isEmpty()) {
            return new EqualsTo(new Column("Id"), new NullValue());
        }
        Expression result = null;
        for (Expression param : func.getParameters()) {
            BinaryExpression comp;
            if (isNot) {
                comp = new NotEqualsTo();
            } else {
                comp = new EqualsTo();
            }
            comp.setLeftExpression(param);
            comp.setRightExpression(new NullValue());

            if (result == null) {
                result = comp;
            } else {
                if (isNot) {
                    result = new OrExpression(result, comp);
                } else {
                    result = new AndExpression(result, comp);
                }
            }
        }
        return new ParenthesedExpressionList<>(result);
    }

    private Expression expandCoalesceBinary(Function func, Expression otherSide, BinaryExpression binary, boolean coalesceIsLeft, List<Object> parameters) {
        if (func.getParameters() == null || func.getParameters().isEmpty()) {
            return new EqualsTo(new Column("Id"), new NullValue());
        }

        List<Expression> terms = new ArrayList<>();
        for (int i = 0; i < func.getParameters().size(); i++) {
            Expression param = (Expression) func.getParameters().get(i);
            Expression comparison = null;

            Object valParam = getLiteralValue(param, parameters);
            Object valOther = getLiteralValue(otherSide, parameters);

            if (valParam != UNRESOLVABLE_VALUE && valOther != UNRESOLVABLE_VALUE) {
                // Both are literals, evaluate client-side
                Object cmpResult = evaluateComparison(coalesceIsLeft ? valParam : valOther, coalesceIsLeft ? valOther : valParam, binary);
                if (cmpResult == Boolean.TRUE) {
                    // True term: check all previous parameters are null
                    Expression term = buildAllPreviousNull(func, i);
                    if (term == null) {
                        // This means it's the very first parameter and it's unconditionally true!
                        // In SOQL, we can represent true as `Id != null`
                        return new ParenthesedExpressionList<>(new NotEqualsTo(new Column("Id"), new NullValue()));
                    }
                    terms.add(new ParenthesedExpressionList<>(term));
                    break; // stop generating further terms since this literal is true and ends the COALESCE evaluation
                } else if (cmpResult == Boolean.FALSE) {
                    // False term: this term is impossible, skip it
                    continue;
                }
            }

            // If we didn't simplify to true/false, construct the comparison node
            Expression leftExpr = coalesceIsLeft ? param : otherSide;
            Expression rightExpr = coalesceIsLeft ? otherSide : param;
            comparison = cloneBinaryExpression(binary, leftExpr, rightExpr);

            // Construct this term: previous are null AND this comparison
            Expression term = buildAllPreviousNull(func, i);
            if (term == null) {
                term = comparison;
            } else {
                term = new AndExpression(term, comparison);
            }
            terms.add(new ParenthesedExpressionList<>(term));
        }

        if (terms.isEmpty()) {
            // Sifted to completely false
            return new EqualsTo(new Column("Id"), new NullValue());
        }

        Expression combined = null;
        for (Expression term : terms) {
            if (combined == null) {
                combined = term;
            } else {
                combined = new OrExpression(combined, term);
            }
        }
        return new ParenthesedExpressionList<>(combined);
    }

    private Expression buildAllPreviousNull(Function func, int limitIndex) {
        Expression term = null;
        for (int j = 0; j < limitIndex; j++) {
            EqualsTo eqNull = new EqualsTo();
            eqNull.setLeftExpression((Expression) func.getParameters().get(j));
            eqNull.setRightExpression(new NullValue());
            if (term == null) {
                term = eqNull;
            } else {
                term = new AndExpression(term, eqNull);
            }
        }
        return term;
    }

    private static final Object UNRESOLVABLE_VALUE = new Object();

    private Object getLiteralValue(Expression expr, List<Object> parameters) {
        if (expr instanceof StringValue sv) {
            return sv.getValue();
        }
        if (expr instanceof LongValue lv) {
            return lv.getValue();
        }
        if (expr instanceof DoubleValue dv) {
            return dv.getValue();
        }
        if (expr instanceof NullValue) {
            return null;
        }
        if (expr instanceof JdbcParameter param) {
            if (parameters != null && param.getIndex() - 1 >= 0 && param.getIndex() - 1 < parameters.size()) {
                return parameters.get(param.getIndex() - 1);
            }
        }
        return UNRESOLVABLE_VALUE;
    }

    private Object evaluateComparison(Object val1, Object val2, BinaryExpression binary) {
        if (val1 == UNRESOLVABLE_VALUE || val2 == UNRESOLVABLE_VALUE) {
            return UNRESOLVABLE_VALUE;
        }
        if (binary instanceof EqualsTo) {
            return java.util.Objects.equals(val1, val2);
        }
        if (binary instanceof NotEqualsTo) {
            return !java.util.Objects.equals(val1, val2);
        }
        if (binary instanceof LikeExpression) {
            if (val1 instanceof String s1 && val2 instanceof String s2) {
                String regex = s2.replace(".", "\\.")
                                 .replace("?", "\\?")
                                 .replace("%", ".*")
                                 .replace("_", ".");
                return s1.matches("(?i)" + regex);
            }
            return java.util.Objects.equals(val1, val2);
        }
        if (binary instanceof GreaterThan) {
            return compareValues(val1, val2) > 0;
        }
        if (binary instanceof GreaterThanEquals) {
            return compareValues(val1, val2) >= 0;
        }
        if (binary instanceof MinorThan) {
            return compareValues(val1, val2) < 0;
        }
        if (binary instanceof MinorThanEquals) {
            return compareValues(val1, val2) <= 0;
        }
        return UNRESOLVABLE_VALUE;
    }

    @SuppressWarnings("unchecked")
    private int compareValues(Object val1, Object val2) {
        if (val1 instanceof Number n1 && val2 instanceof Number n2) {
            return Double.compare(n1.doubleValue(), n2.doubleValue());
        }
        if (val1 instanceof Comparable && val2 instanceof Comparable && val1.getClass().isAssignableFrom(val2.getClass())) {
            return ((Comparable<Object>) val1).compareTo(val2);
        }
        return 0;
    }

    private BinaryExpression cloneBinaryExpression(BinaryExpression source, Expression left, Expression right) {
        try {
            BinaryExpression clone = source.getClass().getDeclaredConstructor().newInstance();
            clone.setLeftExpression(left);
            clone.setRightExpression(right);
            return clone;
        } catch (Exception e) {
            log.warn("Failed to clone binary expression", e);
            return null;
        }
    }


    private void rewriteSelectItems(PlainSelect select) {
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
            } else if (expr instanceof CaseExpression caseExpr) {
                List<Column> cols = new ArrayList<>();
                findColumns(caseExpr, cols);
                for (Column col : cols) {
                    newSelectItems.add(new SelectItem<>(col));
                }
            } else if (containsEmulatedFunctions(expr)) {
                List<Column> cols = new ArrayList<>();
                findColumns(expr, cols);
                for (Column col : cols) {
                    newSelectItems.add(new SelectItem<>(col));
                }
            } else {
                newSelectItems.add(item);
            }
        }
        select.setSelectItems(newSelectItems);
    }

    public static void findColumns(Expression expr, List<Column> result) {
        if (expr == null) {
            return;
        }
        if (expr instanceof Column col) {
            result.add(col);
        } else if (expr instanceof BinaryExpression binary) {
            findColumns(binary.getLeftExpression(), result);
            findColumns(binary.getRightExpression(), result);
        } else if (expr instanceof net.sf.jsqlparser.expression.TrimFunction tf) {
            findColumns(tf.getFromExpression(), result);
            findColumns(tf.getExpression(), result);
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
        } else if (expr instanceof CaseExpression caseExpr) {
            findColumns(caseExpr.getSwitchExpression(), result);
            if (caseExpr.getWhenClauses() != null) {
                for (Expression when : caseExpr.getWhenClauses()) {
                    findColumns(when, result);
                }
            }
            findColumns(caseExpr.getElseExpression(), result);
        } else if (expr instanceof WhenClause whenClause) {
            findColumns(whenClause.getWhenExpression(), result);
            findColumns(whenClause.getThenExpression(), result);
        } else if (expr instanceof IsNullExpression isNull) {
            findColumns(isNull.getLeftExpression(), result);
        } else if (expr instanceof InExpression in) {
            findColumns(in.getLeftExpression(), result);
            findColumns(in.getRightExpression(), result);
        } else if (expr instanceof Between between) {
            findColumns(between.getLeftExpression(), result);
            findColumns(between.getBetweenExpressionStart(), result);
            findColumns(between.getBetweenExpressionEnd(), result);
        } else if (expr instanceof NotExpression not) {
            findColumns(not.getExpression(), result);
        }
    }

    public static boolean containsEmulatedFunctions(net.sf.jsqlparser.expression.Expression expr) {
        if (expr == null) return false;
        if (expr instanceof net.sf.jsqlparser.expression.TrimFunction tf) {
            return true;
        }
        if (expr instanceof net.sf.jsqlparser.expression.Function func) {
            String name = func.getName().toUpperCase();
            if (name.equals("UPPER") || name.equals("LOWER") || name.equals("TRIM") || name.equals("SUBSTRING") || name.equals("REPLACE")) {
                return true;
            }
            if (func.getParameters() != null) {
                for (net.sf.jsqlparser.expression.Expression p : func.getParameters()) {
                    if (containsEmulatedFunctions(p)) return true;
                }
            }
        }
        if (expr instanceof net.sf.jsqlparser.expression.BinaryExpression binary) {
            return containsEmulatedFunctions(binary.getLeftExpression()) || containsEmulatedFunctions(binary.getRightExpression());
        }
        if (expr instanceof net.sf.jsqlparser.expression.NotExpression not) {
            return containsEmulatedFunctions(not.getExpression());
        }
        if (expr instanceof ParenthesedExpressionList paren) {
            for (Object inner : paren) {
                if (containsEmulatedFunctions((Expression) inner)) return true;
            }
        }
        if (expr instanceof net.sf.jsqlparser.expression.CaseExpression caseExpr) {
            if (containsEmulatedFunctions(caseExpr.getSwitchExpression())) return true;
            if (caseExpr.getWhenClauses() != null) {
                for (net.sf.jsqlparser.expression.Expression when : caseExpr.getWhenClauses()) {
                    if (containsEmulatedFunctions(when)) return true;
                }
            }
            if (containsEmulatedFunctions(caseExpr.getElseExpression())) return true;
        }
        if (expr instanceof net.sf.jsqlparser.expression.WhenClause whenClause) {
            return containsEmulatedFunctions(whenClause.getWhenExpression()) || containsEmulatedFunctions(whenClause.getThenExpression());
        }
        if (expr instanceof IsNullExpression isNull) {
            return containsEmulatedFunctions(isNull.getLeftExpression());
        }
        if (expr instanceof InExpression in) {
            return containsEmulatedFunctions(in.getLeftExpression()) || containsEmulatedFunctions(in.getRightExpression());
        }
        if (expr instanceof Between between) {
            return containsEmulatedFunctions(between.getLeftExpression())
                || containsEmulatedFunctions(between.getBetweenExpressionStart())
                || containsEmulatedFunctions(between.getBetweenExpressionEnd());
        }
        return false;
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
