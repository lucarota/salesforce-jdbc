package it.rotaliano.jdbc.salesforce.expression;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

public class ExpressionEngineTest {
    @Test
    public void testCoreExpressions() {
        RowContext ctx = col -> {
            Map<String, Object> map = new HashMap<>();
            map.put("Name", "  John Doe  ");
            map.put("Code", "A-B-C");
            return map.get(col);
        };

        // Column and Literal
        Expression colExpr = new ColumnExpression("Name");
        assertEquals("  John Doe  ", colExpr.evaluate(ctx));

        Expression litExpr = new LiteralExpression("Hello");
        assertEquals("Hello", litExpr.evaluate(ctx));

        // UPPER
        Expression upperExpr = new FunctionExpression("UPPER", List.of(colExpr));
        assertEquals("  JOHN DOE  ", upperExpr.evaluate(ctx));

        // LOWER
        Expression lowerExpr = new FunctionExpression("LOWER", List.of(colExpr));
        assertEquals("  john doe  ", lowerExpr.evaluate(ctx));

        // TRIM
        Expression trimExpr = new FunctionExpression("TRIM", List.of(colExpr));
        assertEquals("John Doe", trimExpr.evaluate(ctx));

        // Nested LOWER(TRIM(Name))
        Expression nested = new FunctionExpression("LOWER", List.of(trimExpr));
        assertEquals("john doe", nested.evaluate(ctx));

        // SUBSTRING
        Expression subExpr = new FunctionExpression("SUBSTRING", List.of(colExpr, new LiteralExpression(3), new LiteralExpression(4)));
        assertEquals("John", subExpr.evaluate(ctx));

        // REPLACE
        Expression replaceExpr = new FunctionExpression("REPLACE", List.of(new ColumnExpression("Code"), new LiteralExpression("-"), new LiteralExpression("")));
        assertEquals("ABC", replaceExpr.evaluate(ctx));
    }

    @Test
    public void testBinaryExpressions() {
        RowContext ctx = col -> {
            Map<String, Object> map = new HashMap<>();
            map.put("Age", 30);
            map.put("NullCol", null);
            return map.get(col);
        };

        Expression ageExpr = new ColumnExpression("Age");
        Expression nullExpr = new ColumnExpression("NullCol");

        // EQUALS
        Expression eqExpr = new BinaryExpression(ageExpr, new LiteralExpression(30), BinaryExpression.Operator.EQUALS);
        assertEquals(true, eqExpr.evaluate(ctx));

        Expression eqExpr2 = new BinaryExpression(ageExpr, new LiteralExpression(25), BinaryExpression.Operator.EQUALS);
        assertEquals(false, eqExpr2.evaluate(ctx));

        // NOT_EQUALS
        Expression neExpr = new BinaryExpression(ageExpr, new LiteralExpression(25), BinaryExpression.Operator.NOT_EQUALS);
        assertEquals(true, neExpr.evaluate(ctx));

        // GREATER_THAN
        Expression gtExpr = new BinaryExpression(ageExpr, new LiteralExpression(25), BinaryExpression.Operator.GREATER_THAN);
        assertEquals(true, gtExpr.evaluate(ctx));

        Expression gtExpr2 = new BinaryExpression(ageExpr, new LiteralExpression(35), BinaryExpression.Operator.GREATER_THAN);
        assertEquals(false, gtExpr2.evaluate(ctx));

        // GREATER_THAN_EQUALS
        Expression gteExpr = new BinaryExpression(ageExpr, new LiteralExpression(30), BinaryExpression.Operator.GREATER_THAN_EQUALS);
        assertEquals(true, gteExpr.evaluate(ctx));

        // LESS_THAN
        Expression ltExpr = new BinaryExpression(ageExpr, new LiteralExpression(35), BinaryExpression.Operator.LESS_THAN);
        assertEquals(true, ltExpr.evaluate(ctx));

        // LESS_THAN_EQUALS
        Expression lteExpr = new BinaryExpression(ageExpr, new LiteralExpression(30), BinaryExpression.Operator.LESS_THAN_EQUALS);
        assertEquals(true, lteExpr.evaluate(ctx));

        // Compare with null
        Expression nullEqExpr = new BinaryExpression(nullExpr, new LiteralExpression(30), BinaryExpression.Operator.EQUALS);
        assertEquals(false, nullEqExpr.evaluate(ctx));

        // Compare using default comparator
        Expression stringCompareExpr = new BinaryExpression(new LiteralExpression("b"), new LiteralExpression("a"), BinaryExpression.Operator.GREATER_THAN);
        assertEquals(true, stringCompareExpr.evaluate(ctx));
    }

    @Test
    public void testLogicalAndNotExpressions() {
        RowContext ctx = col -> null;

        Expression trueExpr = new LiteralExpression(true);
        Expression falseExpr = new LiteralExpression(false);

        // AND
        Expression andTrue = new LogicalExpression(trueExpr, trueExpr, LogicalExpression.Operator.AND);
        assertEquals(true, andTrue.evaluate(ctx));

        Expression andFalse = new LogicalExpression(trueExpr, falseExpr, LogicalExpression.Operator.AND);
        assertEquals(false, andFalse.evaluate(ctx));

        // OR
        Expression orTrue = new LogicalExpression(trueExpr, falseExpr, LogicalExpression.Operator.OR);
        assertEquals(true, orTrue.evaluate(ctx));

        Expression orFalse = new LogicalExpression(falseExpr, falseExpr, LogicalExpression.Operator.OR);
        assertEquals(false, orFalse.evaluate(ctx));

        // NOT
        Expression notTrue = new NotExpression(trueExpr);
        assertEquals(false, notTrue.evaluate(ctx));

        Expression notFalse = new NotExpression(falseExpr);
        assertEquals(true, notFalse.evaluate(ctx));
    }
}
