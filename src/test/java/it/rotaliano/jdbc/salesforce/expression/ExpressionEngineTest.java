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

    @Test
    public void testSubstringTwoArgumentsAndOverflow() {
        RowContext ctx = col -> null;

        // 2-argument substring: "abcdef" starting at 3 (returns "cdef")
        Expression sub2Args = new FunctionExpression("SUBSTRING", List.of(
                new LiteralExpression("abcdef"),
                new LiteralExpression(3)
        ));
        assertEquals("cdef", sub2Args.evaluate(ctx));

        // 2-argument substring: "abcdef" starting at 1 (returns "abcdef")
        Expression subStart1 = new FunctionExpression("SUBSTRING", List.of(
                new LiteralExpression("abcdef"),
                new LiteralExpression(1)
        ));
        assertEquals("abcdef", subStart1.evaluate(ctx));

        // 2-argument substring: "abcdef" starting at 7 (beyond length, returns "")
        Expression subStartOut = new FunctionExpression("SUBSTRING", List.of(
                new LiteralExpression("abcdef"),
                new LiteralExpression(7)
        ));
        assertEquals("", subStartOut.evaluate(ctx));

        // Test with explicit Integer.MAX_VALUE length to ensure no overflow
        Expression subExplicitMax = new FunctionExpression("SUBSTRING", List.of(
                new LiteralExpression("abcdef"),
                new LiteralExpression(3),
                new LiteralExpression(Integer.MAX_VALUE)
        ));
        assertEquals("cdef", subExplicitMax.evaluate(ctx));
    }

    @Test
    public void testMixedTypeNumericComparisons() {
        RowContext ctx = col -> null;

        // Integer 30 and Double 30.0
        Expression eq = new BinaryExpression(new LiteralExpression(30), new LiteralExpression(30.0), BinaryExpression.Operator.EQUALS);
        assertEquals(true, eq.evaluate(ctx));

        Expression ne = new BinaryExpression(new LiteralExpression(30), new LiteralExpression(30.0), BinaryExpression.Operator.NOT_EQUALS);
        assertEquals(false, ne.evaluate(ctx));

        // Integer 30 and Double 25.5
        Expression gt = new BinaryExpression(new LiteralExpression(30), new LiteralExpression(25.5), BinaryExpression.Operator.GREATER_THAN);
        assertEquals(true, gt.evaluate(ctx));

        Expression lt = new BinaryExpression(new LiteralExpression(25.5), new LiteralExpression(30), BinaryExpression.Operator.LESS_THAN);
        assertEquals(true, lt.evaluate(ctx));

        // Long 10L and Float 10.0f
        Expression eqLongFloat = new BinaryExpression(new LiteralExpression(10L), new LiteralExpression(10.0f), BinaryExpression.Operator.EQUALS);
        assertEquals(true, eqLongFloat.evaluate(ctx));

        Expression gteLongFloat = new BinaryExpression(new LiteralExpression(10L), new LiteralExpression(10.0f), BinaryExpression.Operator.GREATER_THAN_EQUALS);
        assertEquals(true, gteLongFloat.evaluate(ctx));

        Expression lteLongFloat = new BinaryExpression(new LiteralExpression(10L), new LiteralExpression(10.0f), BinaryExpression.Operator.LESS_THAN_EQUALS);
        assertEquals(true, lteLongFloat.evaluate(ctx));
    }

    @Test
    public void testThreeValuedLogic() {
        RowContext ctx = col -> null;

        Expression trueExpr = new LiteralExpression(true);
        Expression falseExpr = new LiteralExpression(false);
        Expression nullExpr = new LiteralExpression(null);

        // SQL Three-Valued Logic for AND:
        // true AND null -> null
        assertEquals(null, new LogicalExpression(trueExpr, nullExpr, LogicalExpression.Operator.AND).evaluate(ctx));
        // null AND true -> null
        assertEquals(null, new LogicalExpression(nullExpr, trueExpr, LogicalExpression.Operator.AND).evaluate(ctx));
        // false AND null -> false
        assertEquals(false, new LogicalExpression(falseExpr, nullExpr, LogicalExpression.Operator.AND).evaluate(ctx));
        // null AND false -> false
        assertEquals(false, new LogicalExpression(nullExpr, falseExpr, LogicalExpression.Operator.AND).evaluate(ctx));
        // null AND null -> null
        assertEquals(null, new LogicalExpression(nullExpr, nullExpr, LogicalExpression.Operator.AND).evaluate(ctx));

        // SQL Three-Valued Logic for OR:
        // true OR null -> true
        assertEquals(true, new LogicalExpression(trueExpr, nullExpr, LogicalExpression.Operator.OR).evaluate(ctx));
        // null OR true -> true
        assertEquals(true, new LogicalExpression(nullExpr, trueExpr, LogicalExpression.Operator.OR).evaluate(ctx));
        // false OR null -> null
        assertEquals(null, new LogicalExpression(falseExpr, nullExpr, LogicalExpression.Operator.OR).evaluate(ctx));
        // null OR false -> null
        assertEquals(null, new LogicalExpression(nullExpr, falseExpr, LogicalExpression.Operator.OR).evaluate(ctx));
        // null OR null -> null
        assertEquals(null, new LogicalExpression(nullExpr, nullExpr, LogicalExpression.Operator.OR).evaluate(ctx));

        // SQL Three-Valued Logic for NOT:
        // NOT null -> null
        assertEquals(null, new NotExpression(nullExpr).evaluate(ctx));
    }
}
