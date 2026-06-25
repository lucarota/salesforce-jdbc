package it.rotaliano.jdbc.salesforce.statement.processor;

import it.rotaliano.jdbc.salesforce.expression.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class SqlStringFunctionsTest {
    private RowContext buildContext(String val) {
        return col -> val;
    }

    @Test
    public void testUpper() {
        Expression func = new FunctionExpression("UPPER", List.of(new ColumnExpression("col")));
        assertEquals("ABC", func.evaluate(buildContext("abc")));
        assertEquals("", func.evaluate(buildContext("")));
        assertEquals(null, func.evaluate(buildContext(null)));
    }

    @Test
    public void testLower() {
        Expression func = new FunctionExpression("LOWER", List.of(new ColumnExpression("col")));
        assertEquals("abc", func.evaluate(buildContext("ABC")));
        assertEquals("", func.evaluate(buildContext("")));
        assertEquals(null, func.evaluate(buildContext(null)));
    }

    @Test
    public void testTrim() {
        Expression func = new FunctionExpression("TRIM", List.of(new ColumnExpression("col")));
        assertEquals("abc", func.evaluate(buildContext("  abc  ")));
        assertEquals("abc", func.evaluate(buildContext("abc")));
        assertEquals("", func.evaluate(buildContext("  ")));
        assertEquals(null, func.evaluate(buildContext(null)));
    }

    @Test
    public void testSubstring() {
        RowContext ctx = col -> "ABCDE";
        // Normal
        Expression sub1 = new FunctionExpression("SUBSTRING", List.of(new ColumnExpression("col"), new LiteralExpression(2), new LiteralExpression(2)));
        assertEquals("BC", sub1.evaluate(ctx));

        // Start = 1
        Expression sub2 = new FunctionExpression("SUBSTRING", List.of(new ColumnExpression("col"), new LiteralExpression(1), new LiteralExpression(3)));
        assertEquals("ABC", sub2.evaluate(ctx));

        // Length > string
        Expression sub3 = new FunctionExpression("SUBSTRING", List.of(new ColumnExpression("col"), new LiteralExpression(2), new LiteralExpression(10)));
        assertEquals("BCDE", sub3.evaluate(ctx));

        // Null
        Expression subNull = new FunctionExpression("SUBSTRING", List.of(new LiteralExpression(null), new LiteralExpression(1), new LiteralExpression(3)));
        assertNull(subNull.evaluate(ctx));
    }

    @Test
    public void testReplace() {
        RowContext ctx = col -> "A-B-C";
        // Simple replacement
        Expression rep1 = new FunctionExpression("REPLACE", List.of(new ColumnExpression("col"), new LiteralExpression("-"), new LiteralExpression("")));
        assertEquals("ABC", rep1.evaluate(ctx));

        // No occurrence
        Expression rep2 = new FunctionExpression("REPLACE", List.of(new ColumnExpression("col"), new LiteralExpression("x"), new LiteralExpression("y")));
        assertEquals("A-B-C", rep2.evaluate(ctx));

        // Null input
        Expression repNull = new FunctionExpression("REPLACE", List.of(new LiteralExpression(null), new LiteralExpression("-"), new LiteralExpression("")));
        assertNull(repNull.evaluate(ctx));
    }

    @Test
    public void testNestedSelectCombinations() {
        RowContext ctx = col -> "  AbCdEfG  ";
        
        // LOWER(TRIM(col)) -> "abcdefg"
        Expression trim = new FunctionExpression("TRIM", List.of(new ColumnExpression("col")));
        Expression lowerTrim = new FunctionExpression("LOWER", List.of(trim));
        assertEquals("abcdefg", lowerTrim.evaluate(ctx));

        // UPPER(SUBSTRING(TRIM(col), 3, 3)) -> UPPER("CdE") -> "CDE"
        Expression sub = new FunctionExpression("SUBSTRING", List.of(trim, new LiteralExpression(3), new LiteralExpression(3)));
        Expression upperSub = new FunctionExpression("UPPER", List.of(sub));
        assertEquals("CDE", upperSub.evaluate(ctx));

        // REPLACE(UPPER(TRIM(col)), "D", "X") -> REPLACE("ABCDEFG", "D", "X") -> "ABCXEFG"
        Expression upperTrim = new FunctionExpression("UPPER", List.of(trim));
        Expression replace = new FunctionExpression("REPLACE", List.of(upperTrim, new LiteralExpression("D"), new LiteralExpression("X")));
        assertEquals("ABCXEFG", replace.evaluate(ctx));
    }

    @Test
    public void testNestedWhereCombinations() {
        RowContext ctx = col -> "  Yes  ";

        // TRIM(LOWER(col)) = "yes"
        Expression trim = new FunctionExpression("TRIM", List.of(new ColumnExpression("col")));
        Expression lowerTrim = new FunctionExpression("LOWER", List.of(trim));
        Expression eq = new BinaryExpression(lowerTrim, new LiteralExpression("yes"), BinaryExpression.Operator.EQUALS);
        assertEquals(true, eq.evaluate(ctx));

        // NOT(UPPER(TRIM(col)) = "NO")
        Expression upperTrim = new FunctionExpression("UPPER", List.of(trim));
        Expression eqNo = new BinaryExpression(upperTrim, new LiteralExpression("NO"), BinaryExpression.Operator.EQUALS);
        Expression not = new NotExpression(eqNo);
        assertEquals(true, not.evaluate(ctx));

        // (LOWER(TRIM(col)) = "yes") AND (UPPER(TRIM(col)) = "YES")
        Expression eqYesUpper = new BinaryExpression(upperTrim, new LiteralExpression("YES"), BinaryExpression.Operator.EQUALS);
        Expression and = new LogicalExpression(eq, eqYesUpper, LogicalExpression.Operator.AND);
        assertEquals(true, and.evaluate(ctx));
    }
}
