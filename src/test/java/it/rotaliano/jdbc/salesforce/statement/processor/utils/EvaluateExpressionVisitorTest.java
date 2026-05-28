package it.rotaliano.jdbc.salesforce.statement.processor.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import it.rotaliano.jdbc.salesforce.exceptions.UnsupportedArgumentTypeException;
import java.util.Map;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import net.sf.jsqlparser.schema.Column;


public class EvaluateExpressionVisitorTest {

    static class EvaluateExpressionVisitorTesting extends EvaluateExpressionVisitor {
        EvaluateExpressionVisitorTesting(Map<String, Object> recordFieldsFromDB) {
            super(recordFieldsFromDB);
        }

        public <S> Expression visit(Function function, S context) {
            if ("now".equalsIgnoreCase(function.getName())) {
                result = "2021-Test-Day 12:12:12";
            } else {
                super.visit(function);
            }
            return null;
        }
    }

    @Test
    public void testResolveSimpleTypes() {
        Map<String, Object> recordFieldsFromDB = RecordFieldsBuilder.setId("001xx1111111111111").build();

        EvaluateExpressionVisitor visitorS1 = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new StringValue("TestValue-String").accept(visitorS1);
        assertEquals("TestValue-String", visitorS1.getResult());

        EvaluateExpressionVisitor visitorD1 = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new DoubleValue("1.7976931348623157e+308").accept(visitorD1);
        assertEquals(Double.MAX_VALUE, visitorD1.getResult());

        EvaluateExpressionVisitor visitorL1 = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new LongValue("9223372036854775807").accept(visitorL1);
        assertEquals(9223372036854775807L, visitorL1.getResult());

        EvaluateExpressionVisitor visitorTS1 = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new TimestampValue("'2021-10-25 07:19:33'").accept(visitorTS1);
        assertEquals(java.sql.Timestamp.valueOf("2021-10-25 07:19:33"), visitorTS1.getResult());

        EvaluateExpressionVisitor visitorDate1 = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new DateValue("'1964-10-27'").accept(visitorDate1);
        assertEquals(java.sql.Date.valueOf("1964-10-27"), visitorDate1.getResult());

        EvaluateExpressionVisitor visitorT1 = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new TimeValue("'03:26:57'").accept(visitorT1);
        assertEquals(java.sql.Time.valueOf("03:26:57"), visitorT1.getResult());

    }

    @Test
    public void testResolveAdditions_Dates() {
        Map<String, Object> recordFieldsFromDB = RecordFieldsBuilder.setId("001xx1111111111111").build();

        EvaluateExpressionVisitor visitorTS1 = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Addition()
                .withLeftExpression( new TimestampValue("'2021-10-25 07:19:33'"))
                .withRightExpression(new LongValue("1"))
                .accept(visitorTS1);
        assertEquals(java.sql.Timestamp.valueOf("2021-10-26 07:19:33"), visitorTS1.getResult());

        EvaluateExpressionVisitor visitorDate1 = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Addition()
                .withLeftExpression( new DateValue("'1964-10-27'"))
                .withRightExpression(new LongValue("1"))
                .accept(visitorDate1);
        assertEquals(java.sql.Date.valueOf("1964-10-28"), visitorDate1.getResult());

        EvaluateExpressionVisitor visitorT1 = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Addition()
                .withLeftExpression( new TimeValue("'03:26:57'"))
                .withRightExpression(new DoubleValue("0.000011574")) // 1 second = 1/86400 of day = 1/(24*60*60) of day
                .accept(visitorT1);
        assertEquals(java.sql.Time.valueOf("03:26:58"), visitorT1.getResult());
    }

    @Test
    public void testResolveAdditions_SimpleTypes() {
        Map<String, Object> recordFieldsFromDB = RecordFieldsBuilder.setId("001xx1111111111111").build();

        EvaluateExpressionVisitor visitorSS = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Addition()
                .withLeftExpression( new StringValue("TestPartLeft-"))
                .withRightExpression(new StringValue("TestPartRight~"))
                .accept(visitorSS);
        assertEquals("TestPartLeft-TestPartRight~", visitorSS.getResult());

        EvaluateExpressionVisitor visitorDD = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Addition()
                .withLeftExpression( new DoubleValue("12345678.1231"))
                .withRightExpression(new DoubleValue("88456634.4333"))
                .accept(visitorDD);
        assertEquals(100802312.5564, visitorDD.getResult());

        EvaluateExpressionVisitor visitorLL = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Addition()
                .withLeftExpression( new LongValue("89983422234"))
                .withRightExpression(new LongValue("76463763473"))
                .accept(visitorLL);
        assertEquals(166447185707L, visitorLL.getResult());

        EvaluateExpressionVisitor visitorSNL = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Addition()
                .withLeftExpression( new StringValue("89983422234"))
                .withRightExpression(new LongValue("76463763473"))
                .accept(visitorSNL);
        assertEquals(166447185707L, visitorSNL.getResult());

        EvaluateExpressionVisitor visitorSL = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Addition()
                .withLeftExpression( new StringValue("StrNotNum"))
                .withRightExpression(new LongValue("76463763473"))
                .accept(visitorSL);
        assertEquals("StrNotNum76463763473", visitorSL.getResult());

        EvaluateExpressionVisitor visitorLS = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Addition()
                .withLeftExpression( new LongValue("89983422234"))
                .withRightExpression(new StringValue("76463763473"))
                .accept(visitorLS);
        assertEquals(166447185707L, visitorLS.getResult());

        EvaluateExpressionVisitor visitorLSN = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Addition()
                .withLeftExpression( new LongValue("89983422234"))
                .withRightExpression(new StringValue("StrNotNum"))
                .accept(visitorLSN);
        assertEquals("89983422234StrNotNum", visitorLSN.getResult());

        EvaluateExpressionVisitor visitorDS = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Addition()
                .withLeftExpression( new DoubleValue("12345678.1231"))
                .withRightExpression(new StringValue("88456634.4333"))
                .accept(visitorDS);
        assertEquals(100802312.5564, visitorDS.getResult());

        EvaluateExpressionVisitor visitorDSN = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Addition()
                .withLeftExpression( new DoubleValue("12345678.1231"))
                .withRightExpression(new StringValue("StrNotNum"))
                .accept(visitorDSN);
        assertEquals("1.23456781231E7StrNotNum", visitorDSN.getResult());

        EvaluateExpressionVisitor visitorSD = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Addition()
                .withLeftExpression( new StringValue("12345678.1231"))
                .withRightExpression(new DoubleValue("88456634.4333"))
                .accept(visitorSD);
        assertEquals(100802312.5564, visitorSD.getResult());

        EvaluateExpressionVisitor visitorSND = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Addition()
                .withLeftExpression( new StringValue("StrNotNum"))
                .withRightExpression(new DoubleValue("88456634.4333"))
                .accept(visitorSND);
        assertEquals("StrNotNum8.84566344333E7", visitorSND.getResult());

    }

    @Test
    public void testResolveSubstractions_Dates() {
        Map<String, Object> recordFieldsFromDB = RecordFieldsBuilder.setId("001xx1111111111111").build();

        EvaluateExpressionVisitor visitorTS1 = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Subtraction()
                .withLeftExpression( new TimestampValue("'2021-10-25 07:19:33'"))
                .withRightExpression(new LongValue("1"))
                .accept(visitorTS1);
        assertEquals(java.sql.Timestamp.valueOf("2021-10-24 07:19:33"), visitorTS1.getResult());

        EvaluateExpressionVisitor visitorDate1 = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Subtraction()
                .withLeftExpression( new DateValue("'1964-10-27'"))
                .withRightExpression(new LongValue("1"))
                .accept(visitorDate1);
        assertEquals(java.sql.Date.valueOf("1964-10-26"), visitorDate1.getResult());

        EvaluateExpressionVisitor visitorT1 = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Subtraction()
                .withLeftExpression( new TimeValue("'03:26:57'"))
                .withRightExpression(new DoubleValue("0.000011574")) // 1 second = 1/86400 of day = 1/(24*60*60) of day
                .accept(visitorT1);
        assertEquals(java.sql.Time.valueOf("03:26:56"), visitorT1.getResult());
    }

    @Test
    public void testResolveSubstractions_SimpleTypes() {
        Map<String, Object> recordFieldsFromDB = RecordFieldsBuilder.setId("001xx1111111111111").build();

        EvaluateExpressionVisitor visitorSS = new EvaluateExpressionVisitor(recordFieldsFromDB);
        try {
            new Subtraction()
                    .withLeftExpression(new StringValue("TestPartLeft-"))
                    .withRightExpression(new StringValue("TestPartRight~"))
                    .accept(visitorSS);
            assertNull(visitorSS.getResult()); // Not supported!
        } catch (UnsupportedArgumentTypeException e) {
            // It's right!
        }

        EvaluateExpressionVisitor visitorDD = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Subtraction()
                .withLeftExpression( new DoubleValue("12345678.1231"))
                .withRightExpression(new DoubleValue("88456634.4333"))
                .accept(visitorDD);
        assertEquals(-7.61109563102E7, visitorDD.getResult());

        EvaluateExpressionVisitor visitorLL = new EvaluateExpressionVisitor(recordFieldsFromDB);
        new Subtraction()
                .withLeftExpression( new LongValue("89983422234"))
                .withRightExpression(new LongValue("76463763473"))
                .accept(visitorLL);
        assertEquals(13519658761L, visitorLL.getResult());

        EvaluateExpressionVisitor visitorSNL = new EvaluateExpressionVisitor(recordFieldsFromDB);
        try {
            new Subtraction()
                    .withLeftExpression( new StringValue("89983422234"))
                    .withRightExpression(new LongValue("76463763473"))
                    .accept(visitorSNL);
                assertNull(visitorSS.getResult()); // Not supported!
        } catch (UnsupportedArgumentTypeException e) {
            // It's right!
        }

        EvaluateExpressionVisitor visitorSL = new EvaluateExpressionVisitor(recordFieldsFromDB);
        try {
            new Subtraction()
                    .withLeftExpression( new StringValue("StrNotNum"))
                    .withRightExpression(new LongValue("76463763473"))
                    .accept(visitorSL);
            assertNull(visitorSS.getResult()); // Not supported!
        } catch (UnsupportedArgumentTypeException e) {
            // It's right!
        }

        EvaluateExpressionVisitor visitorLS = new EvaluateExpressionVisitor(recordFieldsFromDB);
        try {
            new Subtraction()
                    .withLeftExpression( new LongValue("89983422234"))
                    .withRightExpression(new StringValue("76463763473"))
                    .accept(visitorLS);
            assertNull(visitorSS.getResult()); // Not supported!
        } catch (UnsupportedArgumentTypeException e) {
            // It's right!
        }

        EvaluateExpressionVisitor visitorLSN = new EvaluateExpressionVisitor(recordFieldsFromDB);
        try {
            new Subtraction()
                    .withLeftExpression( new LongValue("89983422234"))
                    .withRightExpression(new StringValue("StrNotNum"))
                    .accept(visitorLSN);
            assertNull(visitorSS.getResult()); // Not supported!
        } catch (UnsupportedArgumentTypeException e) {
            // It's right!
        }

        EvaluateExpressionVisitor visitorDS = new EvaluateExpressionVisitor(recordFieldsFromDB);
        try {
            new Subtraction()
                    .withLeftExpression( new DoubleValue("12345678.1231"))
                    .withRightExpression(new StringValue("88456634.4333"))
                    .accept(visitorDS);
            assertNull(visitorSS.getResult()); // Not supported!
        } catch (UnsupportedArgumentTypeException e) {
            // It's right!
        }

        EvaluateExpressionVisitor visitorDSN = new EvaluateExpressionVisitor(recordFieldsFromDB);
        try {
            new Subtraction()
                    .withLeftExpression( new DoubleValue("12345678.1231"))
                    .withRightExpression(new StringValue("StrNotNum"))
                    .accept(visitorDSN);
            assertNull(visitorSS.getResult()); // Not supported!
        } catch (UnsupportedArgumentTypeException e) {
            // It's right!
        }

        EvaluateExpressionVisitor visitorSD = new EvaluateExpressionVisitor(recordFieldsFromDB);
        try {
            new Subtraction()
                    .withLeftExpression( new StringValue("12345678.1231"))
                    .withRightExpression(new DoubleValue("88456634.4333"))
                    .accept(visitorSD);
            assertNull(visitorSS.getResult()); // Not supported!
        } catch (UnsupportedArgumentTypeException e) {
            // It's right!
        }

        EvaluateExpressionVisitor visitorSND = new EvaluateExpressionVisitor(recordFieldsFromDB);
        try {
            new Subtraction()
                    .withLeftExpression( new StringValue("StrNotNum"))
                    .withRightExpression(new DoubleValue("88456634.4333"))
                    .accept(visitorSND);
            assertNull(visitorSS.getResult()); // Not supported!
        } catch (UnsupportedArgumentTypeException e) {
            // It's right!
        }
    }

    @Test
    public void testResolveFunctions() {
        Map<String, Object> recordFieldsFromDB = RecordFieldsBuilder.setId("001xx1111111111111").build();

        EvaluateExpressionVisitor visitorS1 = new EvaluateExpressionVisitorTesting(recordFieldsFromDB);
        Function func = new Function();
        func.setName("now");
        func.accept(visitorS1);
        assertEquals("2021-Test-Day 12:12:12", visitorS1.getResult());
    }

    @Test
    public void testResolveCaseExpressions() {
        // Simple CASE: switch on Amount
        CaseExpression simpleCase = new CaseExpression()
                .withSwitchExpression(new Column("Amount"))
                .withWhenClauses(List.of(
                        new WhenClause()
                                .withWhenExpression(new LongValue(1000))
                                .withThenExpression(new StringValue("THOUSAND")),
                        new WhenClause()
                                .withWhenExpression(new LongValue(2000))
                                .withThenExpression(new StringValue("TWO_THOUSAND"))
                ))
                .withElseExpression(new StringValue("OTHER"));

        // Match 1000
        Map<String, Object> dbFields1 = RecordFieldsBuilder.setId("1").build();
        dbFields1.put("Amount", 1000);
        EvaluateExpressionVisitor visitor1 = new EvaluateExpressionVisitor(dbFields1);
        simpleCase.accept(visitor1);
        assertEquals("THOUSAND", visitor1.getResult());

        // Match 2000
        Map<String, Object> dbFields2 = RecordFieldsBuilder.setId("2").build();
        dbFields2.put("Amount", 2000.0); // as double
        EvaluateExpressionVisitor visitor2 = new EvaluateExpressionVisitor(dbFields2);
        simpleCase.accept(visitor2);
        assertEquals("TWO_THOUSAND", visitor2.getResult());

        // Match else
        Map<String, Object> dbFields3 = RecordFieldsBuilder.setId("3").build();
        dbFields3.put("Amount", 3000);
        EvaluateExpressionVisitor visitor3 = new EvaluateExpressionVisitor(dbFields3);
        simpleCase.accept(visitor3);
        assertEquals("OTHER", visitor3.getResult());

        // Searched CASE: WHEN Amount > 1000 THEN 'BIG' ELSE 'SMALL'
        CaseExpression searchedCase = new CaseExpression()
                .withWhenClauses(List.of(
                        new WhenClause()
                                .withWhenExpression(new GreaterThan()
                                        .withLeftExpression(new Column("Amount"))
                                        .withRightExpression(new LongValue(1000)))
                                .withThenExpression(new StringValue("BIG"))
                ))
                .withElseExpression(new StringValue("SMALL"));

        // Greater than 1000
        Map<String, Object> dbFields4 = RecordFieldsBuilder.setId("4").build();
        dbFields4.put("Amount", 1500);
        EvaluateExpressionVisitor visitor4 = new EvaluateExpressionVisitor(dbFields4);
        searchedCase.accept(visitor4);
        assertEquals("BIG", visitor4.getResult());

        // Less than 1000
        Map<String, Object> dbFields5 = RecordFieldsBuilder.setId("5").build();
        dbFields5.put("Amount", 500);
        EvaluateExpressionVisitor visitor5 = new EvaluateExpressionVisitor(dbFields5);
        searchedCase.accept(visitor5);
        assertEquals("SMALL", visitor5.getResult());
    }
}
