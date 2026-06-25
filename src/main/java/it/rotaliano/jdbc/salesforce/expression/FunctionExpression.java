package it.rotaliano.jdbc.salesforce.expression;

import java.util.List;

public class FunctionExpression implements Expression {
    private final String name;
    private final List<Expression> arguments;

    public FunctionExpression(String name, List<Expression> arguments) {
        this.name = name.toUpperCase();
        this.arguments = arguments;
    }

    @Override
    public Object evaluate(RowContext row) {
        if (arguments == null || arguments.isEmpty()) {
            return null;
        }
        Object val0 = arguments.get(0).evaluate(row);
        if (val0 == null) {
            return null;
        }
        String str = val0.toString();

        switch (name) {
            case "UPPER":
                return str.toUpperCase();
            case "LOWER":
                return str.toLowerCase();
            case "TRIM":
                return str.trim();
            case "SUBSTRING":
                if (arguments.size() < 2) {
                    return null;
                }
                Object startVal = arguments.get(1).evaluate(row);
                if (startVal == null) {
                    return null;
                }
                int start = ((Number) startVal).intValue();
                int length = Integer.MAX_VALUE;
                if (arguments.size() >= 3) {
                    Object lenVal = arguments.get(2).evaluate(row);
                    if (lenVal != null) {
                        length = ((Number) lenVal).intValue();
                    }
                }
                if (length < 0) {
                    return "";
                }
                int startIdx = Math.max(0, start - 1);
                if (startIdx >= str.length()) {
                    return "";
                }
                if (length > str.length() - startIdx) {
                    length = str.length() - startIdx;
                }
                int endIdx = startIdx + length;
                if (endIdx < startIdx) {
                    return "";
                }
                return str.substring(startIdx, endIdx);
            case "REPLACE":
                if (arguments.size() < 3) {
                    return str;
                }
                Object searchVal = arguments.get(1).evaluate(row);
                Object replVal = arguments.get(2).evaluate(row);
                if (searchVal == null || replVal == null) {
                    return null;
                }
                return str.replace(searchVal.toString(), replVal.toString());
            default:
                throw new UnsupportedOperationException("Function " + name + " not supported");
        }
    }
}
