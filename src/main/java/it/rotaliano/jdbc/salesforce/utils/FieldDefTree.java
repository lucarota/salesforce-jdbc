package it.rotaliano.jdbc.salesforce.utils;

import it.rotaliano.jdbc.salesforce.delegates.ForceResultField;
import it.rotaliano.jdbc.salesforce.delegates.PartnerResultToCartesianTable;
import it.rotaliano.jdbc.salesforce.statement.FieldDef;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FieldDefTree extends TreeNode<FieldDef> {

    @Getter
    @Setter
    private String relationshipName;

    private final List<FieldDef> sqlOrderFields = new ArrayList<>();

    /**
     * Records a field in SQL SELECT order. Called by SelectSpecVisitor as it
     * visits each SELECT item sequentially.
     */
    public void addSqlOrderField(FieldDef field) {
        sqlOrderFields.add(field);
    }

    public int getSqlOrderSize() {
        return sqlOrderFields.size();
    }

    @Override
    public List<FieldDef> flatten() {
        List<FieldDef> all = new ArrayList<>();
        if (getData() != null) {
            FieldDef self = getData();
            if (relationshipName != null) {
                self = new FieldDef(self.getName(), relationshipName + "." + self.getFullName(), self.getAlias(), self.getType());
            }
            all.add(self);
        }
        for (TreeNode<FieldDef> child : getChildren()) {
            List<FieldDef> childFlat = child.flatten();
            if (relationshipName != null) {
                childFlat = childFlat.stream()
                        .map(f -> f == null ? null : new FieldDef(f.getName(), relationshipName + "." + f.getFullName(), f.getAlias(), f.getType()))
                        .collect(Collectors.toList());
            }
            all.addAll(childFlat);
        }
        if (all.isEmpty()) {
            all.add(null);
        }
        return all;
    }

    /**
     * Returns flat field definitions in the original SQL SELECT order.
     * Relation fields that were grouped in the tree for Salesforce API compatibility
     * are reordered to match the SELECT clause. Subquery and function fields that
     * are not tracked in SQL order are appended at their natural tree position.
     */
    public List<FieldDef> flattenInSqlOrder() {
        if (sqlOrderFields.isEmpty()) {
            return flatten();
        }
        // Collect identity set of all SQL-tracked fields
        java.util.Set<FieldDef> tracked = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        tracked.addAll(sqlOrderFields);

        List<FieldDef> result = new ArrayList<>(sqlOrderFields);
        // Append fields from non-leaf children (subqueries) and any untracked leaf fields
        for (FieldDef treeField : flatten()) {
            if (treeField != null && !tracked.contains(treeField)) {
                result.add(treeField);
            }
        }
        return result;
    }

    private static final java.util.Set<String> FUNCTION_NAMES = new java.util.HashSet<>(java.util.Arrays.asList(
        "AVG", "COUNT", "COUNT_DISTINCT", "MIN", "MAX", "SUM",
        "CALENDAR_MONTH", "CALENDAR_QUARTER", "CALENDAR_YEAR",
        "DAY_IN_MONTH", "DAY_IN_WEEK", "DAY_IN_YEAR", "DAY_ONLY",
        "FISCAL_MONTH", "FISCAL_QUARTER", "FISCAL_YEAR",
        "HOUR_IN_DAY", "WEEK_IN_MONTH", "WEEK_IN_YEAR"
    ));

    private static boolean isFieldNameMatch(String schemaName, String rowName) {
        if (schemaName == null || rowName == null) {
            return false;
        }
        if (schemaName.equalsIgnoreCase(rowName)) {
            return true;
        }
        int firstDot = rowName.indexOf('.');
        if (firstDot != -1) {
            String strippedRowName = rowName.substring(firstDot + 1);
            if (schemaName.equalsIgnoreCase(strippedRowName)) {
                return true;
            }
        }
        // If rowName is expr0, expr1, etc., it matches any function/aggregate field
        if (rowName.toLowerCase().startsWith("expr")) {
            String upperSchema = schemaName.toUpperCase();
            if (upperSchema.contains("(") || FUNCTION_NAMES.contains(upperSchema)) {
                return true;
            }
            for (String func : FUNCTION_NAMES) {
                if (upperSchema.startsWith(func)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static List<List<ForceResultField>> expand(List<TreeNode<ForceResultField>> rows, FieldDefTree schema) {
        PartnerResultToCartesianTable<FieldDef, ForceResultField> expander = new PartnerResultToCartesianTable<>(schema,
            (s, row) -> {
                List<FieldDef> flatSchema = s.flatten();
                int schemaSize = flatSchema.size();
                java.util.List<ForceResultField> alignedRow = new java.util.ArrayList<>();
                java.util.Set<Integer> matchedIndices = new java.util.HashSet<>();
                for (FieldDef field : flatSchema) {
                    ForceResultField matched = null;
                    for (int j = 0; j < row.size(); j++) {
                        if (matchedIndices.contains(j)) {
                            continue;
                        }
                        ForceResultField rf = row.get(j);
                        if (rf != null && isFieldNameMatch(field.getFullName(), rf.getFullName())) {
                            matched = rf;
                            matchedIndices.add(j);
                            break;
                        }
                    }
                    if (matched != null) {
                        alignedRow.add(new ForceResultField(null, field.getType(), field.getFullName(), matched.getValue()));
                    } else {
                        alignedRow.add(new ForceResultField(null, field.getType(), field.getFullName(), null));
                    }
                }
                row.clear();
                row.addAll(alignedRow);
                return row;
            });
        return expander.expandOn(rows);
    }

}
