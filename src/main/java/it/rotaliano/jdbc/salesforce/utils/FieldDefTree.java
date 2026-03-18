package it.rotaliano.jdbc.salesforce.utils;

import it.rotaliano.jdbc.salesforce.delegates.ForceResultField;
import it.rotaliano.jdbc.salesforce.delegates.PartnerResultToCartesianTable;
import it.rotaliano.jdbc.salesforce.statement.FieldDef;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FieldDefTree extends TreeNode<FieldDef> {

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

    public static List<List<ForceResultField>> expand(List<TreeNode<ForceResultField>> rows, FieldDefTree schema) {
        PartnerResultToCartesianTable<FieldDef, ForceResultField> expander = new PartnerResultToCartesianTable<>(schema,
            (s, row) -> {
                int schemaSize = s.getChildrenCount();
                int rowSize = row.size();
                if (schemaSize > rowSize) {
                    /* Remove relation with missing values */
                    Iterator<ForceResultField> ii = row.iterator();
                    while (ii.hasNext()) {
                        String filedName = ii.next().getFullName();
                        boolean found = false;
                        for (int i = 0; i < schemaSize; i++) {
                            TreeNode<FieldDef> child = schema.getChild(i);
                            FieldDef field = child.getData();
                            if (field != null && field.getFullName() != null && field.getFullName().equalsIgnoreCase(filedName)) {
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            ii.remove();
                        }
                    }
                    for (int i = 0; i < schemaSize; i++) {
                        FieldDef field = schema.getChild(i).getData();
                        if (row.size() == i) {
                            row.add(new ForceResultField(field.getEntity(), field.getType(), field.getName(), null));
                        } else if (!field.getFullName().equalsIgnoreCase(row.get(i).getFullName())) {
                            row.add(i, new ForceResultField(field.getEntity(), field.getType(), field.getName(), null));
                        }
                    }
                }
                return row;
            });
        return expander.expandOn(rows);
    }

}
