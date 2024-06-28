package com.ascendix.jdbc.salesforce.utils;

import com.ascendix.jdbc.salesforce.delegates.ForceResultField;
import com.ascendix.jdbc.salesforce.delegates.PartnerResultToCartesianTable;
import com.ascendix.jdbc.salesforce.statement.FieldDef;

import java.util.Iterator;
import java.util.List;

public class FieldDefTree extends TreeNode<FieldDef> {

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
                            if (field != null && field.getFullName() != null && field.getFullName().equals(filedName)) {
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
                        if (!field.getFullName().equals(row.get(i).getFullName())) {
                            row.add(i, new ForceResultField(field.getEntity(), field.getType(), field.getName(), null));
                        }
                    }
                }
                return row;
            });
        return expander.expandOn(rows);
    }

}
