package com.ascendix.jdbc.salesforce.utils;

import com.ascendix.jdbc.salesforce.delegates.ForceResultField;
import com.ascendix.jdbc.salesforce.delegates.PartnerResultToCartesianTable;
import com.ascendix.jdbc.salesforce.statement.FieldDef;

import java.util.List;

public class FieldDefTree extends TreeNode<FieldDef> {

    public static List<List<ForceResultField>> expand(List<TreeNode<ForceResultField>> rows, FieldDefTree schema) {
        PartnerResultToCartesianTable<FieldDef, ForceResultField> expander = new PartnerResultToCartesianTable<>(schema);
        return expander.expandOn(rows);
    }

}
