package com.ascendix.jdbc.salesforce.delegates;

import com.ascendix.jdbc.salesforce.utils.TreeNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PartnerResultToCartesianTable<S, R> {

    private final TreeNode<S> schema;

    public PartnerResultToCartesianTable(TreeNode<S> schema) {
        this.schema = schema;
    }

    public List<List<R>> expandOn(List<TreeNode<R>> rows) {
        return expandOn(rows, 0,0);
    }

    public List<List<R>> expandOn(List<TreeNode<R>> rows, int columnPosition, int schemaPosition) {
        return rows.stream()
            .map(row -> expandRow(row, columnPosition, schemaPosition))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    private List<List<R>> expandRow(TreeNode<R> row, int columnPosition, int schemaPosition) {
        if (schemaPosition > schema.getChildrenCount() - 1) {
            return List.of(row.flatten());
        } else if (schema.getChild(schemaPosition).isLeaf()) {
            return expandOn(List.of(row), columnPosition + 1, schemaPosition + 1);
        } else {
            List<TreeNode<R>> result = new ArrayList<>();
            int nestedListSize = schema.getChild(schemaPosition).getChildrenCount();
            TreeNode<R> value = row.getChild(columnPosition);
            if (value.getChildrenCount() > 0) {
                value.getChildren().forEach(item -> result.add(expandRow(row, item, columnPosition)));
            } else {
                result.add(expandRow(row, TreeNode.nCopies(nestedListSize, null), columnPosition));
            }
            return expandOn(result, columnPosition + nestedListSize, schemaPosition + 1);
        }
    }

    private TreeNode<R> expandRow(TreeNode<R> row, TreeNode<R> nestedItem, int position) {
        List<R> nestedItemsToInsert = nestedItem.flatten();
        TreeNode<R> newTree = row.deepCopy();
        newTree.removeChild(position);
        AtomicInteger i = new AtomicInteger(position);
        nestedItemsToInsert.forEach(item -> newTree.addChild(item, i.getAndIncrement()));
        return newTree;
    }
}
