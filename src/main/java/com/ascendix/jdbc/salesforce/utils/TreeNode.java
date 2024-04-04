package com.ascendix.jdbc.salesforce.utils;


import com.ascendix.jdbc.salesforce.statement.FieldDef;

import java.util.ArrayList;
import java.util.List;


public class TreeNode<T> {

    private final T data;
    private final List<TreeNode<T>> children;

    public TreeNode() {
        this(null);
    }
    public TreeNode(T data) {
        this.data = data;
        this.children = new ArrayList<>();
    }

    public int getChildrenCount() {
        return children.size();
    }

    public TreeNode<T> getChild(int index) {
        return children.get(index);
    }

    public boolean isLeaf() {
        return children.isEmpty();
    }

    public TreeNode<T> addChild(T child) {
        TreeNode<T> childNode = new TreeNode<T>(child);
        this.children.add(childNode);
        return childNode;
    }

    public void addTree(TreeNode<T> tree) {
        this.children.add(tree);
    }

    public List<T> flatten() {
        ArrayList<T> all = new ArrayList<>();
        if (data != null) {
            all.add(data);
        }
        for (TreeNode<T> child : children) {
            all.addAll(child.flatten());
        }
        return all;
    }
}