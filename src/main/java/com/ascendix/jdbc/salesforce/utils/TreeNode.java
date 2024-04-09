package com.ascendix.jdbc.salesforce.utils;


import lombok.Getter;

import java.util.*;


public class TreeNode<T> {

    @Getter
    private final T data;
    @Getter
    private final List<TreeNode<T>> children;

    public TreeNode() {
        this((T)null);
    }
    public TreeNode(T data) {
        this.data = data;
        this.children = new ArrayList<>();
    }

    public TreeNode(List<TreeNode<T>> children) {
        this.data = null;
        this.children = children;
    }

    public static <T> TreeNode<T> nCopies(int size, T o) {
        TreeNode<T> node = new TreeNode<>();
        node.children.addAll(Collections.nCopies(size, new TreeNode<>()));
        return node;
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

    public TreeNode<T> addChild(T child, int position) {
        TreeNode<T> childNode = new TreeNode<T>(child);
        this.children.add(position, childNode);
        return childNode;
    }

    public TreeNode<T> removeChild(int position) {
        return this.children.remove(position);
    }

    public void addTreeNode(TreeNode<T> tree) {
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
        if (all.isEmpty()) {
            all.add(null);
        }
        return all;
    }

    public TreeNode<T> deepCopy() {
        TreeNode<T> node = new TreeNode<>(data);
        for (TreeNode<T> child : children) {
            node.addTreeNode(child.deepCopy());
        }
        return node;
    }

    @Override
    public String toString() {
        return "Data: " + data + " Children: " + children.size();
    }

    public String toTree() {
        return toTree(this, 0, false);
    }

    public String toTree(TreeNode<T> root, int level, boolean isLast) {
        if (root == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("    ".repeat(Math.max(0, level - 1)));

        if (level > 0) {
            sb.append(" ".repeat(level));
            if (isLast) {
                sb.append("└── ");
            } else {
                sb.append("├── ");
            }
        }
        sb.append(root.data != null ? root.data : "<o>").append(System.lineSeparator());

        for (int i = 0; i < root.getChildrenCount(); i++) {
            TreeNode<T> child = root.getChild(i);
            sb.append(toTree(child, level + 1, i == root.getChildrenCount() - 1));
        }
        return sb.toString();
    }

}