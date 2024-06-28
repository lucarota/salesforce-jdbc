package com.ascendix.jdbc.salesforce.delegates;

import com.ascendix.jdbc.salesforce.utils.TreeNode;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class PartnerResultToCartesianTableTest {

    @Test
    public void testExpandSimple() {
        TreeNode<Object> schema = new TreeNode<>();
        schema.addChild(new Object());
        schema.addChild(new Object());
        schema.addChild(new Object());
        schema.addChild(new Object());

        List<List<Integer>> expected = List.of(Arrays.asList(1, 2, 3, 4));

        TreeNode<Integer> row = new TreeNode<>();
        row.addChild(1);
        row.addChild(2);
        row.addChild(3);
        row.addChild(4);

        List<TreeNode<Integer>> rows = new ArrayList<>();
        rows.add(row);

        PartnerResultToCartesianTable<Object, Integer> p = new PartnerResultToCartesianTable<>(schema, (s, r) -> r);

        List<List<Integer>> actual = p.expandOn(rows);

        assertEquals(expected, actual);
    }

    @Test
    public void testExpandWhenNothingToExpand() {
        TreeNode<Object> schema = new TreeNode<>();
        schema.addChild(new Object());
        schema.addChild(new Object());
        schema.addChild(new Object());
        schema.addChild(new Object());

        List<List<String>> expected = Arrays.asList(Arrays.asList("11", "12", "13", "14"),
                Arrays.asList("21", "22", "23", "24"), Arrays.asList("31", "32", "33", "34"));

        TreeNode<String> row1 = new TreeNode<>();
        row1.addChild("11");
        row1.addChild("12");
        row1.addChild("13");
        row1.addChild("14");

        TreeNode<String> row2 = new TreeNode<>();
        row2.addChild("21");
        row2.addChild("22");
        row2.addChild("23");
        row2.addChild("24");

        TreeNode<String> row3 = new TreeNode<>();
        row3.addChild("31");
        row3.addChild("32");
        row3.addChild("33");
        row3.addChild("34");

        List<TreeNode<String>> rows = new ArrayList<>();
        rows.add(row1);
        rows.add(row2);
        rows.add(row3);

        PartnerResultToCartesianTable<Object, String> p = new PartnerResultToCartesianTable<>(schema, (s, r) -> r);
        List<List<String>> actual = p.expandOn(rows);

        assertEquals(expected, actual);
    }

    @Test
    public void testExpandWhenOneNestedList() {
        TreeNode<Object> schema = new TreeNode<>();
        schema.addChild(new Object());
        schema.addChild(null).addChild(new Object()).addChild(new Object()).addChild(new Object());
        schema.addChild(new Object());
        schema.addChild(new Object());

        TreeNode<String> row = new TreeNode<>();
        row.addChild("1");
        TreeNode<String> node = new TreeNode<>();
        node.addChild("21");
        node.addChild("22");
        node.addChild("23");
        row.addTreeNode(node);
        row.addChild("3");
        row.addChild("4");
        List<TreeNode<String>> rows = new ArrayList<>();
        rows.add(row);

        PartnerResultToCartesianTable<Object, String> p = new PartnerResultToCartesianTable<>(schema, (s, r) -> r);
        List<List<String>> actual = p.expandOn(rows);

        List<List<String>> expected = Arrays.asList(Arrays.asList("1", "21", "3", "4"),
                Arrays.asList("1", "22", "3", "4"), Arrays.asList("1", "23", "3", "4"));
        assertEquals(expected, actual);
    }

    @Test
    public void testExpandWhenTwoNestedListAndOneRow() {
        TreeNode<Object> schema = new TreeNode<>();
        schema.addChild(new Object());
        TreeNode<Object> node2 = schema.addChild(null);
        node2.addChild(new Object());
        node2.addChild(new Object());
        schema.addChild(new Object());
        TreeNode<Object> node4 = schema.addChild(null);
        node4.addChild(new Object());
        node4.addChild(new Object());

        TreeNode<Integer> subrow11 = new TreeNode<>();
        subrow11.addChild(1);
        subrow11.addChild(2);

        TreeNode<Integer> subrow12 = new TreeNode<>();
        subrow12.addChild(3);
        subrow12.addChild(4);

        TreeNode<Integer> subrow1 = new TreeNode<>();
        subrow1.addTreeNode(subrow11);
        subrow1.addTreeNode(subrow12);

        TreeNode<Integer> subrow21 = new TreeNode<>();
        subrow21.addChild(5);
        subrow21.addChild(6);

        TreeNode<Integer> subrow22 = new TreeNode<>();
        subrow22.addChild(7);
        subrow22.addChild(8);

        TreeNode<Integer> subrow2 = new TreeNode<>();
        subrow2.addTreeNode(subrow21);
        subrow2.addTreeNode(subrow22);

        TreeNode<Integer> row = new TreeNode<>();
        row.addChild(11);
        row.addTreeNode(subrow1);
        row.addChild(12);
        row.addTreeNode(subrow2);

        List<TreeNode<Integer>> rows = new ArrayList<>();
        rows.add(row);

        List<List<Integer>> expected = Arrays.asList(Arrays.asList(11, 1, 2, 12, 5, 6),
                Arrays.asList(11, 3, 4, 12, 5, 6), Arrays.asList(11, 1, 2, 12, 7, 8),
                Arrays.asList(11, 3, 4, 12, 7, 8));

        PartnerResultToCartesianTable<Object, Integer> p = new PartnerResultToCartesianTable<>(schema, (s, r) -> r);
        List<List<Integer>> actual = p.expandOn(rows);

        assertEquals(expected.size(), actual.size());
        for (List<Integer> l : expected) {
            assertTrue(actual.contains(l));
        }
    }

    @Test
    public void testExpandWhenOneNestedListAndTwoRows() {
        TreeNode<Object> schema = new TreeNode<>();
        schema.addChild(new Object());
        TreeNode<Object> node = schema.addChild(null);
        node.addChild(new Object());
        node.addChild(new Object());
        schema.addChild(new Object());
        schema.addChild(new Object());

        TreeNode<Integer> subrow11 = new TreeNode<>();
        subrow11.addChild(1);
        subrow11.addChild(2);

        TreeNode<Integer> subrow12 = new TreeNode<>();
        subrow12.addChild(3);
        subrow12.addChild(4);

        TreeNode<Integer> subrow1 = new TreeNode<>();
        subrow1.addTreeNode(subrow11);
        subrow1.addTreeNode(subrow12);

        TreeNode<Integer> row1 = new TreeNode<>();
        row1.addChild(11);
        row1.addTreeNode(subrow1);
        row1.addChild(12);
        row1.addChild(13);

        TreeNode<Integer> subrow21 = new TreeNode<>();
        subrow21.addChild(21);
        subrow21.addChild(22);

        TreeNode<Integer> subrow22 = new TreeNode<>();
        subrow22.addChild(23);
        subrow22.addChild(24);

        TreeNode<Integer> subrow23 = new TreeNode<>();
        subrow23.addChild(25);
        subrow23.addChild(26);

        TreeNode<Integer> subrow2 = new TreeNode<>();
        subrow2.addTreeNode(subrow21);
        subrow2.addTreeNode(subrow22);
        subrow2.addTreeNode(subrow23);

        TreeNode<Integer> row2 = new TreeNode<>();
        row2.addChild(20);
        row2.addTreeNode(subrow2);
        row2.addChild(41);
        row2.addChild(42);

        List<TreeNode<Integer>> rows = new ArrayList<>();
        rows.add(row1);
        rows.add(row2);
        log.info(row1.toTree());
        log.info(row2.toTree());
        PartnerResultToCartesianTable<Object, Integer> p = new PartnerResultToCartesianTable<>(schema, (s, r) -> r);
        List<List<Integer>> actual = p.expandOn(rows);

        List<List<Integer>> expected = Arrays.asList(Arrays.asList(11, 1, 2, 12, 13), Arrays.asList(11, 3, 4, 12, 13),
                Arrays.asList(20, 21, 22, 41, 42), Arrays.asList(20, 23, 24, 41, 42),
                Arrays.asList(20, 25, 26, 41, 42));
        assertEquals(expected, actual);
    }

    @Test
    public void testExpandWhenOneNestedListIsEmpty() {
        TreeNode<Object> schema = new TreeNode<>();
        schema.addChild(new Object());
        TreeNode<Object> node = schema.addChild(null);
        node.addChild(new Object());
        node.addChild(new Object());
        schema.addChild(new Object());
        schema.addChild(new Object());

        TreeNode<Integer> row = new TreeNode<>();
        row.addChild(11);
        row.addTreeNode(new TreeNode<>());
        row.addChild(12);
        row.addChild(13);

        List<TreeNode<Integer>> rows = new ArrayList<>();
        rows.add(row);

        PartnerResultToCartesianTable<Object, Integer> p = new PartnerResultToCartesianTable<>(schema, (s, r) -> r);
        List<List<Integer>> actual = p.expandOn(rows);

        List<List<Integer>> expected = List.of(Arrays.asList(11, null, null, 12, 13));
        assertEquals(expected, actual);
    }

    @Test
    public void testExpandWhenNestedListIsEmpty() {
        TreeNode<Object> schema = new TreeNode<>();
        schema.addChild(new Object());
        TreeNode<Object> node = schema.addChild(null);
        node.addChild(new Object());
        node.addChild(new Object());

        TreeNode<Integer> row = new TreeNode<>();
        row.addChild(11);
        row.addTreeNode(new TreeNode<>());

        List<TreeNode<Integer>> rows = new ArrayList<>();
        rows.add(row);

        PartnerResultToCartesianTable<Object, Integer> p = new PartnerResultToCartesianTable<>(schema, (s, r) -> r);
        List<List<Integer>> actual = p.expandOn(rows);

        List<List<Integer>> expected = List.of(Arrays.asList(11, null, null));
        assertEquals(expected, actual);
    }

}
