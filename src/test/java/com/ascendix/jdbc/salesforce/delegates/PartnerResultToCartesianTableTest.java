package com.ascendix.jdbc.salesforce.delegates;

import com.ascendix.jdbc.salesforce.statement.FieldDef;
import com.ascendix.jdbc.salesforce.utils.FieldDefTree;
import com.ascendix.jdbc.salesforce.utils.TreeNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartnerResultToCartesianTableTest {

    @Test
    public void testExpandSimple() {
        FieldDefTree schema = new FieldDefTree();
        schema.addChild(new FieldDef("name", "", "string"));
        schema.addChild(new FieldDef("name", "", "string"));
        schema.addChild(new FieldDef("name", "", "string"));
        schema.addChild(new FieldDef("name", "", "string"));

        List<List> expected = Arrays.asList(
            Arrays.asList(1, 2, 3, 4)
        );

        List<List> actual = PartnerResultToCartesianTable.expand(expected, schema);

        assertEquals(expected, actual);
    }

    @Test
    public void testExpandWhenNothingToExpand() {
        FieldDefTree schema = new FieldDefTree();
        schema.addChild(new FieldDef("name", "", "string"));
        schema.addChild(new FieldDef("name", "", "string"));
        schema.addChild(new FieldDef("name", "", "string"));
        schema.addChild(new FieldDef("name", "", "string"));

        List<List> expected = Arrays.asList(
            Arrays.asList(1, 2, 3, 4),
            Arrays.asList("1", "2", "3", "4"),
            Arrays.asList("11", "12", "13", "14"),
            Arrays.asList("21", "22", "23", "24")
        );

        List<List> actual = PartnerResultToCartesianTable.expand(expected, schema);

        assertEquals(expected, actual);
    }

    @Test
    public void testExpandWhenOneNestedList() {
        FieldDefTree schema = new FieldDefTree();
        schema.addChild(new FieldDef("name", "", "string"));
        schema.addChild(null)
                .addChild(new FieldDef("name", "", "string"))
                .addChild(new FieldDef("name", "", "string"))
                .addChild(new FieldDef("name", "", "string"));
        schema.addChild(new FieldDef("name", "", "string"));
        schema.addChild(new FieldDef("name", "", "string"));

        List<List> list = List.of(
            Arrays.asList("1", Arrays.asList("21", "22", "23"), "3", "4")
        );

        List<List> expected = Arrays.asList(
            Arrays.asList("1", "21", "3", "4"),
            Arrays.asList("1", "22", "3", "4"),
            Arrays.asList("1", "23", "3", "4")
        );

        List<List> actual = PartnerResultToCartesianTable.expand(list, schema);

        assertEquals(expected, actual);
    }

    @Test
    public void testExpandWhenTwoNestedListAndOneRow() {
        FieldDefTree schema = new FieldDefTree();
        schema.addChild(new FieldDef("name", "", "string"));
        TreeNode<FieldDef> node2 = schema.addChild(null);
        node2.addChild(new FieldDef("name", "", "string"));
        node2.addChild(new FieldDef("name", "", "string"));
        schema.addChild(new FieldDef("name", "", "string"));
        TreeNode<FieldDef> node4 = schema.addChild(null);
        node4.addChild(new FieldDef("name", "", "string"));
        node4.addChild(new FieldDef("name", "", "string"));

        List<List> list = List.of(
            Arrays.asList(11,
                Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4)),
                12,
                Arrays.asList(Arrays.asList(5, 6), Arrays.asList(7, 8)))
        );

        List<List> expected = Arrays.asList(
            Arrays.asList(11, 1, 2, 12, 5, 6),
            Arrays.asList(11, 3, 4, 12, 5, 6),
            Arrays.asList(11, 1, 2, 12, 7, 8),
            Arrays.asList(11, 3, 4, 12, 7, 8)
        );

        List<List> actual = PartnerResultToCartesianTable.expand(list, schema);

        assertEquals(expected.size(), actual.size());
        for (List l : expected) {
            assertTrue(actual.contains(l));
        }
    }

    @Test
    public void testExpandWhenOneNestedListAndTwoRows() {
        FieldDefTree schema = new FieldDefTree();
        schema.addChild(new FieldDef("name", "", "string"));
        TreeNode<FieldDef> node = schema.addChild(null);
        node.addChild(new FieldDef("name", "", "string"));
        node.addChild(new FieldDef("name", "", "string"));
        schema.addChild(new FieldDef("name", "", "string"));
        schema.addChild(new FieldDef("name", "", "string"));

        List<List> list = Arrays.asList(
            Arrays.asList(11, Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4)), 12, 13),
            Arrays.asList(20,
                Arrays.asList(Arrays.asList(21, 22), Arrays.asList(23, 24), Arrays.asList(25, 26)),
                41,
                42)
        );

        List<List> expected = Arrays.asList(
            Arrays.asList(11, 1, 2, 12, 13),
            Arrays.asList(11, 3, 4, 12, 13),
            Arrays.asList(20, 21, 22, 41, 42),
            Arrays.asList(20, 23, 24, 41, 42),
            Arrays.asList(20, 25, 26, 41, 42)
        );

        List<List> actual = PartnerResultToCartesianTable.expand(list, schema);

        assertEquals(expected, actual);
    }

    @Test
    public void testExpandWhenOneNestedListIsEmpty() {
        FieldDefTree schema = new FieldDefTree();
        schema.addChild(new FieldDef("name", "", "string"));
        TreeNode<FieldDef> node = schema.addChild(null);
        node.addChild(new FieldDef("name", "", "string"));
        node.addChild(new FieldDef("name", "", "string"));
        schema.addChild(new FieldDef("name", "", "string"));
        schema.addChild(new FieldDef("name", "", "string"));

        List<List> list = List.of(
            Arrays.asList(11, new ArrayList<>(), 12, 13)
        );

        List<List> expected = List.of(
            Arrays.asList(11, null, null, 12, 13)
        );

        List<List> actual = PartnerResultToCartesianTable.expand(list, schema);

        assertEquals(expected, actual);
    }

    @Test
    public void testExpandWhenNestedListIsEmpty() {
        FieldDefTree schema = new FieldDefTree();
        schema.addChild(new FieldDef("name", "", "string"));
        TreeNode<FieldDef> node = schema.addChild(null);
        node.addChild(new FieldDef("name", "", "string"));
        node.addChild(new FieldDef("name", "", "string"));

        List<List> list = List.of(
            Arrays.asList(11, new Object())
        );

        List<List> expected = List.of(
            Arrays.asList(11, null, null)
        );

        List<List> actual = PartnerResultToCartesianTable.expand(list, schema);

        assertEquals(expected, actual);
    }
}
