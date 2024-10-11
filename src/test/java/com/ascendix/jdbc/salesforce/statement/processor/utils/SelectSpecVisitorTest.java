package com.ascendix.jdbc.salesforce.statement.processor.utils;

import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.statement.FieldDef;
import com.ascendix.jdbc.salesforce.utils.FieldDefTree;
import com.sforce.soap.partner.ChildRelationship;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SelectSpecVisitorTest {

    private PartnerService partnerService;

    private FieldDefTree fieldDefTree;

    private SelectSpecVisitor selectSpecVisitor;

    @BeforeEach
    public void setup() {
        partnerService = mock(PartnerService.class);
        fieldDefTree = new FieldDefTree();
        selectSpecVisitor = new SelectSpecVisitor("Account", fieldDefTree, partnerService);

        // Mock DescribeSObjectResult Account
        DescribeSObjectResult describeResult = mock(DescribeSObjectResult.class);
        Field mockField = mock(Field.class);
        ChildRelationship mockChildRelationship = mock(ChildRelationship.class);
        when(mockField.getName()).thenReturn("Name");
        when(mockField.getType()).thenReturn(FieldType.string);
        when(mockChildRelationship.getRelationshipName()).thenReturn("Contact");
        when(mockChildRelationship.getChildSObject()).thenReturn("Contact");
        when(describeResult.getFields()).thenReturn(new Field[]{mockField});
        when(describeResult.getChildRelationships()).thenReturn(new ChildRelationship[]{mockChildRelationship});
        when(describeResult.getName()).thenReturn("Account");

        when(partnerService.describeSObject(eq("Account"))).thenReturn(describeResult);

        // Mock DescribeSObjectResult Contact
        describeResult = mock(DescribeSObjectResult.class);
        mockField = mock(Field.class);
        when(mockField.getName()).thenReturn("Name");
        when(mockField.getType()).thenReturn(FieldType.string);
        when(describeResult.getFields()).thenReturn(new Field[]{mockField});
        when(describeResult.getName()).thenReturn("Contact");

        when(partnerService.describeSObject(eq("Contact"))).thenReturn(describeResult);
    }

    @Test
    public void testVisitColumn()  {
        Column column = new Column();
        column.setColumnName("Name");
        column.setTable(new Table("Account"));

        SelectItem selectItem = new SelectItem<>(column);
        selectSpecVisitor.visit(selectItem, null);

        assertEquals(1, fieldDefTree.getChildrenCount());
        FieldDef fieldDef = fieldDefTree.getChild(0).getData();
        assertNotNull(fieldDef);
        assertEquals("Name", fieldDef.getName());
        assertEquals("Name", fieldDef.getFullName());
        assertEquals("Name", fieldDef.getAlias());
    }

    @Test
    public void testVisitColumnPrefix() {
        Column column = new Column();
        column.setColumnName("Name");
        column.setTable(new Table("Account"));

        SelectItem selectItem = new SelectItem<>(column);
        selectSpecVisitor.visit(selectItem, null);

        assertEquals(1, fieldDefTree.getChildrenCount());
        FieldDef fieldDef = fieldDefTree.getChild(0).getData();
        assertNotNull(fieldDef);
        assertEquals("Name", fieldDef.getName());
        assertEquals("Name", fieldDef.getFullName());
        assertEquals("Name", fieldDef.getAlias());
    }

    @Test
    void testVisitColumnWithoutAlias() {
        SelectItem<Expression> selectItem = new SelectItem(new Column("Name"));
        selectSpecVisitor.visit(selectItem, null);
        assertEquals(1, fieldDefTree.getChildrenCount());
        FieldDef fieldDef = fieldDefTree.getChildren().get(0).getData();
        assertEquals("Name", fieldDef.getName());
        assertEquals("Name", fieldDef.getAlias());
    }

    @Test
    void testVisitColumnWithAlias() {
        SelectItem selectItem = new SelectItem(new Column("Name"));
        selectItem.setAlias(new Alias("AliasTest"));
        selectSpecVisitor.visit(selectItem, null);
        assertEquals(1, fieldDefTree.getChildrenCount());
        FieldDef fieldDef = fieldDefTree.getChildren().get(0).getData();
        assertEquals("Name", fieldDef.getName());
        assertEquals("AliasTest", fieldDef.getAlias());
    }

    @Test
    void testVisitFunction() {
        net.sf.jsqlparser.expression.Function function = new net.sf.jsqlparser.expression.Function();
        function.setName("COUNT");
        SelectItem selectItem = new SelectItem(function);
        selectSpecVisitor.visit(selectItem, null);
        assertEquals(1, fieldDefTree.getChildrenCount());
        FieldDef fieldDef = fieldDefTree.getChildren().get(0).getData();
        assertEquals("COUNT", fieldDef.getName());
        assertEquals("int", fieldDef.getType());
    }

    @Test
    public void testVisit_Function() throws JSQLParserException {
        Function function = new Function();
        function.setName("COUNT");
        function.setParameters(new Column("Id"));

        SelectItem selectItem = new SelectItem<>(function);
        selectSpecVisitor.visit(selectItem, null);

        assertEquals(1, fieldDefTree.getChildrenCount());
        FieldDef fieldDef = fieldDefTree.getChild(0).getData();
        assertNotNull(fieldDef);
        assertEquals("COUNT", fieldDef.getName());
        assertEquals("COUNT", fieldDef.getFullName());
        assertEquals("COUNT", fieldDef.getAlias());
    }

    @Test
    public void testVisit_ParenthesedSelect() throws JSQLParserException {
        ParenthesedSelect parenthesedSelect = new ParenthesedSelect();
        PlainSelect plainSelect = new PlainSelect();
        plainSelect.addSelectItem(new Column("Name"));
        plainSelect.setFromItem(new Table("Contact"));
        parenthesedSelect.setSelect(plainSelect);

        SelectItem selectItem = new SelectItem<>(parenthesedSelect);
        selectSpecVisitor.visit(selectItem, null);

        assertEquals( 1, fieldDefTree.getChildrenCount());
        FieldDef fieldDef = fieldDefTree.getChild(0).getChild(0).getData();
        assertNotNull(fieldDef);
        assertEquals("Name", fieldDef.getName());
        assertEquals("Name", fieldDef.getFullName());
        assertEquals("Name", fieldDef.getAlias());
    }
}