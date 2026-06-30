package it.rotaliano.jdbc.salesforce.utils;

import it.rotaliano.jdbc.salesforce.delegates.ForceResultField;
import it.rotaliano.jdbc.salesforce.statement.FieldDef;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FieldDefTreeTest {

    @Test
    public void testExpandSubqueryFields() {
        // Build schema
        // SELECT Id, (SELECT Title FROM CombinedAttachments) FROM Account
        FieldDefTree schema = new FieldDefTree();
        schema.addChild(new FieldDef("Id", "Id", "Id", "id"));
        
        FieldDefTree subquerySchema = new FieldDefTree();
        subquerySchema.setRelationshipName("CombinedAttachments");
        subquerySchema.addChild(new FieldDef("Title", "Title", "Title", "string"));
        schema.addTreeNode(subquerySchema);

        // Build result rows tree (similar to what removeServiceInfo produces)
        // Main row node
        TreeNode<ForceResultField> row = new TreeNode<>();
        row.addChild(new ForceResultField(null, "id", "Id", "0015E00001QpN0lQAF"));
        
        // Subquery node
        TreeNode<ForceResultField> subqueryNode = new TreeNode<>();
        
        // Subquery record 1
        TreeNode<ForceResultField> subqueryRecord = new TreeNode<>();
        subqueryRecord.addChild(new ForceResultField(null, "string", "CombinedAttachments.Title", "Attachment1.pdf"));
        subqueryNode.addTreeNode(subqueryRecord);
        
        row.addTreeNode(subqueryNode);

        List<TreeNode<ForceResultField>> rows = new ArrayList<>();
        rows.add(row);

        // Expand
        List<List<ForceResultField>> result = FieldDefTree.expand(rows, schema);

        // Assertions
        assertEquals(1, result.size());
        List<ForceResultField> flatRow = result.get(0);
        assertEquals(2, flatRow.size());
        
        assertEquals("Id", flatRow.get(0).getFullName());
        assertEquals("0015E00001QpN0lQAF", flatRow.get(0).getValue());
        
        assertEquals("CombinedAttachments.Title", flatRow.get(1).getFullName());
        assertEquals("Attachment1.pdf", flatRow.get(1).getValue());
    }
}
