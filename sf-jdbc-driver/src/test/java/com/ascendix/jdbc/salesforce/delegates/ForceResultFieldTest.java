package com.ascendix.jdbc.salesforce.delegates;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ForceResultFieldTest {

    @Test
    public void testFieldOrder() {
        ForceResultField resultField = new ForceResultField("entity", "field", "name", "value");
        assertEquals("entity", resultField.getEntityType());
        assertEquals("field", resultField.getFieldType());
        assertEquals("name", resultField.getName());
        assertEquals("value", resultField.getValue());
        assertEquals("entity.name", resultField.getFullName());

        resultField = new ForceResultField(null, "field", "name", "value");
        assertEquals("name", resultField.getFullName());
    }
}
