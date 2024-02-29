package com.ascendix.jdbc.salesforce.metadata;

import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class TypeInfoTest {

    @Test
    public void testLookupTypeInfo() {
        TypeInfo actual = TypeInfo.lookupTypeInfo("int");

        assertEquals("int", actual.getTypeName());
        assertEquals(Types.INTEGER, actual.getSqlDataType());
    }

    @Test
    public void testLookupTypeInfo_IfTypeUnknown() {
        TypeInfo actual = TypeInfo.lookupTypeInfo("my strange type");

        assertEquals("other", actual.getTypeName());
        assertEquals(Types.OTHER, actual.getSqlDataType());
    }

}
