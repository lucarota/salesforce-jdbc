package com.ascendix.jdbc.salesforce.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Types;
import org.junit.jupiter.api.Test;

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
