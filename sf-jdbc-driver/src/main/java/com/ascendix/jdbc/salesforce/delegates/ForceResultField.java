package com.ascendix.jdbc.salesforce.delegates;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class ForceResultField {

    public static final String NESTED_RESULT_SET_FIELD_TYPE = "nestedResultSet";

    private final String entityType;
    private final String name;
    private final String fieldType;
    @Setter
    private Object value;

    public String getFullName() {
        return entityType != null ? entityType + "." + name : name;
    }
}
