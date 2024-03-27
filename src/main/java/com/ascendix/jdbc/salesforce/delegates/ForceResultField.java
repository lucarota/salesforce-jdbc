package com.ascendix.jdbc.salesforce.delegates;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class ForceResultField {

    public static final String NESTED_RESULT_SET_FIELD_TYPE = "nestedResultSet";

    private String entityType;
    private String fieldType;
    private String name;
    @Setter
    private Object value;

    public String getFullName() {
        return entityType != null ? entityType + "." + name : name;
    }
}
