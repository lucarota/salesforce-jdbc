package com.ascendix.jdbc.salesforce.statement;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
public class FieldDef {

    /**
     * Name of the field (or Name of the field used in aggregation)
     */
    private final String name;
    /**
     * Full name of the field with sub entity or name of the aggregation function like:
     * 1) Owner.Name  for select Owner.Name from Account
     * 2) maxLastName for MAX(LastName)
     */
    private final String fullName;
    private final String alias;
    private final String type;

    public FieldDef(String name, String fullName, String alias, String type) {
        this.name = name;
        this.fullName = fullName;
        this.alias = alias;
        this.type = type;
    }

    public String getEntity() {
        int dot = fullName.lastIndexOf(".");
        if (dot == -1) {
            return "";
        }
        return fullName.substring(dot + 1);
    }
}
