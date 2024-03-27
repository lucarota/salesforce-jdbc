package com.ascendix.jdbc.salesforce.statement;

import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
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
    private final String alias;
    private final String type;

    public FieldDef(String name, String alias, String type) {
        this.name = name;
        this.alias = alias;
        this.type = type;
    }
}
