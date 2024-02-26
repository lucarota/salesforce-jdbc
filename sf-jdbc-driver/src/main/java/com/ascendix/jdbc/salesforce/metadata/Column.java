package com.ascendix.jdbc.salesforce.metadata;

import java.io.Serializable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter
@RequiredArgsConstructor
public class Column implements Serializable {

    private final String name;
    private final String type;

    @Setter
    private Table table;
    @Setter
    private String referencedTable;
    @Setter
    private String referencedColumn;
    @Setter
    private String comments;
    @Setter
    private Integer length;
    @Setter
    private boolean nillable;
    @Setter
    private boolean calculated;
}
