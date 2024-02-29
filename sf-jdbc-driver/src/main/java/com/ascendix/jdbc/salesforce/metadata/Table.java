package com.ascendix.jdbc.salesforce.metadata;

import java.io.Serializable;
import java.util.List;
import lombok.Getter;

@Getter
public class Table implements Serializable {

    private final String name;
    private final String comments;
    private final List<Column> columns;
    private final boolean queryable;

    public Table(String name, String comments, List<Column> columns, boolean queryable) {
        this.name = name;
        this.comments = comments;
        this.columns = columns;
        this.queryable = queryable;
        for (Column c : columns) {
            c.setTable(this);
        }
    }

    public Column findColumn(String columnName) {
        return columns.stream()
            .filter(column -> columnName.equals(column.getName()))
            .findFirst()
            .orElse(null);
    }
}
