package com.ascendix.jdbc.salesforce.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import lombok.Getter;

public class ColumnMap<K, V> implements Serializable {

    private static final long serialVersionUID = 2705233366870541749L;

    @Getter
    private final ArrayList<K> columnNames = new ArrayList<>();
    @Getter
    private final ArrayList<V> values = new ArrayList<>();
    private int columnPosition = 0;

    public V put(K key, V value) {
        columnNames.add(columnPosition, key);
        values.add(columnPosition, value);
        columnPosition++;
        return value;
    }

    public ColumnMap<K, V> add(K key, V value) {
        put(key, value);
        return this;
    }

    public V get(K key) {
        int index = columnNames.indexOf(key);
        return index != -1 ? values.get(index) : null;
    }

    /**
     * Get a column name by index, starting at 1, that represents the insertion
     * order into the map.
     */
    public V getByIndex(int index) {
        return values.get(index - 1);
    }

    public int size() {
        return columnNames.size();
    }
}
