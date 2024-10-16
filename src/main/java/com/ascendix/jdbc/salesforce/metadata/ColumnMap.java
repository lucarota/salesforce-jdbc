package com.ascendix.jdbc.salesforce.metadata;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import lombok.Getter;

public class ColumnMap<K, V> implements Serializable {

    @Serial
    private static final long serialVersionUID = 2705233366870541749L;

    @Getter
    private final ArrayList<K> columnNames = new ArrayList<>();
    private final ArrayList<K> columnLabels = new ArrayList<>();
    @Getter
    private final ArrayList<V> values = new ArrayList<>();
    @Getter
    private final ArrayList<TypeInfo> types = new ArrayList<>();

    public V put(K key, K label, V value, TypeInfo typeInfo) {
        columnNames.add(key);
        columnLabels.add(label);
        values.add(value);
        types.add(typeInfo);
        return value;
    }

    public V put(K key, V value, TypeInfo typeInfo) {
        put(key, key, value, typeInfo);
        return value;
    }

    public ColumnMap<K, V> add(K key, V value, TypeInfo typeInfo) {
        put(key, value, typeInfo);
        return this;
    }

    public V get(K key) {
        int index = columnNames.indexOf(key);
        if (index == -1) {
            index = columnLabels.indexOf(key);
        }
        return index != -1 ? values.get(index) : null;
    }

    public TypeInfo getTypeInfo(K key) {
        int index = columnNames.indexOf(key);
        if (index == -1) {
            index = columnLabels.indexOf(key);
        }
        return index != -1 ? types.get(index) : null;
    }

    /**
     * Get a column name by index, starting at 1, that represents the insertion
     * order into the map.
     */
    public V getByIndex(int index) {
        return values.get(index - 1);
    }

    public TypeInfo getTypeInfoByIndex(int index) {
        return types.get(index - 1);
    }

    public int size() {
        return columnNames.size();
    }
}
