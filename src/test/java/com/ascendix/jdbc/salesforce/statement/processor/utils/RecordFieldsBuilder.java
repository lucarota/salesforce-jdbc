package com.ascendix.jdbc.salesforce.statement.processor.utils;

import java.util.HashMap;
import java.util.Map;

public class RecordFieldsBuilder {

    Map<String, Object> rec = new HashMap<>();

    public static RecordFieldsBuilder setId(String id) {
        return new RecordFieldsBuilder().set("Id", id);
    }

    public static Map<String, Object> id(String id) {
        return new RecordFieldsBuilder().set("Id", id).build();
    }

    public RecordFieldsBuilder set(String field, Object value) {
        rec.put(field, value);
        return this;
    }

    public Map<String, Object> build() {
        return rec;
    }

}
