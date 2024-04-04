package com.ascendix.jdbc.salesforce.delegates;

import com.ascendix.jdbc.salesforce.utils.FieldDefTree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings({"rawtypes", "unchecked"})
public class PartnerResultToCartesianTable {

    private final FieldDefTree schema;

    private PartnerResultToCartesianTable(FieldDefTree schema) {
        this.schema = schema;
    }

    public static List<List> expand(List<List> list, FieldDefTree schema) {
        PartnerResultToCartesianTable expander = new PartnerResultToCartesianTable(schema);
        return expander.expandOn(list, 0, 0);
    }

    private List<List> expandOn(List<List> rows, int columnPosition, int schemaPosition) {
        return rows.stream()
            .map(row -> expandRow(row, columnPosition, schemaPosition))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    private List<List> expandRow(List row, int columnPosition, int schemaPosition) {
        List<List> result = new ArrayList<>();
        if (schemaPosition > schema.getChildrenCount() - 1) {
            result.add(row);
            return result;
        } else if (!schema.getChild(schemaPosition).isLeaf()) {
            int nestedListSize = schema.getChild(schemaPosition).getChildrenCount();
            Object value = row.get(columnPosition);
            List nestedList = value instanceof List ? (List) value : Collections.emptyList();
            if (nestedList.isEmpty()) {
                result.add(expandRow(row, Collections.nCopies(nestedListSize, null), columnPosition));
            } else {
                nestedList.forEach(item -> result.add(expandRow(row, item, columnPosition)));
            }
            return expandOn(result, columnPosition + nestedListSize, schemaPosition + 1);
        } else {
            result.add(row);
            return expandOn(result, columnPosition + 1, schemaPosition + 1);
        }
    }

    private static List expandRow(List row, Object nestedItem, int position) {
        List nestedItemsToInsert = nestedItem instanceof List ? (List) nestedItem : Collections.singletonList(nestedItem);
        List newRow = new ArrayList<>(row.subList(0, position));
        newRow.addAll(nestedItemsToInsert);
        newRow.addAll(row.subList(position + 1, row.size()));
        return newRow;
    }
}
