package com.ascendix.jdbc.salesforce.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IteratorUtils {

    private IteratorUtils() {
        // Utility class
    }

    public static <E> List<E> toList(Iterator<? extends E> iterator) {
        if (iterator == null) {
            throw new NullPointerException("Iterator must not be null");
        } else {
            List<E> list = new ArrayList<>();

            while(iterator.hasNext()) {
                list.add(iterator.next());
            }

            return list;
        }
    }
}
