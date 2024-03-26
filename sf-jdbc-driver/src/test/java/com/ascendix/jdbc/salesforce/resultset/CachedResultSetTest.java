package com.ascendix.jdbc.salesforce.resultset;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ascendix.jdbc.salesforce.metadata.ColumnMap;
import java.util.Calendar;
import java.util.Date;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CachedResultSetTest {

    private CachedResultSet cachedResultSet;

    @BeforeEach
    void setUp() {
        ColumnMap<String, Object> columnMap = new ColumnMap<>();
        cachedResultSet = new CachedResultSet(columnMap);
    }

    @Test
    void testParseDate() {
        Date actual = cachedResultSet.parseDate("2017-06-23");

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(actual);

        assertEquals(2017, calendar.get(Calendar.YEAR));
        assertEquals(Calendar.JUNE, calendar.get(Calendar.MONTH));
        assertEquals(23, calendar.get(Calendar.DAY_OF_MONTH));
    }

}
