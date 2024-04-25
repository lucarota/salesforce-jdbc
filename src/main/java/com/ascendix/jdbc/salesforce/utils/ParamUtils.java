package com.ascendix.jdbc.salesforce.utils;

import java.time.format.DateTimeFormatter;

public class ParamUtils {
    public static final DateTimeFormatter SQL_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter SQL_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
}
