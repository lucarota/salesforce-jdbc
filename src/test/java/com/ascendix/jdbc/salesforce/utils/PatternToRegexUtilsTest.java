package com.ascendix.jdbc.salesforce.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.ParseException;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PatternToRegexUtilsTest {

    static Stream<Arguments> data() {
        return Stream.of(
            Arguments.of("%", "(?i)^.*$", List.of("abc", "XXX", "yyy.AAA_BBB"), List.of()),
            Arguments.of("a%", "(?i)^\\Qa\\E.*$", List.of("a", "abc"), List.of("bca", "x")),
            Arguments.of("a_", "(?i)^\\Qa\\E.$", List.of("ab"), List.of("a", "bc", "abc")),
            Arguments.of("a.", "(?i)^\\Qa.\\E$", List.of("a."), List.of("ab")),
            Arguments.of("a[.]", "(?i)^\\Qa\\E[.]$", List.of("a."), List.of("ab")),
            Arguments.of("a[_]", "(?i)^\\Qa\\E[_]$", List.of("a_"), List.of("ab")),
            Arguments.of("%abc[%]%", "(?i)^.*\\Qabc\\E[%].*$", List.of("abc%", "XXXabc%", "XXXabc%Y"), List.of("abc", "XXXabc", "abcYYY"))
        );
    }

    @ParameterizedTest
    @MethodSource("data")
    public void toRegEx(String input, String expected, List<String> testOK, List<String> testKO) throws ParseException {
        final Pattern actual = PatternToRegexUtils.toRegEx(input);
        assertEquals(expected, actual.toString());

        for (String ok : testOK) {
            assertTrue(actual.matcher(ok).find(), ok);
        }

        for (String ko : testKO) {
            assertFalse(actual.matcher(ko).find(), ko);
        }
    }

}