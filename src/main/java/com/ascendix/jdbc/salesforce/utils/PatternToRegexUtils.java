package com.ascendix.jdbc.salesforce.utils;

import java.util.regex.Pattern;

public class PatternToRegexUtils {

    private PatternToRegexUtils() {
        // Utility class
    }

    public static Pattern toRegEx(String pattern) {
        final PatternParser parser = new PatternParser(pattern);

        return Pattern.compile("(?i)^" + parser.parse() + "$");
    }

    static class PatternParser {

        private int index = 0;
        private final String pattern;

        public PatternParser(String pattern) {
            this.pattern = pattern;
        }

        public String parse() {
            StringBuilder regex = new StringBuilder();
            while (hasMoreChars()) {
                char currentChar = peekNextChar();
                if (currentChar == '%') {
                    regex.append(parseWildcard());
                } else if (currentChar == '_') {
                    regex.append(parseWildchar());
                } else if (currentChar == '[') {
                    regex.append(parseEscape());
                } else {
                    regex.append(parseString());
                }
            }
            return regex.toString();
        }

        private String parseWildcard() {
            consumeNextChar('%');
            return ".*";
        }

        private String parseWildchar() {
            consumeNextChar('_');
            return ".";
        }

        private String parseString() {
            StringBuilder sb = new StringBuilder();
            char currentChar = peekNextChar();
            while (currentChar != '%' && currentChar != '_' && currentChar != '[') {
                sb.append(getNextChar());
                if (!hasMoreChars()) {
                    break;
                }
                currentChar = peekNextChar();
            }
            return Pattern.quote(sb.toString());
        }

        private String parseEscape() {
            consumeNextChar('[');
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            char currentChar = getNextChar();
            while (currentChar != ']') {
                sb.append(currentChar);
                currentChar = getNextChar();
            }
            sb.append("]");
            return sb.toString();
        }

        private char getNextChar() {
            return pattern.charAt(index++);
        }

        private char peekNextChar() {
            return pattern.charAt(index);
        }

        private void consumeNextChar(char expected) {
            char currentChar = getNextChar();
            if (currentChar != expected) {
                throw new IllegalArgumentException(buildErrorMessage(
                    "Expected '" + expected + "' but found '" + currentChar + "'"));
            }
        }

        private boolean hasMoreChars() {
            return pattern.length() > index;
        }

        private String buildErrorMessage(String msg) {
            int start = index - 10;
            int end = index + 10;
            if (start < 0) {
                start = 0;
            }
            if (end > pattern.length()) {
                end = pattern.length();
            }
            return msg + ": " + pattern.substring(start, index) + "|~|" + pattern.substring(index, end);
        }
    }
}
