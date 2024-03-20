package com.ascendix.salesforce.oauth;

import java.util.*;

public class JSONParser {

    private int index = 0;
    private final String json;

    public JSONParser(String json) {
        this.json = json;
    }

    public Object parse() {
        char currentChar = peekNextChar();
        if (currentChar == '{') {
            return parseObject();
        } else if (currentChar == '[') {
            return parseArray();
        } else if (Character.isDigit(currentChar) || currentChar == '-') {
            return parseNumber();
        } else if (currentChar == '"') {
            return parseString();
        } else if (currentChar == 't' || currentChar == 'f') {
            return parseBoolean();
        } else if (currentChar == 'n') {
            return parseNull();
        } else {
            throw new IllegalArgumentException(buildErrorMessage("Invalid JSON format"));
        }
    }

    private Map<String, Object> parseObject() {
        Map<String, Object> map = new LinkedHashMap<>();
        consumeNextChar('{');
        skipWhitespace();
        while (peekNextChar() != '}') {
            String key = parseString();
            skipWhitespace();
            consumeNextChar(':');
            skipWhitespace();
            Object value = parse();
            map.put(key, value);
            skipWhitespace();
            if (peekNextChar() == ',') {
                consumeNextChar(',');
                skipWhitespace();
            }
        }
        consumeNextChar('}');
        return map;
    }

    private List<Object> parseArray() {
        List<Object> list = new ArrayList<>();
        consumeNextChar('[');
        skipWhitespace();
        while (peekNextChar() != ']') {
            Object value = parse();
            list.add(value);
            skipWhitespace();
            if (peekNextChar() == ',') {
                consumeNextChar(',');
                skipWhitespace();
            }
        }
        consumeNextChar(']');
        return list;
    }

    private Number parseNumber() {
        StringBuilder sb = new StringBuilder();
        char currentChar = peekNextChar();
        while (Character.isDigit(currentChar) || currentChar == '.' || currentChar == '-') {
            sb.append(currentChar);
            index++;
            if (index < json.length()) {
                currentChar = json.charAt(index);
            } else {
                break;
            }
        }
        String value = sb.toString();
        if (value.contains(".")) {
            return Double.parseDouble(value);
        } else {
            return Integer.parseInt(value);
        }
    }

    private String parseString() {
        consumeNextChar('"');
        StringBuilder sb = new StringBuilder();
        char currentChar = getNextChar();
        while (currentChar != '"') {
            sb.append(currentChar);
            currentChar = getNextChar();
        }
        return sb.toString();
    }

    private Boolean parseBoolean() {
        String value = consumeNextChars(4);
        if (value.equals("true")) {
            return true;
        } else if (value.equals("fals") && getNextChar() == 'e') {
            return false;
        } else {
            throw new IllegalArgumentException(buildErrorMessage("Invalid boolean value"));
        }
    }

    private Object parseNull() {
        String value = consumeNextChars(4);
        if (value.equals("null")) {
            return null;
        } else {
            throw new IllegalArgumentException(buildErrorMessage("Invalid value"));
        }
    }

    private char getNextChar() {
        return json.charAt(index++);
    }

    private char peekNextChar() {
        return json.charAt(index);
    }

    private void consumeNextChar(char expected) {
        char currentChar = getNextChar();
        if (currentChar != expected) {
            throw new IllegalArgumentException(buildErrorMessage("Expected '" + expected + "' but found '" + currentChar + "'"));
        }
    }

    private String consumeNextChars(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(getNextChar());
        }
        return sb.toString();
    }

    private void skipWhitespace() {
        while (index < json.length() && Character.isWhitespace(json.charAt(index))) {
            index++;
        }
    }

    private String buildErrorMessage(String msg) {
        int start = index - 10;
        int end = index + 10;
        if (start < 0) {
            start = 0;
        }
        if (end > json.length()) {
            end = json.length();
        }
        return msg + ": " + json.substring(start, index) + "|~|" + json.substring(index, end);
    }
}
