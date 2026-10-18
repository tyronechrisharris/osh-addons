package org.sensorhub.impl.datastore.postgis.builder.filter;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.geotools.api.filter.And;
import org.geotools.api.filter.BinaryComparisonOperator;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.Not;
import org.geotools.api.filter.Or;
import org.geotools.api.filter.PropertyIsEqualTo;
import org.geotools.api.filter.PropertyIsGreaterThan;
import org.geotools.api.filter.PropertyIsGreaterThanOrEqualTo;
import org.geotools.api.filter.PropertyIsLessThan;
import org.geotools.api.filter.PropertyIsLessThanOrEqualTo;
import org.geotools.api.filter.PropertyIsLike;
import org.geotools.api.filter.PropertyIsNotEqualTo;
import org.geotools.api.filter.expression.Literal;
import org.geotools.api.filter.expression.PropertyName;

import java.math.BigDecimal;
import java.util.regex.Pattern;

/** Evaluates the supported OSCAR CQL subset directly against archived JSON. */
public final class JsonCqlEvaluator {
    private JsonCqlEvaluator() {
    }

    public static boolean matches(Filter filter, String json) {
        if (filter == null || filter == Filter.INCLUDE)
            return true;
        if (filter == Filter.EXCLUDE)
            return false;
        JsonElement value = JsonParser.parseString(json);
        if (!value.isJsonObject())
            return false;
        return evaluate(filter, value.getAsJsonObject());
    }

    private static boolean evaluate(Filter filter, JsonObject json) {
        if (filter == Filter.INCLUDE)
            return true;
        if (filter == Filter.EXCLUDE)
            return false;
        if (filter instanceof And and)
            return and.getChildren().stream().allMatch(child -> evaluate(child, json));
        if (filter instanceof Or or)
            return or.getChildren().stream().anyMatch(child -> evaluate(child, json));
        if (filter instanceof Not not)
            return !evaluate(not.getFilter(), json);
        if (filter instanceof PropertyIsLike like)
            return like(like, json);
        if (filter instanceof BinaryComparisonOperator comparison)
            return compare(comparison, json);
        throw new IllegalArgumentException("Unsupported archived CQL predicate: " + filter.getClass().getName());
    }

    private static boolean compare(BinaryComparisonOperator filter, JsonObject json) {
        if (!(filter.getExpression1() instanceof PropertyName property) ||
                !(filter.getExpression2() instanceof Literal literal))
            throw new IllegalArgumentException("Archived CQL comparisons require property-to-literal expressions");
        JsonElement actual = resolve(json, property.getPropertyName());
        Object expected = literal.getValue();
        boolean absent = actual == null || actual instanceof JsonNull || actual.isJsonNull();

        if (filter instanceof PropertyIsEqualTo)
            return expected == null ? absent : !absent && equalsScalar(actual, expected, filter.isMatchingCase());
        if (filter instanceof PropertyIsNotEqualTo)
            return expected == null ? !absent : !absent && sameScalarType(actual, expected) &&
                    !equalsScalar(actual, expected, filter.isMatchingCase());
        if (absent || !actual.isJsonPrimitive() || !actual.getAsJsonPrimitive().isNumber())
            return false;
        BigDecimal left = actual.getAsBigDecimal();
        BigDecimal right;
        try {
            right = expected instanceof Number ? new BigDecimal(expected.toString()) : new BigDecimal(String.valueOf(expected));
        } catch (NumberFormatException error) {
            return false;
        }
        int order = left.compareTo(right);
        if (filter instanceof PropertyIsGreaterThan)
            return order > 0;
        if (filter instanceof PropertyIsGreaterThanOrEqualTo)
            return order >= 0;
        if (filter instanceof PropertyIsLessThan)
            return order < 0;
        if (filter instanceof PropertyIsLessThanOrEqualTo)
            return order <= 0;
        throw new IllegalArgumentException("Unsupported archived CQL comparison: " + filter.getClass().getName());
    }

    private static boolean like(PropertyIsLike filter, JsonObject json) {
        if (!(filter.getExpression() instanceof PropertyName property))
            throw new IllegalArgumentException("Archived CQL LIKE requires a property expression");
        JsonElement actual = resolve(json, property.getPropertyName());
        if (actual == null || actual.isJsonNull() || !actual.isJsonPrimitive())
            return false;
        int flags = filter.isMatchingCase() ? 0 : Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
        return Pattern.compile(likeRegex(filter), flags).matcher(actual.getAsString()).matches();
    }

    private static String likeRegex(PropertyIsLike filter) {
        String wildcard = filter.getWildCard();
        String single = filter.getSingleChar();
        String escape = filter.getEscape();
        StringBuilder regex = new StringBuilder("^");
        String literal = filter.getLiteral();
        for (int index = 0; index < literal.length();) {
            if (!escape.isEmpty() && literal.startsWith(escape, index)) {
                index += escape.length();
                if (index < literal.length()) {
                    int point = literal.codePointAt(index);
                    regex.append(Pattern.quote(new String(Character.toChars(point))));
                    index += Character.charCount(point);
                }
            } else if (!wildcard.isEmpty() && literal.startsWith(wildcard, index)) {
                regex.append(".*");
                index += wildcard.length();
            } else if (!single.isEmpty() && literal.startsWith(single, index)) {
                regex.append('.');
                index += single.length();
            } else {
                int point = literal.codePointAt(index);
                regex.append(Pattern.quote(new String(Character.toChars(point))));
                index += Character.charCount(point);
            }
        }
        return regex.append('$').toString();
    }

    private static JsonElement resolve(JsonObject root, String path) {
        JsonElement current = root;
        for (String part : path.split("\\.")) {
            if (current == null || !current.isJsonObject())
                return null;
            current = current.getAsJsonObject().get(part);
        }
        return current;
    }

    private static boolean sameScalarType(JsonElement actual, Object expected) {
        if (!actual.isJsonPrimitive())
            return false;
        if (expected instanceof Boolean)
            return actual.getAsJsonPrimitive().isBoolean();
        if (expected instanceof Number)
            return actual.getAsJsonPrimitive().isNumber();
        if (expected instanceof String)
            return actual.getAsJsonPrimitive().isString();
        return true;
    }

    private static boolean equalsScalar(JsonElement actual, Object expected, boolean matchingCase) {
        if (!sameScalarType(actual, expected))
            return false;
        if (expected instanceof Number)
            return actual.getAsBigDecimal().compareTo(new BigDecimal(expected.toString())) == 0;
        if (expected instanceof Boolean)
            return actual.getAsBoolean() == (Boolean) expected;
        String left = actual.getAsString();
        String right = String.valueOf(expected);
        return matchingCase ? left.equals(right) : left.equalsIgnoreCase(right);
    }
}
