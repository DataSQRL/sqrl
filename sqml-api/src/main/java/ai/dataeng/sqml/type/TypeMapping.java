package ai.dataeng.sqml.type;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

public class TypeMapping {

    public static boolean inferForType(SqmlType type) {
        return type instanceof SqmlType.StringSqmlType;
    }

    public static SqmlType.ScalarSqmlType inferType(SqmlType.ScalarSqmlType original, Object value) {
        Preconditions.checkArgument(value != null);
        if (original instanceof SqmlType.StringSqmlType) {
            String s = (String)value;
            if (testParse(v -> Long.parseLong(v), s)) return SqmlType.IntegerSqmlType.INSTANCE;
            else if (testParse(v -> Double.parseDouble(v), s)) return SqmlType.FloatSqmlType.INSTANCE;
            else if (parseBoolean(s)) return SqmlType.BooleanSqmlType.INSTANCE;
            else if (testParse(v -> UUID.fromString(v), s)) return SqmlType.UuidSqmlType.INSTANCE;
            else if (testParse(v -> OffsetDateTime.parse(v), s)) return SqmlType.DateTimeSqmlType.INSTANCE;
            else if (testParse(v -> ZonedDateTime.parse(v), s)) return SqmlType.DateTimeSqmlType.INSTANCE;
            else if (testParse(v -> LocalDateTime.parse(v), s)) return SqmlType.DateTimeSqmlType.INSTANCE;
        }
        return original;
    }

    private static Consumer<String> PARSE_BOOLEAN = new Consumer<String>() {
        @Override
        public void accept(String s) {

        }
    };

    private static boolean parseBoolean(String s) {
        if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")) {
            return true;
        } else return false;
    }

    private static boolean testParse(Consumer<String> parser, String value) {
        try {
            parser.accept(value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    public static SqmlType.ScalarSqmlType scalar2Sqml(Object value) {
        if (value==null) return SqmlType.NullSqmlType.INSTANCE;
        else if (value instanceof UUID) return SqmlType.UuidSqmlType.INSTANCE;
        else if (value instanceof Number) {
            if (value instanceof Integer || value instanceof Long || value instanceof Byte || value instanceof Short || value instanceof BigInteger) {
                return SqmlType.IntegerSqmlType.INSTANCE;
            } else if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
                return SqmlType.FloatSqmlType.INSTANCE;
            } else return SqmlType.NumberSqmlType.INSTANCE;
        }
        else if (value instanceof String) return SqmlType.StringSqmlType.INSTANCE;
        else if (value instanceof Boolean) return SqmlType.BooleanSqmlType.INSTANCE;
        else if (value instanceof Temporal) {
            if (value instanceof ZonedDateTime || value instanceof OffsetDateTime) return SqmlType.DateTimeSqmlType.INSTANCE;
            else throw new IllegalArgumentException(String.format("Unrecognized DateTime format: %s [%s]", value, value.getClass()));
        }
        else throw new IllegalArgumentException(String.format("Unrecognized data type: %s [%s]", value, value.getClass()));
    }

}
