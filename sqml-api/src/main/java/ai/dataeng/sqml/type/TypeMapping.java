package ai.dataeng.sqml.type;

import ai.dataeng.sqml.ingest.NamePath;
import ai.dataeng.sqml.ingest.SchemaAdjustment;
import ai.dataeng.sqml.ingest.SchemaAdjustmentSettings;
import com.google.common.base.Preconditions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

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
            else if (testBoolean(s)) return SqmlType.BooleanSqmlType.INSTANCE;
            else if (testParse(v -> UUID.fromString(v), s)) return SqmlType.UuidSqmlType.INSTANCE;
            else if (testParse(v -> OffsetDateTime.parse(v), s)) return SqmlType.DateTimeSqmlType.INSTANCE;
            else if (testParse(v -> ZonedDateTime.parse(v), s)) return SqmlType.DateTimeSqmlType.INSTANCE;
            else if (testParse(v -> LocalDateTime.parse(v), s)) return SqmlType.DateTimeSqmlType.INSTANCE;
        }
        return original;
    }

    public static SchemaAdjustment<Object> adjustType(SqmlType.ScalarSqmlType datatype, Object value, NamePath path, SchemaAdjustmentSettings settings) {
        if (datatype instanceof SqmlType.StringSqmlType) {
            if (value instanceof String) return SchemaAdjustment.none();
            else if (settings.castDataType()) return SchemaAdjustment.data(Objects.toString(value));
            else return incompatibleType(datatype,value,path);
        } else if (datatype instanceof SqmlType.IntegerSqmlType) {
            if (value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte) return SchemaAdjustment.none();
            else if (value instanceof Number && settings.castDataType()) return SchemaAdjustment.data(((Number)value).longValue());
            else if (value instanceof String && settings.castDataType()) return parseFromString(value,s -> Long.parseLong(s),path,datatype);
            else return incompatibleType(datatype,value,path);
        } else if (datatype instanceof SqmlType.FloatSqmlType) {
            if (value instanceof Float || value instanceof Double) return SchemaAdjustment.none();
            else if (value instanceof Number && settings.castDataType()) return SchemaAdjustment.data(((Number)value).doubleValue());
            else if (value instanceof String && settings.castDataType()) return parseFromString(value,s -> Double.parseDouble(s),path,datatype);
            else return incompatibleType(datatype,value,path);
        } else if (datatype instanceof SqmlType.NumberSqmlType) {
            if (value instanceof Number) return SchemaAdjustment.none();
            else if (value instanceof String && settings.castDataType()) return parseFromString(value,s -> Double.parseDouble(s),path,datatype);
            else return incompatibleType(datatype,value,path);
        } else if (datatype instanceof SqmlType.BooleanSqmlType) {
            if (value instanceof Boolean) return SchemaAdjustment.none();
            else if (value instanceof String && settings.castDataType()) return parseFromString(value,s -> parseBoolean,path,datatype);
            else return incompatibleType(datatype,value,path);
        } else if (datatype instanceof SqmlType.UuidSqmlType) {
            if (value instanceof UUID) return SchemaAdjustment.none();
            else return incompatibleType(datatype,value,path);
        } else if (datatype instanceof SqmlType.DateTimeSqmlType) { //TODO: need to make more robust!
            if (value instanceof OffsetDateTime) return SchemaAdjustment.none();
            else if (value instanceof String && settings.castDataType()) return parseFromString(value,s -> OffsetDateTime.parse(s),path,datatype);
            else return incompatibleType(datatype,value,path);
        }
        return SchemaAdjustment.none();
    }

    private static SchemaAdjustment<Object> incompatibleType(SqmlType.ScalarSqmlType datatype, Object value, NamePath path) {
        return SchemaAdjustment.error(path, value, String.format("Incompatible data type. Expected data of type [%s]",datatype));
    }

    private static SchemaAdjustment parseFromString(Object value, Function<String,Object> parser, NamePath path, SqmlType.ScalarSqmlType datatype) {
        Preconditions.checkArgument(value instanceof String);
        try {
            Object result = parser.apply((String)value);
            return SchemaAdjustment.data(result);
        } catch (IllegalArgumentException e) {
            return SchemaAdjustment.error(path, value, String.format("Could not parse value to data type [%s]",datatype));
        } catch (DateTimeParseException e) {
            return SchemaAdjustment.error(path, value, String.format("Could not parse value to data type [%s]",datatype));
        }
    }

    private static boolean testBoolean(String s) {
        if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")) {
            return true;
        } else return false;
    }

    private static Function<String,Boolean> parseBoolean = new Function<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
            if (s.equalsIgnoreCase("true")) return true;
            else if (s.equalsIgnoreCase("false")) return false;
            throw new IllegalArgumentException("Not a boolean");
        }
    };

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
