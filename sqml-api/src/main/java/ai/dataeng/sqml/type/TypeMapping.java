package ai.dataeng.sqml.type;

import ai.dataeng.sqml.ingest.NamePath;
import ai.dataeng.sqml.ingest.SchemaAdjustment;
import ai.dataeng.sqml.ingest.SchemaAdjustmentSettings;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AbstractDataType;

public class TypeMapping {

    public static boolean inferForType(Type type) {
        return type instanceof StringType;
    }

    public static ScalarType inferType(ScalarType original, Object value) {
        Preconditions.checkArgument(value != null);
        if (original instanceof StringType) {
            String s = (String)value;
            if (testParse(v -> Long.parseLong(v), s)) return IntegerType.INSTANCE;
            else if (testParse(v -> Double.parseDouble(v), s)) return FloatType.INSTANCE;
            else if (testBoolean(s)) return BooleanType.INSTANCE;
            else if (testParse(v -> UUID.fromString(v), s)) return UuidType.INSTANCE;
            else if (testParse(v -> OffsetDateTime.parse(v), s)) return DateTimeType.INSTANCE;
            else if (testParse(v -> ZonedDateTime.parse(v), s)) return DateTimeType.INSTANCE;
            else if (testParse(v -> LocalDateTime.parse(v), s)) return DateTimeType.INSTANCE;
        }
        return original;
    }

    public static SchemaAdjustment<Object> adjustType(ScalarType datatype, Object value, NamePath path, SchemaAdjustmentSettings settings) {
        if (datatype instanceof StringType) {
            if (value instanceof String) return SchemaAdjustment.none();
            else if (settings.castDataType()) return SchemaAdjustment.data(Objects.toString(value));
            else return incompatibleType(datatype,value,path);
        } else if (datatype instanceof IntegerType) {
            if (value instanceof Long) return SchemaAdjustment.none();
            else if (value instanceof Integer || value instanceof Short || value instanceof Byte) return SchemaAdjustment.data(((Number)value).longValue());
            else if (value instanceof Number && settings.castDataType()) return SchemaAdjustment.data(((Number)value).longValue());
            else if (value instanceof String && settings.castDataType()) return parseFromString(value,s -> Long.parseLong(s),path,datatype);
            else return incompatibleType(datatype,value,path);
        } else if (datatype instanceof FloatType) {
            if (value instanceof Double) return SchemaAdjustment.none();
            else if (value instanceof Float) return SchemaAdjustment.data(((Number)value).doubleValue());
            else if (value instanceof Number && settings.castDataType()) return SchemaAdjustment.data(((Number)value).doubleValue());
            else if (value instanceof String && settings.castDataType()) return parseFromString(value,s -> Double.parseDouble(s),path,datatype);
            else return incompatibleType(datatype,value,path);
        } else if (datatype instanceof NumberType) {
            if (value instanceof Double) return SchemaAdjustment.none();
            if (value instanceof Number) return SchemaAdjustment.data(((Number)value).doubleValue());
            else if (value instanceof String && settings.castDataType()) return parseFromString(value,s -> Double.parseDouble(s),path,datatype);
            else return incompatibleType(datatype,value,path);
        } else if (datatype instanceof BooleanType) {
            if (value instanceof Boolean) return SchemaAdjustment.none();
            else if (value instanceof String && settings.castDataType()) return parseFromString(value,s -> parseBoolean,path,datatype);
            else return incompatibleType(datatype,value,path);
        } else if (datatype instanceof UuidType) {
            if (value instanceof UUID) return SchemaAdjustment.none();
            else return incompatibleType(datatype,value,path);
        } else if (datatype instanceof DateTimeType) { //TODO: need to make more robust!
            if (value instanceof OffsetDateTime) return SchemaAdjustment.none();
            else if (value instanceof String && settings.castDataType()) return parseFromString(value,s -> OffsetDateTime.parse(s),path,datatype);
            else return incompatibleType(datatype,value,path);
        }
        return SchemaAdjustment.none();
    }

    public static AbstractDataType getFlinkDataType(ScalarType datatype) {
        if (datatype instanceof StringType) {
            return DataTypes.STRING();
        } else if (datatype instanceof IntegerType) {
            return DataTypes.BIGINT();
        } else if (datatype instanceof FloatType) {
            return DataTypes.DECIMAL(6,9);
        } else if (datatype instanceof NumberType) {
            return DataTypes.DECIMAL(6, 9);
        } else if (datatype instanceof BooleanType) {
            return DataTypes.BOOLEAN();
        } else if (datatype instanceof UuidType) {
            return DataTypes.STRING();
        } else if (datatype instanceof DateTimeType) { //TODO: need to make more robust!
            return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
        } else {
            throw new IllegalArgumentException("Unrecognized data type: " + datatype);
        }
    }

    public static final ObjectArrayTypeInfo INSTANT_ARRAY_TYPE_INFO = ObjectArrayTypeInfo.getInfoFor(Instant[].class, BasicTypeInfo.INSTANT_TYPE_INFO);


    public static TypeInformation getFlinkTypeInfo(ScalarType datatype, boolean isArray) {
        if (datatype instanceof StringType) {
            if (isArray) return BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
            else return BasicTypeInfo.STRING_TYPE_INFO;
        } else if (datatype instanceof IntegerType) {
            if (isArray) return BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO;
            else return BasicTypeInfo.LONG_TYPE_INFO;
        } else if (datatype instanceof FloatType) {
            if (isArray) return BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
            else return BasicTypeInfo.DOUBLE_TYPE_INFO;
        } else if (datatype instanceof NumberType) {
            if (isArray) return BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
            else return BasicTypeInfo.DOUBLE_TYPE_INFO;
        } else if (datatype instanceof BooleanType) {
            if (isArray) return BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO;
            else return BasicTypeInfo.BOOLEAN_TYPE_INFO;
        } else if (datatype instanceof UuidType) {
            if (isArray) return BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
            else return BasicTypeInfo.STRING_TYPE_INFO;
        } else if (datatype instanceof DateTimeType) { //TODO: need to make more robust!
            if (isArray) return INSTANT_ARRAY_TYPE_INFO;
            else return BasicTypeInfo.INSTANT_TYPE_INFO;
        } else {
            throw new IllegalArgumentException("Unrecognized data type: " + datatype);
        }
    }

    private static SchemaAdjustment<Object> incompatibleType(ScalarType datatype, Object value, NamePath path) {
        return SchemaAdjustment.error(path, value, String.format("Incompatible data type. Expected data of type [%s]",datatype));
    }

    private static SchemaAdjustment parseFromString(Object value, Function<String,Object> parser, NamePath path, ScalarType datatype) {
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

    public static ScalarType scalar2Sqml(Object value) {
        if (value==null) return NullType.INSTANCE;
        else if (value instanceof UUID) return UuidType.INSTANCE;
        else if (value instanceof Number) {
            if (value instanceof Integer || value instanceof Long || value instanceof Byte || value instanceof Short || value instanceof BigInteger) {
                return IntegerType.INSTANCE;
            } else if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
                return FloatType.INSTANCE;
            } else return NumberType.INSTANCE;
        }
        else if (value instanceof String) return StringType.INSTANCE;
        else if (value instanceof Boolean) return BooleanType.INSTANCE;
        else if (value instanceof Temporal) {
            if (value instanceof ZonedDateTime || value instanceof OffsetDateTime) return DateTimeType.INSTANCE;
            else throw new IllegalArgumentException(String.format("Unrecognized DateTime format: %s [%s]", value, value.getClass()));
        }
        else throw new IllegalArgumentException(String.format("Unrecognized data type: %s [%s]", value, value.getClass()));
    }

}
