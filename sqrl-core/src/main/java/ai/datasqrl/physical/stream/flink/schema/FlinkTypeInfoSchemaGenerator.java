package ai.datasqrl.physical.stream.flink.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.input.AbstractFlexibleTableConverterVisitor;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import ai.datasqrl.schema.type.Type;
import ai.datasqrl.schema.type.basic.*;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.List;
import java.util.Optional;

@Value
public class FlinkTypeInfoSchemaGenerator extends AbstractFlexibleTableConverterVisitor<TypeInformation> {

    public static final FlinkTypeInfoSchemaGenerator INSTANCE = new FlinkTypeInfoSchemaGenerator();

    @Override
    protected Optional<TypeInformation> createTable(TableBuilder<TypeInformation> tblBuilder) {
        List<Pair<Name,TypeInformation>> columns = tblBuilder.getColumns();
        return Optional.of(Types.ROW_NAMED(
                columns.stream().map(Pair::getKey).map(Name::getCanonical).toArray(i -> new String[i]),
                columns.stream().map(Pair::getValue).toArray(i -> new TypeInformation[i])));
    }

    @Override
    public TypeInformation nullable(TypeInformation type, boolean notnull) {
        return type; //Does not support nullability
    }

    @Override
    protected SqrlTypeConverter<TypeInformation> getTypeConverter() {
        return SqrlType2TypeInfoConverter.INSTANCE;
    }

    @Override
    public TypeInformation wrapArray(TypeInformation type, boolean notnull) {
        return Types.OBJECT_ARRAY(type);
    }

    public static TypeInformation convert(FlexibleTableConverter converter) {
        return converter.apply(INSTANCE).get();
    }

    public static class SqrlType2TypeInfoConverter implements SqrlTypeConverter<TypeInformation> {

        public static SqrlType2TypeInfoConverter INSTANCE = new SqrlType2TypeInfoConverter();

        @Override
        public TypeInformation visitType(Type type, Void context) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public <J> TypeInformation visitBasicType(AbstractBasicType<J> type, Void context) {
            throw new UnsupportedOperationException("Basic type is not supported in Table API: " + type);
        }

        @Override
        public TypeInformation visitBooleanType(BooleanType type, Void context) {
            return BasicTypeInfo.BOOLEAN_TYPE_INFO;
        }

        @Override
        public TypeInformation visitDateTimeType(DateTimeType type, Void context) {
            return BasicTypeInfo.INSTANT_TYPE_INFO;
        }

        @Override
        public TypeInformation visitFloatType(FloatType type, Void context) {
            return BasicTypeInfo.DOUBLE_TYPE_INFO;
        }

        @Override
        public TypeInformation visitIntegerType(IntegerType type, Void context) {
            return BasicTypeInfo.LONG_TYPE_INFO;
        }

        @Override
        public TypeInformation visitStringType(StringType type, Void context) {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }

        @Override
        public TypeInformation visitUuidType(UuidType type, Void context) {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }

        @Override
        public TypeInformation visitIntervalType(IntervalType type, Void context) {
            return BasicTypeInfo.LONG_TYPE_INFO;
        }
    }

}
