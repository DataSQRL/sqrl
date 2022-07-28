package ai.datasqrl.physical.stream.flink.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.input.SimpleFlexibleTableConverterVisitor;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import ai.datasqrl.schema.builder.NestedTableBuilder;
import ai.datasqrl.schema.builder.SimpleTableBuilder;
import ai.datasqrl.schema.input.SqrlTypeConverter;
import ai.datasqrl.schema.type.Type;
import ai.datasqrl.schema.type.basic.*;
import com.google.common.base.Preconditions;
import lombok.Value;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Optional;

/*
TODO: Need to add watermark based on inferred or defined timestamp
schemaBuilder.columnByExpression("__rowtime", "CAST(_ingest_time AS TIMESTAMP_LTZ(3))");
schemaBuilder.watermark(ReservedName.INGEST_TIME.getCanonical(),
ReservedName.INGEST_TIME.getCanonical() + " - INTERVAL '10' SECOND");
 */
@Value
public class FlinkTableSchemaGenerator extends SimpleFlexibleTableConverterVisitor<DataType> {

    private final Schema.Builder schemaBuilder = Schema.newBuilder();

    @Override
    public Optional<DataType> endTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton) {
        SimpleTableBuilder<DataType> tblBuilder = stack.removeFirst();
        if (isNested) {
            return createTable(tblBuilder);
        } else {
            for (SimpleTableBuilder.Column<DataType> column : tblBuilder.getColumns(true,true)) {
                schemaBuilder.column(column.getId().getCanonical(),column.getType());
            };
            return Optional.empty();
        }
    }

    @Override
    protected Optional<DataType> createTable(SimpleTableBuilder<DataType> tblBuilder) {
        List<NestedTableBuilder.Column<DataType>> columns = tblBuilder.getColumns(true,true);
        DataTypes.Field[] fields = new DataTypes.Field[columns.size()];
        for (int i = 0; i < fields.length; i++) {
            NestedTableBuilder.Column<DataType> column = columns.get(i);
            fields[i] = DataTypes.FIELD(column.getId().getCanonical(),column.getType());
        }
        return Optional.of(DataTypes.ROW(fields));
    }

    @Override
    public DataType nullable(DataType type, boolean nullable) {
        if (nullable) return type.nullable();
        else return type.notNull();
    }

    @Override
    protected SqrlTypeConverter<DataType> getTypeConverter() {
        return SqrlType2TableConverter.INSTANCE;
    }

    @Override
    public DataType wrapArray(DataType type) {
        return DataTypes.ARRAY(type);
    }

    public Schema getSchema() {
        Preconditions.checkState(stack.isEmpty());
        //TODO: add watermark
        return schemaBuilder.build();
    }

    public static Schema convert(FlexibleTableConverter converter) {
        FlinkTableSchemaGenerator tblGen = new FlinkTableSchemaGenerator();
        converter.apply(tblGen);
        return tblGen.getSchema();
    }

    public static class SqrlType2TableConverter implements SqrlTypeConverter<DataType> {

        public static SqrlType2TableConverter INSTANCE = new SqrlType2TableConverter();

        @Override
        public DataType visitType(Type type, Void context) {
            throw new UnsupportedOperationException("Should not be called");
        }

        @Override
        public <J> DataType visitBasicType(AbstractBasicType<J> type, Void context) {
            throw new UnsupportedOperationException("Basic type is not supported in Table API: " + type);
        }

        @Override
        public DataType visitBooleanType(BooleanType type, Void context) {
            return DataTypes.BOOLEAN();
        }

        @Override
        public DataType visitDateTimeType(DateTimeType type, Void context) {
            return DataTypes.TIMESTAMP_LTZ(3);
        }

        @Override
        public DataType visitFloatType(FloatType type, Void context) {
            return DataTypes.DOUBLE();
        }

        @Override
        public DataType visitIntegerType(IntegerType type, Void context) {
            return DataTypes.BIGINT();
        }

        @Override
        public DataType visitStringType(StringType type, Void context) {
            return DataTypes.STRING();
        }

        @Override
        public DataType visitUuidType(UuidType type, Void context) {
            return DataTypes.STRING();
        }

        @Override
        public DataType visitIntervalType(IntervalType type, Void context) {
            return DataTypes.INTERVAL(DataTypes.SECOND(3));
        }
    }

}
