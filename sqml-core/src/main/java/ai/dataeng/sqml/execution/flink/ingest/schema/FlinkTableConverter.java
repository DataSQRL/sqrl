package ai.dataeng.sqml.execution.flink.ingest.schema;

import ai.dataeng.sqml.io.sources.SourceRecord;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.tree.name.ReservedName;
import ai.dataeng.sqml.type.ArrayType;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.basic.*;
import ai.dataeng.sqml.type.constraint.ConstraintHelper;
import ai.dataeng.sqml.type.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.type.schema.FlexibleSchemaHelper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class FlinkTableConverter {

    public Schema tableSchemaConversion(FlexibleDatasetSchema.TableField table) {
        NamePath path = NamePath.of(table.getName());
        Schema.Builder schemaBuilder = Schema.newBuilder();
        getFields(table.getFields()).map(p -> fieldTypeSchemaConversion(p.getKey(),p.getRight(),path)).forEach(dtf -> {
            schemaBuilder.column(dtf.getName(),dtf.getDataType());
        });
        schemaBuilder.column(ReservedName.UUID.getCanonical(), toFlinkDataType(UuidType.INSTANCE));
        schemaBuilder.column(ReservedName.INGEST_TIME.getCanonical(), toFlinkDataType(DateTimeType.INSTANCE));
        schemaBuilder.column(ReservedName.SOURCE_TIME.getCanonical(), toFlinkDataType(DateTimeType.INSTANCE));
        //TODO: adjust based on configuration
        schemaBuilder.columnByExpression("__rowtime", "CAST(_ingest_time AS TIMESTAMP_LTZ(3))");
        schemaBuilder.watermark("__rowtime", "__rowtime - INTERVAL '10' SECOND");
        return schemaBuilder.build();
    }

    private static Stream<Pair<Name, FlexibleDatasetSchema.FieldType>> getFields(RelationType<FlexibleDatasetSchema.FlexibleField> relation) {
        return relation.getFields().stream().flatMap(field -> field.getTypes().stream().map( ftype -> {
            Name name = FlexibleSchemaHelper.getCombinedName(field,ftype);
            return Pair.of(name,ftype);
        }));
    }

    private DataTypes.Field fieldTypeSchemaConversion(Name name, FlexibleDatasetSchema.FieldType ftype,
                                                      NamePath path) {
//        boolean notnull = !isMixedType && ConstraintHelper.isNonNull(ftype.getConstraints());

        DataType dt;
        if (ftype.getType() instanceof RelationType) {
            List<DataTypes.Field> dtfs = new ArrayList<>();
            final NamePath nestedpath = path.resolve(name);
            getFields((RelationType<FlexibleDatasetSchema.FlexibleField>) ftype.getType())
                    .map(p -> fieldTypeSchemaConversion(p.getKey(),p.getRight(),nestedpath))
                    .forEach(dtf -> {
                        dtfs.add(dtf);
                    });
            if (!isSingleton(ftype)) {
                dtfs.add(DataTypes.FIELD(ReservedName.ARRAY_IDX.getCanonical(),toFlinkDataType(IntegerType.INSTANCE)));
                dt = DataTypes.ROW(dtfs.toArray(new DataTypes.Field[dtfs.size()]));
                dt = DataTypes.ARRAY(dt);
            } else {
                dt = DataTypes.ROW(dtfs.toArray(new DataTypes.Field[dtfs.size()]));
            }
        } else if (ftype.getType() instanceof ArrayType) {
            dt = wrapArray((ArrayType) ftype.getType());
        } else {
            assert ftype.getType() instanceof BasicType;
            dt = toFlinkDataType((BasicType)ftype.getType());
        }
        return DataTypes.FIELD(name.getCanonical(),dt);
    }

    private DataType wrapArray(ArrayType arrType) {
        Type subType = arrType.getSubType();
        DataType dt;
        if (subType instanceof ArrayType) {
            dt = wrapArray((ArrayType) subType);
        } else {
            assert subType instanceof BasicType;
            dt = toFlinkDataType((BasicType)subType);
        }
        return DataTypes.ARRAY(dt);
    }

    private static boolean isSingleton(FlexibleDatasetSchema.FieldType ftype) {
        return ConstraintHelper.getCardinality(ftype.getConstraints()).isSingleton();
    }

    public DataType toFlinkDataType(BasicType type) {
        if (type instanceof StringType) {
            return DataTypes.STRING();
        } else if (type instanceof DateTimeType) {
            return DataTypes.DATE();
        } else if (type instanceof BooleanType) {
            return DataTypes.BOOLEAN();
        } else if (type instanceof BigIntegerType) {
            return DataTypes.BIGINT();
        } else if (type instanceof DoubleType) {
            return DataTypes.DOUBLE();
        } else if (type instanceof FloatType) {
            return DataTypes.FLOAT();
        } else if (type instanceof IntegerType) {
            return DataTypes.BIGINT();
        } else if (type instanceof NumberType) {
            return DataTypes.DOUBLE();
        } else if (type instanceof IntervalType) {
            return DataTypes.BIGINT();
        } else if (type instanceof UuidType) {
            return DataTypes.STRING();
        } else {
            throw new UnsupportedOperationException("Unexpected data type: " + type);
        }
    }

    public static class SourceRecord2RowMapper implements MapFunction<SourceRecord<Name>, Row> {

        private final FlexibleDatasetSchema.TableField tableSchema;

        public SourceRecord2RowMapper(FlexibleDatasetSchema.TableField tableSchema) {
            this.tableSchema = tableSchema;
        }

        @Override
        public Row map(SourceRecord<Name> sourceRecord) throws Exception {
            Object[] cols = constructRows(sourceRecord.getData(), tableSchema.getFields());
            int offset = cols.length;
            cols = Arrays.copyOf(cols,cols.length+3);
            cols[offset++]=sourceRecord.getUuid().toString();
            cols[offset++]=sourceRecord.getIngestTime();
            cols[offset++]=sourceRecord.getSourceTime();
            return Row.ofKind(RowKind.INSERT,cols);
        }

        private Object[] constructRows(Map<Name, Object> data, RelationType<FlexibleDatasetSchema.FlexibleField> schema) {
            return getFields(schema)
                    .map(p -> {
                        Name name = p.getKey();
                        FlexibleDatasetSchema.FieldType ftype = p.getRight();
                        if (ftype.getType() instanceof RelationType) {
                            RelationType<FlexibleDatasetSchema.FlexibleField> subType = (RelationType<FlexibleDatasetSchema.FlexibleField>) ftype.getType();
                            if (isSingleton(ftype)) {
                                return Row.of(constructRows((Map<Name, Object>) data.get(name),subType));
                            } else {
                                int idx = 0;
                                List<Map<Name, Object>> nestedData = (List<Map<Name, Object>>)data.get(name);
                                List<Row> result = new ArrayList<>();
                                for (Map<Name, Object> item : nestedData) {
                                    Object[] cols = constructRows(item,subType);
                                    //Add index
                                    cols = Arrays.copyOf(cols,cols.length+1);
                                    cols[cols.length-1] = idx++;
                                    result.add(Row.of(cols));
                                }
                                return result;
                            }
                        } else {
                            //Data is already correctly prepared by schema validation map-step
                            return data.get(name);
                        }
                    })
                    .toArray();
        }

    }


}
