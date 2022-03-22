package ai.dataeng.sqml.execution.flink.ingest.schema;

import ai.dataeng.sqml.execution.flink.environment.util.FlinkUtilities;
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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class FlinkTableConverter {

    public Pair<Schema, TypeInformation> tableSchemaConversion(FlexibleDatasetSchema.TableField table) {
        NamePath path = NamePath.of(table.getName());
        Schema.Builder schemaBuilder = Schema.newBuilder();
        List<String> rowNames = new ArrayList<>();
        List<TypeInformation> rowCols = new ArrayList<>();
        getFields(table.getFields()).map(p -> fieldTypeSchemaConversion(p.getKey(),p.getRight(),path)).forEach(p -> {
            DataTypes.Field dtf = p.getLeft();
            schemaBuilder.column(dtf.getName(),dtf.getDataType());
            rowNames.add(dtf.getName());
            rowCols.add(p.getRight());
        });
        schemaBuilder.column(ReservedName.UUID.getCanonical(), toFlinkDataType(UuidType.INSTANCE));
        rowNames.add(ReservedName.UUID.getCanonical()); rowCols.add(FlinkUtilities.getFlinkTypeInfo(UuidType.INSTANCE,false));
        schemaBuilder.column(ReservedName.INGEST_TIME.getCanonical(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
        rowNames.add(ReservedName.INGEST_TIME.getCanonical()); rowCols.add(FlinkUtilities.getFlinkTypeInfo(DateTimeType.INSTANCE,false));
        schemaBuilder.column(ReservedName.SOURCE_TIME.getCanonical(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
        rowNames.add(ReservedName.SOURCE_TIME.getCanonical()); rowCols.add(FlinkUtilities.getFlinkTypeInfo(DateTimeType.INSTANCE,false));
        //TODO: adjust based on configuration
//        schemaBuilder.columnByExpression("__rowtime", "CAST(_ingest_time AS TIMESTAMP_LTZ(3))");
        schemaBuilder.watermark(ReservedName.INGEST_TIME.getCanonical(), ReservedName.INGEST_TIME.getCanonical() + " - INTERVAL '10' SECOND");
        return Pair.of(schemaBuilder.build(),Types.ROW_NAMED(rowNames.toArray(new String[0]),rowCols.toArray(new TypeInformation[0])));
    }

    private static Stream<Pair<Name, FlexibleDatasetSchema.FieldType>> getFields(RelationType<FlexibleDatasetSchema.FlexibleField> relation) {
        return relation.getFields().stream().flatMap(field -> field.getTypes().stream().map( ftype -> {
            Name name = FlexibleSchemaHelper.getCombinedName(field,ftype);
            return Pair.of(name,ftype);
        }));
    }

    private Pair<DataTypes.Field,TypeInformation> fieldTypeSchemaConversion(Name name, FlexibleDatasetSchema.FieldType ftype,
                                                      NamePath path) {
//        boolean notnull = !isMixedType && ConstraintHelper.isNonNull(ftype.getConstraints());

        DataType dt; TypeInformation ti;
        if (ftype.getType() instanceof RelationType) {
            List<DataTypes.Field> dtfs = new ArrayList<>();
            List<TypeInformation> tis = new ArrayList<>();
            final NamePath nestedpath = path.resolve(name);
            getFields((RelationType<FlexibleDatasetSchema.FlexibleField>) ftype.getType())
                    .map(p -> fieldTypeSchemaConversion(p.getKey(),p.getRight(),nestedpath))
                    .forEach(p -> {
                        dtfs.add(p.getLeft());
                        tis.add(p.getRight());
                    });
            if (!isSingleton(ftype)) {
                dtfs.add(DataTypes.FIELD(ReservedName.ARRAY_IDX.getCanonical(),toFlinkDataType(IntegerType.INSTANCE)));
                tis.add(BasicTypeInfo.INT_TYPE_INFO);
            }
            dt = DataTypes.ROW(dtfs.toArray(new DataTypes.Field[dtfs.size()]));
            ti = Types.ROW_NAMED(dtfs.stream().map(dtf -> dtf.getName()).toArray(i -> new String[i]),
                    tis.toArray(new TypeInformation[tis.size()]));
            if (!isSingleton(ftype)) {
                dt = DataTypes.ARRAY(dt);
                ti = Types.LIST(ti);
            }
        } else if (ftype.getType() instanceof ArrayType) {
            Pair<DataType,TypeInformation> p = wrapArraySchema((ArrayType) ftype.getType());
            dt = p.getLeft(); ti = p.getRight();
        } else {
            assert ftype.getType() instanceof BasicType;
            dt = toFlinkDataType((BasicType)ftype.getType());
            ti = FlinkUtilities.getFlinkTypeInfo((BasicType) ftype.getType(), false);
        }
        return Pair.of(DataTypes.FIELD(name.getCanonical(),dt),ti);
    }

    private Pair<DataType,TypeInformation> wrapArraySchema(ArrayType arrType) {
        Type subType = arrType.getSubType();
        DataType dt;
        TypeInformation ti;
        if (subType instanceof ArrayType) {
            Pair<DataType,TypeInformation> p = wrapArraySchema((ArrayType) subType);
            dt = p.getLeft(); ti = p.getRight();
        } else {
            assert subType instanceof BasicType;
            dt = toFlinkDataType((BasicType)subType);
            ti = FlinkUtilities.getFlinkTypeInfo((BasicType) subType, false);
        }
        return Pair.of(DataTypes.ARRAY(dt), Types.LIST(ti));
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

    public SourceRecord2RowMapper getRowMapper(FlexibleDatasetSchema.TableField tableSchema) {
        return new SourceRecord2RowMapper(tableSchema);
    }

    public static class SourceRecord2RowMapper implements MapFunction<SourceRecord.Named, Row> {

        private final FlexibleDatasetSchema.TableField tableSchema;

        public SourceRecord2RowMapper(FlexibleDatasetSchema.TableField tableSchema) {
            this.tableSchema = tableSchema;
        }

        @Override
        public Row map(SourceRecord.Named sourceRecord) throws Exception {
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
