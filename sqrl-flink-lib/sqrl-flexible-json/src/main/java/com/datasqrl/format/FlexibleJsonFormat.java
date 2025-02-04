package com.datasqrl.format;

import static org.apache.flink.formats.json.JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER;
import static org.apache.flink.formats.json.JsonFormatOptions.MAP_NULL_KEY_LITERAL;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.io.tables.SchemaValidator;
import com.datasqrl.schema.constraint.NotNull;
import com.datasqrl.schema.input.FlexibleFieldSchema.Field.Builder;
import com.datasqrl.schema.input.FlexibleFieldSchema.FieldType;
import com.datasqrl.schema.input.FlexibleSchemaValidator;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.FlexibleTypeMatcher;
import com.datasqrl.schema.input.RelationType;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.datasqrl.schema.input.SchemaElementDescription;
import com.datasqrl.schema.input.SchemaField;
import com.datasqrl.schema.type.Type;
import com.datasqrl.schema.type.basic.BigIntType;
import com.datasqrl.schema.type.basic.BooleanType;
import com.datasqrl.schema.type.basic.DoubleType;
import com.datasqrl.schema.type.basic.IntervalType;
import com.datasqrl.schema.type.basic.ObjectType;
import com.datasqrl.schema.type.basic.StringType;
import com.datasqrl.schema.type.basic.TimestampType;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatFactory;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonFormatOptionsUtil;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

public class FlexibleJsonFormat implements DeserializationFormatFactory,
    SerializationFormatFactory {

  public static final String FORMAT_NAME = "flexible-json";

  @Override
  public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
      Context context, ReadableConfig formatOptions) {
    FactoryUtil.validateFactoryOptions(this, formatOptions);

    return new ProjectableDecodingFormat<DeserializationSchema<RowData>>() {
      @SneakyThrows
      @Override
      public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context,
          DataType physicalDataType, int[][] projections) {
        final DataType producedDataType = Projection.of(projections).project(physicalDataType);
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation(
            producedDataType);
        JsonRowDataDeserializationSchema jsonRowDataDeserializationSchema = new JsonRowDataDeserializationSchema(
            rowType, rowDataTypeInfo, false, false, TimestampFormat.ISO_8601);

        SchemaAdjustmentSettings settings = SchemaAdjustmentSettings.DEFAULT;
        RelationType relationType = createFlexibleTableSchema(rowType);
        FlexibleTableSchema schema = new FlexibleTableSchema(Name.system("schema"),
            SchemaElementDescription.NONE, null, false,relationType, List.of());
        SchemaValidator validator = new FlexibleSchemaValidator(schema,
            true, settings, NameCanonicalizer.SYSTEM,
            new FlexibleTypeMatcher(settings));

        return new JsonFlexibleSchemaDelegate(jsonRowDataDeserializationSchema, validator);
      }

      @Override
      public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
      }
    };
  }

  private RelationType createFlexibleTableSchema(RowType rowType) {
    List<SchemaField> types = new ArrayList<>();
    Builder builder = new Builder();
    for (RowField field : rowType.getFields()) {
      builder.setName(Name.system(field.getName()));

      switch (field.getType().getTypeRoot()) {
        case CHAR:
        case VARCHAR:
        case BOOLEAN:
        case DECIMAL:
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
        case DATE:
        case TIME_WITHOUT_TIME_ZONE:
        case TIMESTAMP_WITHOUT_TIME_ZONE:
        case TIMESTAMP_WITH_TIME_ZONE:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_DAY_TIME:
        case MAP:
          builder.setTypes(List.of(new FieldType(Name.system(field.getName()),
              getType(field.getType(), this::createFlexibleTableSchema), 0,
              field.getType().isNullable() ? List.of() : List.of(NotNull.INSTANCE))));
          break;
        case BINARY:
        case VARBINARY:
          break;
        case ARRAY:
          //todo: array depth
          ArrayType arrayType = (ArrayType) field.getType();
          builder.setTypes(List.of(new FieldType(Name.system(field.getName()),
              getType(arrayType.getElementType(), this::createFlexibleTableSchema), 1,
              field.getType().isNullable() ? List.of() : List.of(NotNull.INSTANCE))));

          break;
        case MULTISET:
          break;
        case ROW:
          builder.setTypes(List.of(new FieldType(Name.system(field.getName()),
              createFlexibleTableSchema((RowType)field.getType()), 0,
              field.getType().isNullable() ? List.of() : List.of(NotNull.INSTANCE))));
          break;
        case DISTINCT_TYPE:
          break;
        case STRUCTURED_TYPE:
          break;
        case NULL:
          break;
        case RAW:
          break;
        case SYMBOL:
          break;
        case UNRESOLVED:
          break;
      }

      types.add(builder.build());
    }


    return new RelationType(types);
  }

  private Type getType(LogicalType type, Function<RowType, Type> rowCallback) {
    switch (type.getTypeRoot()) {
      case DATE: //do not alter fields during schema adjustment
      case TIME_WITHOUT_TIME_ZONE:
      case MAP:
        return ObjectType.INSTANCE;
      case CHAR:
      case VARCHAR:
        return StringType.INSTANCE;
      case BOOLEAN:
        return BooleanType.INSTANCE;
      case BINARY:
      case VARBINARY:
        break;
      case DECIMAL:
      case FLOAT:
      case DOUBLE:
        return DoubleType.INSTANCE;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return BigIntType.INSTANCE;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TimestampType.INSTANCE;
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY_TIME:
        return IntervalType.INSTANCE;
      case ROW:
        return rowCallback.apply((RowType)type);
      case ARRAY:
      case MULTISET:
      case DISTINCT_TYPE:
      case STRUCTURED_TYPE:
      case NULL:
      case RAW:
      case SYMBOL:
      case UNRESOLVED:
    }
    throw new RuntimeException("Cannot use flexible schema with type: " + type.getTypeRoot().name());
  }

  @Override
  public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
      Context context, ReadableConfig formatOptions) {
    FactoryUtil.validateFactoryOptions(this, formatOptions);
    JsonFormatOptionsUtil.validateEncodingFormatOptions(formatOptions);

    TimestampFormat timestampOption = JsonFormatOptionsUtil.getTimestampFormat(formatOptions);
    JsonFormatOptions.MapNullKeyMode mapNullKeyMode =
        JsonFormatOptionsUtil.getMapNullKeyMode(formatOptions);
    String mapNullKeyLiteral = formatOptions.get(MAP_NULL_KEY_LITERAL);

    final boolean encodeDecimalAsPlainNumber =
        formatOptions.get(ENCODE_DECIMAL_AS_PLAIN_NUMBER);

    return new EncodingFormat<SerializationSchema<RowData>>() {
      @Override
      public SerializationSchema<RowData> createRuntimeEncoder(
          DynamicTableSink.Context context, DataType consumedDataType) {
        final RowType rowType = (RowType) consumedDataType.getLogicalType();
        return new SqrlJsonRowDataSerializationSchema(
            rowType,
            timestampOption,
            mapNullKeyMode,
            mapNullKeyLiteral,
            encodeDecimalAsPlainNumber);
      }

      @Override
      public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
      }
    };
  }

  @Override
  public String factoryIdentifier() {
    return FORMAT_NAME;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return new JsonFormatFactory().requiredOptions();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return new JsonFormatFactory().optionalOptions();
  }
}
