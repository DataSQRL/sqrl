package com.datasqrl.format;

import static org.apache.flink.formats.json.JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER;
import static org.apache.flink.formats.json.JsonFormatOptions.FAIL_ON_MISSING_FIELD;
import static org.apache.flink.formats.json.JsonFormatOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.json.JsonFormatOptions.MAP_NULL_KEY_LITERAL;
import static org.apache.flink.formats.json.JsonFormatOptions.MAP_NULL_KEY_MODE;
import static org.apache.flink.formats.json.JsonFormatOptions.TIMESTAMP_FORMAT;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.SchemaValidator;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.input.FlexibleSchemaValidator;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.FlexibleTypeMatcher;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.datasqrl.schema.input.external.SchemaImport;
import com.datasqrl.schema.input.external.TableDefinition;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
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
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

@AutoService(org.apache.flink.table.factories.Factory.class)
public class FlexibleJsonFormat implements DeserializationFormatFactory,
    SerializationFormatFactory {

  ConfigOption<String> SCHEMA = ConfigOptions.key("schema").stringType().noDefaultValue();

  @Override
  public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
      DynamicTableFactory.Context context, ReadableConfig formatOptions) {
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

        ObjectMapper objectMapper = new ObjectMapper();
        String schemaString = formatOptions.get(SCHEMA);
        TableDefinition tableDefinition = objectMapper.readValue(schemaString,
            TableDefinition.class);

        SchemaImport importer = new SchemaImport(Constraint.FACTORY_LOOKUP,
            NameCanonicalizer.SYSTEM);
        Optional<FlexibleTableSchema> convert = importer.convert(tableDefinition,
            ErrorCollector.root());
        SchemaValidator validator = getValidator(SchemaAdjustmentSettings.DEFAULT, true,
            convert.get());

        return new JsonFlexibleSchemaDelegate(jsonRowDataDeserializationSchema, validator);
      }

      @Override
      public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
      }
    };
  }

  public SchemaValidator getValidator(SchemaAdjustmentSettings schemaAdjustmentSettings,
      boolean hasSourceTimestamp, FlexibleTableSchema schema) {
    FlexibleSchemaValidator validator = new FlexibleSchemaValidator(schema, hasSourceTimestamp,
        schemaAdjustmentSettings, NameCanonicalizer.SYSTEM,
        new FlexibleTypeMatcher(schemaAdjustmentSettings));
    return validator;
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
    return "flexible-json";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(FAIL_ON_MISSING_FIELD);
    options.add(IGNORE_PARSE_ERRORS);
    options.add(TIMESTAMP_FORMAT);
    options.add(MAP_NULL_KEY_MODE);
    options.add(MAP_NULL_KEY_LITERAL);
    options.add(ENCODE_DECIMAL_AS_PLAIN_NUMBER);
    options.add(SCHEMA);
    return options;
  }
}
