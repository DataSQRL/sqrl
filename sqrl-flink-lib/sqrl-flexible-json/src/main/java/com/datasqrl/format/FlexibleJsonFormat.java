package com.datasqrl.format;

import static org.apache.flink.formats.json.JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER;
import static org.apache.flink.formats.json.JsonFormatOptions.MAP_NULL_KEY_LITERAL;

import java.util.Set;
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
import org.apache.flink.table.types.logical.RowType;

public class FlexibleJsonFormat implements DeserializationFormatFactory,
    SerializationFormatFactory {

  public static final String FORMAT_NAME = "flexible-json";

  /**
   * This just delegates to the "standard" json format in Flink
   * @param context
   * @param formatOptions
   * @return
   */
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
        return jsonRowDataDeserializationSchema;
      }

      @Override
      public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
      }
    };
  }

  /**
   * This uses a SQRL specific encoding format so that we can add support for SQRL types
   * @param context
   * @param formatOptions
   * @return
   */
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
