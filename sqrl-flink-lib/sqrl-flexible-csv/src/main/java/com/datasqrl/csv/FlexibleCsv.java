package com.datasqrl.csv;

import java.io.IOException;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.csv.CsvFormatFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;

public class FlexibleCsv implements DeserializationFormatFactory {
  final CsvFormatFactory csvJson;

  public FlexibleCsv() {
    csvJson = new CsvFormatFactory();
  }

  ConfigOption<Boolean> skipHeader = ConfigOptions.key("skip-header").booleanType().defaultValue(true);

  @Override
  public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
      DynamicTableFactory.Context factoryContext, ReadableConfig formatOptions) {
    FactoryUtil.validateFactoryOptions(this, formatOptions);
    ProjectableDecodingFormat<DeserializationSchema<RowData>> decodingFormat =
        (ProjectableDecodingFormat)csvJson.createDecodingFormat(
        factoryContext, formatOptions);

    return new ProjectableDecodingFormat<DeserializationSchema<RowData>>() {
      @SneakyThrows
      @Override
      public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context,
          DataType physicalDataType, int[][] projections) {
        DeserializationSchema<RowData> runtimeDecoder = decodingFormat.createRuntimeDecoder(context,
            physicalDataType, projections);
        boolean skipHeaderBool = formatOptions.get(skipHeader);
        RuntimeDecoderDelegate decoderDelegate = new RuntimeDecoderDelegate(runtimeDecoder,
            skipHeaderBool);
        return decoderDelegate;
      }

      @Override
      public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
      }
    };
  }

  public static class RuntimeDecoderDelegate implements DeserializationSchema<RowData> {

    private final DeserializationSchema<RowData> runtimeDecoder;
    private final boolean skipHeader;
    private boolean hasSkipped = false;

    public RuntimeDecoderDelegate(DeserializationSchema<RowData> runtimeDecoder,
        boolean skipHeader) {
      this.runtimeDecoder = runtimeDecoder;
      this.skipHeader = skipHeader;
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
      if (skipHeader && !hasSkipped) {
        this.hasSkipped = true;
        return null;
      }
      return runtimeDecoder.deserialize(message);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
      return runtimeDecoder.isEndOfStream(nextElement);
    }

    @Override
    public TypeInformation getProducedType() {
      return runtimeDecoder.getProducedType();
    }

    @Override
    public void open(InitializationContext context) throws Exception {
      runtimeDecoder.open(context);
    }
  }

  @Override
  public String factoryIdentifier() {
    return "flexible-csv";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(skipHeader);
  }
}
