package com.datasqrl.format;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.io.SourceRecord.Named;
import com.datasqrl.io.SourceRecord.Raw;
import com.datasqrl.io.tables.SchemaValidator;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.jackson.JacksonMapperFactory;

//@Slf4j
public abstract class FlexibleSchemaDelegate implements DeserializationSchema<RowData> {
  protected DeserializationSchema schema;
  protected final SchemaValidator validator;
  protected ObjectMapper objectMapper;

  public FlexibleSchemaDelegate(DeserializationSchema schema, SchemaValidator validator) {
    this.schema = schema;
    this.validator = validator;
  }

  @Override
  public void open(InitializationContext context) throws Exception {
    schema.open(context);
    objectMapper =
        JacksonMapperFactory.createObjectMapper()
            .configure(
                JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(),
                true);
  }

  @Override
  public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
    if (message == null) {
      System.out.println("message is null for json record, skipping");
      return;
    }

    Map<String, Object> data = parse(message);
    if (data == null) {
      System.out.println("data is null for json record, skipping");
      return;
    }

    ErrorCollector errorCollector = new ErrorCollector(ErrorPrefix.ROOT) {
      @Override
      public RuntimeException exception(ErrorLabel label, String msg, Object... args) {
        System.out.println(message);
        return super.exception(label, msg, args);
      }
    };

    Named named = validator.verifyAndAdjust(new Raw(data, Instant.now()), errorCollector);
    if (errorCollector.hasErrors()) {
      System.out.println("json record has validation errors, skipping");
      System.out.println(ErrorPrinter.prettyPrint(errorCollector));
      return;
    }

    JsonNode jsonNode = objectMapper.valueToTree(named.getData());
    try {
      RowData deserialize = (RowData) schema.deserialize(jsonNode.toString().getBytes());
      out.collect(deserialize);
    } catch (Exception e) {
      System.out.println("json record has could not be deserialized, skipping");
      e.printStackTrace();
    }
  }

  @Override
  public RowData deserialize(byte[] message) throws IOException {
    return null;
  }

  public abstract Map<String, Object> parse(byte[] message);

  @Override
  public boolean isEndOfStream(RowData nextElement) {
    return schema.isEndOfStream(nextElement);
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return schema.getProducedType();
  }
}
