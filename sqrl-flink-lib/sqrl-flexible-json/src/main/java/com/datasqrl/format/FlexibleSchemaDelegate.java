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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.jackson.JacksonMapperFactory;

// @Slf4j
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
            .configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
  }

  @Override
  public RowData deserialize(byte[] message) throws IOException {
    if (message == null) {
      return null;
    }

    Map<String, Object> data = parse(message);
    if (data == null) {
      return null;
    }

    ErrorCollector errorCollector =
        new ErrorCollector(ErrorPrefix.ROOT) {
          @Override
          public RuntimeException exception(ErrorLabel label, String msg, Object... args) {
            System.out.println(message);
            return super.exception(label, msg, args);
          }
        };

    Named named = validator.verifyAndAdjust(new Raw(data, Instant.now()), errorCollector);
    if (errorCollector.hasErrors()) {
      System.out.println(ErrorPrinter.prettyPrint(errorCollector));
      return null;
    }

    JsonNode jsonNode = objectMapper.valueToTree(named.getData());
    return (RowData) schema.deserialize(jsonNode.toString().getBytes());
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
