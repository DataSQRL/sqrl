//package com.datasqrl.schema.input;
//
//import com.datasqrl.engine.stream.FunctionWithError;
//import com.datasqrl.error.ErrorCollector;
//import com.datasqrl.io.SourceRecord.Named;
//import com.datasqrl.io.SourceRecord.Raw;
//import com.datasqrl.name.Name;
//import com.datasqrl.name.NameCanonicalizer;
//import io.vertx.core.json.JsonObject;
//import io.vertx.json.schema.JsonSchema;
//import io.vertx.json.schema.JsonSchemaOptions;
//import io.vertx.json.schema.OutputUnit;
//import io.vertx.json.schema.Validator;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import java.util.function.Consumer;
//import java.util.stream.Collectors;
//import lombok.Value;
//
//@Value
//public class JsonSchemaValidator implements SchemaValidator {
//
//  JsonSchema schema;
//  JsonSchemaOptions options;
//  NameCanonicalizer canonicalizer;
//
//  @Override
//  public FunctionWithError<Raw, Named> getFunction() {
//    return new JsonSchemaFunction();
//  }
//
//  public class JsonSchemaFunction implements FunctionWithError<Raw, Named> {
//
//    Validator validator = Validator.create(schema, options);
//
//    @Override
//    public Optional<Named> apply(Raw raw, Consumer<ErrorCollector> errorCollectorConsumer) {
//      JsonObject jsonObject = new JsonObject(raw.getData());
//      OutputUnit result = validator.validate(jsonObject);
//      if (!result.getValid()) {
//        ErrorCollector collector = ErrorCollector.root();
//        result.getErrors().forEach(e -> collector.fatal(e.toString()));
//        errorCollectorConsumer.accept(collector);
//        return Optional.empty();
//      }
//
//      return Optional.of(raw.replaceData(adjust(raw.getData())));
//    }
//
//    private Map<Name, Object> adjust(Map<String, Object> data) {
//      //Todo: convert to our objects
//      return data.entrySet().stream()
//          .collect(Collectors.toMap(e -> canonicalizer.name(e.getKey()), e -> {
//            if (e.getValue() instanceof List) {
//              return adjust((List) e);
//            } else if (e.getValue() instanceof Map) {
//              return adjust((Map<String, Object>) e.getValue());
//            } else {
//              return e.getValue();
//            }
//          }));
//    }
//
//    private List adjust(List<Object> data) {
//      return data.stream()
//          .map(e -> {
//            if (e instanceof List) {
//              return adjust((List) e);
//            } else if (e instanceof Map) {
//              return adjust((Map<String, Object>) e);
//            } else {
//              return e;
//            }
//          })
//          .collect(Collectors.toList());
//    }
//  }
//}
