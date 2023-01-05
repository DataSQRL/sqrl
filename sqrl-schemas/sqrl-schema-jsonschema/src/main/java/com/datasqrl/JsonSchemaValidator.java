package com.datasqrl;

import com.datasqrl.engine.stream.FunctionWithError;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.SourceRecord.Named;
import com.datasqrl.io.SourceRecord.Raw;
import com.datasqrl.schema.input.SchemaValidator;
import io.vertx.json.schema.OutputUnit;
import io.vertx.json.schema.Validator;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class JsonSchemaValidator implements SchemaValidator {
  Validator validator;

  @Override
  public FunctionWithError<Raw, Named> getFunction() {
    return (SourceRecord.Raw raw,
        Supplier<ErrorCollector> errorCollectorSupplier)-> {
      OutputUnit outputUnit = validator.validate(raw.getData());
      if (outputUnit.getValid()) {
        errorCollectorSupplier.get()
            .fatal("Invalid record encountered");
      }


      return null;
    };
  }


}
