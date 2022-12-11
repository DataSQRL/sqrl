package com.datasqrl.schema.input;

import com.datasqrl.engine.stream.FunctionWithError;
import com.datasqrl.io.SourceRecord;

public interface SchemaValidator {
  FunctionWithError<SourceRecord.Raw, SourceRecord.Named> getFunction();
}
