package com.datasqrl.discovery.process;

import com.datasqrl.engine.stream.FunctionWithError;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.discovery.SourceRecord.Raw;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class ParseJson implements
    FunctionWithError<String, Raw> {

  private transient ObjectMapper mapper = SqrlObjectMapper.INSTANCE;

  @Override
  public Optional<Raw> apply(String line,
      Supplier<ErrorCollector> errorCollector) {
    try {
      Map<String, Object> record = mapper.readValue(line, LinkedHashMap.class);
      return Optional.of(new Raw(record, null));
    } catch (IOException e) {
      errorCollector.get().fatal(e.getMessage());
      return Optional.empty();
    }
  }

}