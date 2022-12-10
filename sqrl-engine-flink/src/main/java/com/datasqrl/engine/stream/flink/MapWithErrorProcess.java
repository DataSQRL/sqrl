/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.engine.stream.FunctionWithError;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@Slf4j
public class MapWithErrorProcess<Input, Output> extends ProcessFunction<Input, Output> {

  private final OutputTag<ProcessError> errorTag;
  private final FunctionWithError<Input, Output> function;
  private final ErrorLocation errorLocation;

  public MapWithErrorProcess(OutputTag<ProcessError> errorTag,
      FunctionWithError<Input, Output> function, ErrorLocation errorLocation) {
    this.errorTag = errorTag;
    this.function = function;
    this.errorLocation = errorLocation;
  }

  @Override
  public void processElement(Input input, Context context,
      Collector<Output> out) {
    ErrorHolder errorHolder = new ErrorHolder(errorLocation);
    Optional<Output> result = function.apply(input, errorHolder);
    if (errorHolder.hasErrors()) {
      ProcessError perr = ProcessError.of(errorHolder.errors, input);
      context.output(errorTag, perr);
      log.error(perr.toString());
    }
    if (result.isPresent()) {
      out.collect(result.get());
    }
  }

  private static class ErrorHolder implements Supplier<ErrorCollector> {

    private final ErrorLocation location;
    private ErrorCollector errors;

    private ErrorHolder(ErrorLocation location) {
      this.location = location;
    }

    public boolean hasErrors() {
      return errors != null && errors.hasErrors();
    }

    @Override
    public ErrorCollector get() {
      errors = new ErrorCollector(location);
      return errors;
    }
  }

}
