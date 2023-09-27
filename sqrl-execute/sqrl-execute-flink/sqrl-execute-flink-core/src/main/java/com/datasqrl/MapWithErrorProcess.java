/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

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

  private final OutputTag<InputError> errorTag;
  private final FunctionWithError<Input, Output> function;
  private final ErrorLocation errorLocation;

  public MapWithErrorProcess(OutputTag<InputError> errorTag,
      FunctionWithError<Input, Output> function, ErrorLocation errorLocation) {
    this.errorTag = errorTag;
    this.function = function;
    this.errorLocation = errorLocation;
  }

  @Override
  public void processElement(Input input, Context context,
      Collector<Output> out) {
    ErrorHolder errorHolder = new ErrorHolder(errorLocation);
    Optional<Output> result = Optional.empty();
    try {
      result = function.apply(input, errorHolder);
    } catch (Exception e) {
      errorHolder.get().handle(e);
    }
    if (!errorHolder.isEmpty()) {
      InputError inputErr = InputError.of(errorHolder.errors, input);
      context.output(errorTag, inputErr);
      inputErr.printToLog(log);
    }
    if (result.isPresent()) {
      out.collect(result.get());
    }
  }

  /**
   * This class is a proxy in front of ErrorCollector which lazily initializes it
   * in order to avoid creating an {@link com.datasqrl.error.ErrorCollection} for
   * every processed record which would be a significant overhead.
   */
  private static class ErrorHolder implements Supplier<ErrorCollector> {

    private final ErrorLocation location;
    private ErrorCollector errors;

    private ErrorHolder(ErrorLocation location) {
      this.location = location;
    }

    public boolean isEmpty() {
      return errors == null || errors.isEmpty();
    }

    @Override
    public synchronized ErrorCollector get() {
      if (errors==null) {
        errors = new ErrorCollector(location);
      }
      return errors;
    }
  }

}
