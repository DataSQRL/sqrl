package com.datasqrl.engine.stream.flink;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.engine.stream.FunctionWithError;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Optional;
import java.util.function.Consumer;

public class MapWithErrorProcess<Input, Output> extends ProcessFunction<Input, Output> {

  private final OutputTag<ProcessError> errorTag;
  private final FunctionWithError<Input, Output> function;

  public MapWithErrorProcess(OutputTag<ProcessError> errorTag, FunctionWithError<Input, Output> function) {
    this.errorTag = errorTag;
    this.function = function;
  }

  @Override
  public void processElement(Input input, Context context,
      Collector<Output> out) {
    ErrorHolder errorHolder = new ErrorHolder();
    Optional<Output> result = function.apply(input,errorHolder);
    if (errorHolder.hasErrors()) {
      context.output(errorTag, ProcessError.of(errorHolder.errors, input));
    }
    if (result.isPresent()) out.collect(result.get());
  }

  private static class ErrorHolder implements Consumer<ErrorCollector> {

    private ErrorCollector errors;

    @Override
    public void accept(ErrorCollector errorMessages) {
      errors = errorMessages;
    }

    public boolean hasErrors() {
      return errors!=null && errors.hasErrors();
    }

  }

}
