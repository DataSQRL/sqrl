package ai.datasqrl.execute.flink;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.execute.FunctionWithError;
import lombok.Value;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public class MapWithErrorProcess<Input, Output> extends ProcessFunction<Input, Output> {

  public static final String ERROR_TAG_PREFIX = "error";

  private final OutputTag<Error> errorTag;
  private final FunctionWithError<Input, Output> function;

  public MapWithErrorProcess(OutputTag<Error> errorTag, FunctionWithError<Input, Output> function) {
    this.errorTag = errorTag;
    this.function = function;
  }

  @Override
  public void processElement(Input input, Context context,
      Collector<Output> out) {
    ErrorHolder errorHolder = new ErrorHolder();
    Optional<Output> result = function.apply(input,errorHolder);
    if (errorHolder.hasErrors()) {
      context.output(errorTag, MapWithErrorProcess.Error.of(errorHolder.errors, input));
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

  @Value
  public static class Error<Input> implements Serializable {

    private List<String> errors;
    private Input input;

    public static<Input> Error<Input> of(ErrorCollector errors, Input input) {
      List<String> errorMsgs = new ArrayList<>();
      errors.forEach(e -> errorMsgs.add(e.toString()));
      return new Error(errorMsgs, input);
    }

  }
}
