package ai.datasqrl.physical.stream;

import ai.datasqrl.config.error.ErrorCollector;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;

@FunctionalInterface
public interface FunctionWithError<Input,Result> extends BiFunction<Input, Consumer<ErrorCollector>, Optional<Result>>, Serializable {
}
