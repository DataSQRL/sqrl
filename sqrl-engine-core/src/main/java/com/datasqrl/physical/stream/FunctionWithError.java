package com.datasqrl.physical.stream;

import com.datasqrl.error.ErrorCollector;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;

@FunctionalInterface
public interface FunctionWithError<Input,Result> extends BiFunction<Input, Consumer<ErrorCollector>, Optional<Result>>, Serializable {
}
