/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream;

import com.datasqrl.error.ErrorCollector;
import java.io.Serializable;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

@FunctionalInterface
public interface FunctionWithError<Input, Result> extends
    BiFunction<Input, Supplier<ErrorCollector>, Optional<Result>>, Serializable {

}
