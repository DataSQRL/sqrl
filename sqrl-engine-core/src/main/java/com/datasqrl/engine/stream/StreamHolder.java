/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream;

import com.datasqrl.error.ErrorLocation;

public interface StreamHolder<T> {

  <R> StreamHolder<R> mapWithError(FunctionWithError<T, R> function, String errorName,
      ErrorLocation errorLocation, Class<R> clazz);

  void printSink();

}
