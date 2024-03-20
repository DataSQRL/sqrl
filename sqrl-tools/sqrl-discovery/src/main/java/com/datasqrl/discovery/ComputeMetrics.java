/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery;

import com.datasqrl.engine.stream.FunctionWithError;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.SourceRecord.Raw;
import com.datasqrl.metadata.stats.SourceTableStatistics;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ComputeMetrics implements FunctionWithError<Raw, SourceTableStatistics> {


  public ComputeMetrics() {
  }

  @Override
  public Optional<SourceTableStatistics> apply(Raw raw,
      Supplier<ErrorCollector> errorCollectorSupplier) {
    SourceTableStatistics acc = new SourceTableStatistics();
    ErrorCollector errors = errorCollectorSupplier.get();
    acc.validate(raw, errors);
    if (!errors.isFatal()) {
      acc.add(raw, null);
      return Optional.of(acc);
    }
    return Optional.empty();
  }
}
