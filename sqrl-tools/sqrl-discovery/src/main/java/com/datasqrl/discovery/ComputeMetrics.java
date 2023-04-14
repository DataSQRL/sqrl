/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery;

import com.datasqrl.engine.stream.FunctionWithError;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.SourceRecord.Raw;
import com.datasqrl.io.stats.SourceTableStatistics;
import com.datasqrl.io.tables.AbstractExternalTable.Digest;
import com.datasqrl.io.tables.TableSource;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ComputeMetrics implements FunctionWithError<Raw, SourceTableStatistics> {

  private final TableSource.Digest tableDigest;

  public ComputeMetrics(Digest tableDigest) {
    this.tableDigest = tableDigest;
  }

  @Override
  public Optional<SourceTableStatistics> apply(Raw raw,
      Supplier<ErrorCollector> errorCollectorSupplier) {
    SourceTableStatistics acc = new SourceTableStatistics();
    ErrorCollector errors = errorCollectorSupplier.get();
    acc.validate(raw, tableDigest, errors);
    if (!errors.isFatal()) {
      acc.add(raw, tableDigest);
      return Optional.of(acc);
    }
    return Optional.empty();
  }
}
