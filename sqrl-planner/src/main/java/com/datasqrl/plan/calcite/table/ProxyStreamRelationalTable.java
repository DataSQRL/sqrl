/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.table;

import com.datasqrl.io.stats.TableStatistic;
import com.datasqrl.name.Name;
import com.datasqrl.engine.pipeline.ExecutionStage;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;

/**
 * A relational table that is defined by a {@link StreamRelationalTableImpl}, i.e. the change stream
 * that is generated from an underlying state table as captured by the {@link RelNode} in the
 * {@code sourceTable}.
 */
public class ProxyStreamRelationalTable extends ProxySourceRelationalTable {

  @Getter
  private final StreamRelationalTableImpl baseTable;

  public ProxyStreamRelationalTable(@NonNull Name rootTableId,
      @NonNull TimestampHolder.Base timestamp,
      RelNode relNode, StreamRelationalTableImpl baseTable, ExecutionStage execution,
      TableStatistic tableStatistic) {
    super(rootTableId, TableType.STREAM, relNode, PullupOperator.Container.EMPTY, timestamp,
        1,
        tableStatistic,
        execution);
    this.baseTable = baseTable;
  }

}
