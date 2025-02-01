/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.config.EngineType;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.canonicalizer.Name;
import java.util.function.Predicate;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

/**
 * An source table that represents the source of table data from an external system.
 *
 * This class is wrapped by a {@link ProxyImportRelationalTable} to be used in Calcite schema
 */
@Value
public class ImportedRelationalTableImpl extends SourceRelationalTableImpl implements
    ImportedRelationalTable {

  TableSource tableSource;
  RelDataType baseRowType;

  //Currently, we hardcode all table sources to support only stream engines
  private final Predicate<ExecutionStage> supportsStage =
      stage -> stage.getEngine().getType() == EngineType.STREAMS;

  public ImportedRelationalTableImpl(@NonNull Name nameId, RelDataType baseRowType,
      TableSource tableSource) {
    super(nameId);
    this.baseRowType = baseRowType;
    this.tableSource = tableSource;
  }

  public TableSource getTableSource() {
    return tableSource;
  }

  @Override
  public RelDataType getRowType() {
    return baseRowType;
  }

}
