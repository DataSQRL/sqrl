/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import java.util.function.Predicate;

import org.apache.calcite.rel.type.RelDataType;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.io.tables.TableSource;

import lombok.NonNull;
import lombok.Value;

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
      stage -> stage.getEngine().getType() == Type.STREAMS;

  public ImportedRelationalTableImpl(@NonNull Name nameId, RelDataType baseRowType,
      TableSource tableSource) {
    super(nameId);
    this.baseRowType = baseRowType;
    this.tableSource = tableSource;
  }

  @Override
public TableSource getTableSource() {
    return tableSource;
  }

  @Override
  public RelDataType getRowType() {
    return baseRowType;
  }

}
