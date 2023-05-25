/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.io.tables.AbstractExternalTable;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.ReservedName;
import java.util.List;
import java.util.function.Predicate;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

@Value
public class ImportedRelationalTableImpl extends SourceRelationalTableImpl implements
    ImportedRelationalTable {

  TableSource tableSource;
  RelDataType baseRowType;

  //Currently, we hardcode all table sources to support only stream engines
  private final Predicate<ExecutionStage> supportsStage =
      stage -> stage.getEngine().getType()== Type.STREAM;

  public ImportedRelationalTableImpl(@NonNull Name nameId, RelDataType baseRowType,
      TableSource tableSource) {
    super(nameId);
    this.baseRowType = baseRowType;
    this.tableSource = tableSource;
  }

  @Override
  public RelDataType getRowType() {
    return baseRowType;
  }

  @Override
  public List<String> getPrimaryKeyNames() {
    return List.of(ReservedName.UUID.getCanonical());
  }


}
