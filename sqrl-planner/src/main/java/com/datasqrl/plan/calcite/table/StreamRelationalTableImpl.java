/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.table;

import com.datasqrl.engine.stream.flink.plan.TableRegistration;
import com.datasqrl.name.Name;
import com.datasqrl.name.ReservedName;
import com.datasqrl.schema.UniversalTable;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

@Getter
public class StreamRelationalTableImpl extends SourceRelationalTableImpl implements
    StreamRelationalTable {

  @Setter
  private RelNode baseRelation;
  private final RelDataType streamRowType;
  private final UniversalTable streamSchema;
  private final StateChangeType stateChangeType;

  public StreamRelationalTableImpl(@NonNull Name nameId, RelNode baseRelation,
      RelDataType streamRowType,
      UniversalTable streamSchema, StateChangeType stateChangeType) {
    super(nameId);
    this.baseRelation = baseRelation;
    this.streamRowType = streamRowType;
    this.streamSchema = streamSchema;
    this.stateChangeType = stateChangeType;
  }

  @Override
  public RelDataType getRowType() {
    return streamRowType;
  }

  @Override
  public List<String> getPrimaryKeyNames() {
    return List.of(ReservedName.UUID.getCanonical());
  }

  @Override
  public <R, C> R accept(TableRegistration<R, C> tableRegistration, C context) {
    return tableRegistration.accept(this, context);
  }
}
