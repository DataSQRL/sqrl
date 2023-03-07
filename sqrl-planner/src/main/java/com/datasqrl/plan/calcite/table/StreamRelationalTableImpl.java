/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.table;

import com.datasqrl.engine.stream.plan.TableRegistration;
import com.datasqrl.name.Name;
import com.datasqrl.name.ReservedName;
import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.schema.UniversalTable;
import com.google.common.base.Preconditions;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

@Getter
public class StreamRelationalTableImpl extends SourceRelationalTableImpl implements
    StreamRelationalTable {

  @Setter
  private RelNode baseRelation;
  private final RelDataType streamRowType;
  private final BaseRelationMeta baseTableMetaData;
  private final UniversalTable streamSchema;
  private final StateChangeType stateChangeType;

  public StreamRelationalTableImpl(@NonNull Name nameId, RelNode baseRelation,
      RelDataType streamRowType, BaseRelationMeta baseTableMetaData,
      UniversalTable streamSchema, StateChangeType stateChangeType) {
    super(nameId);
    this.baseRelation = baseRelation;
    this.streamRowType = streamRowType;
    this.baseTableMetaData = baseTableMetaData;
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

  @Value
  public static class BaseRelationMeta implements StreamRelationalTable.BaseTableMetaData {

    int[] keyIdx;
    int[] selectIdx;
    int timestampIdx;

    public BaseRelationMeta(AnnotatedLP baseRelation) {
      this.keyIdx = baseRelation.getPrimaryKey().targetsAsArray();
      this.selectIdx = baseRelation.getSelect().targetsAsArray();
      //Get timestamp index, primary key, and projects for the base relation meta data
      if (baseRelation.getType()==TableType.TEMPORAL_STATE) {
        //Must have a fixed timestamp
        this.timestampIdx = baseRelation.timestamp.getTimestampCandidate().getIndex();
      } else {
        Preconditions.checkArgument(baseRelation.getType() == TableType.STATE);
        //We don't have a timestamp
        this.timestampIdx = -1;
      }
    }

    public boolean hasTimestamp() {
      return timestampIdx >=0;
    }

  }

}
