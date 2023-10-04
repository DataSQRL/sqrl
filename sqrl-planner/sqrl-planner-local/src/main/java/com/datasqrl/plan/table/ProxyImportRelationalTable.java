/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.rules.SQRLConverter.Config.ConfigBuilder;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

/**
 * A relational table that is defined by the imported data from a {@link TableSource}.
 * <p>
 * This is a phyiscal relation with a schema that captures the input data.
 */
public class ProxyImportRelationalTable extends PhysicalRelationalTable {

  @Getter
  private final ImportedRelationalTableImpl baseTable;

  public ProxyImportRelationalTable(@NonNull Name rootTableId, @NonNull Name tableName,
      @NonNull TimestampInference timestamp, @NonNull RelDataType rowType,
      ImportedRelationalTableImpl baseTable, TableStatistic tableStatistic) {
    super(rootTableId, tableName, TableType.STREAM, rowType, timestamp,   1, tableStatistic);
    this.baseTable = baseTable;
  }

  @Override
  public int addInlinedColumn(AddedColumn column, @NonNull RelDataTypeFactory typeFactory,
                              Optional<Integer> timestampScore) {
    int index = super.addInlinedColumn(column, typeFactory, timestampScore);
    //TODO: Need to refactor this. This makes the assumption that it's a timestamp column
    Preconditions.checkArgument(timestampScore.isPresent());
    this.timestamp = TimestampInference.buildImport().addImport(index, timestampScore.get()).build();
    return index;
  }

  @Override
  public List<ExecutionStage> getSupportedStages(ExecutionPipeline pipeline, ErrorCollector errors) {
    List<ExecutionStage> stages = pipeline.getStages().stream().filter(stage ->
            baseTable.getSupportsStage().test(stage))
        .collect(Collectors.toList());
    errors.checkFatal(!stages.isEmpty(),"Could not supported execution stage for "
        + "table [%s] in pipeline [%s]", this, pipeline);
    return stages;
  }

  @Override
  public ConfigBuilder getBaseConfig() {
    SQRLConverter.Config.ConfigBuilder builder = SQRLConverter.Config.builder();
    getAssignedStage().ifPresent(stage -> builder.stage(stage));
    return builder;
  }
}
