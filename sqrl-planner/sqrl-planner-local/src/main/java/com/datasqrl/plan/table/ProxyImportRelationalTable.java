/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.calcite.TimestampAssignableTable;
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

import com.datasqrl.util.CalciteUtil;
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
public class ProxyImportRelationalTable extends PhysicalRelationalTable implements TimestampAssignableTable {

  @Getter
  private final ImportedRelationalTableImpl baseTable;

  public ProxyImportRelationalTable(@NonNull Name rootTableId, @NonNull Name tableName,
      @NonNull TimestampInference timestamp, @NonNull RelDataType rowType,
      ImportedRelationalTableImpl baseTable, TableStatistic tableStatistic) {
    super(rootTableId, tableName, TableType.STREAM, rowType, timestamp,   1, tableStatistic);
    this.baseTable = baseTable;
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

  @Override
  public void assignTimestamp(int index) {
    timestamp.getCandidateByIndex(index).fixAsTimestamp();
  }

  @Override
  public int addColumn(@NonNull AddedColumn column, @NonNull RelDataTypeFactory typeFactory) {
    int colIdx = super.addColumn(column, typeFactory);
    if (CalciteUtil.isTimestamp(column.getDataType())) {
      //Add timestamp candidate
      TimestampInference.ImportBuilder timestampBuilder = TimestampInference.buildImport();
      timestamp.getCandidates().forEach(cand -> timestampBuilder.addImport(cand.getIndex(), cand.getScore()));
      timestampBuilder.addImport(colIdx, CalciteTableFactory.ADDED_TIMESTAMP_SCORE);
      timestamp = timestampBuilder.build();
    }
    return colIdx;
  }

}
