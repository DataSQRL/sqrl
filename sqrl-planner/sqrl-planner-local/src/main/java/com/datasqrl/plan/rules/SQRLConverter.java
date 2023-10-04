package com.datasqrl.plan.rules;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.AnalyzedAPIQuery;
import com.datasqrl.plan.hints.WatermarkHint;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.table.*;
import com.datasqrl.plan.util.SelectIndexMap;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;
import java.util.function.Consumer;

@Value
public class SQRLConverter {

  private RelBuilder relBuilder;

  public SQRLConverter(RelBuilder relBuilder) {
    this.relBuilder = relBuilder;
  }

  public AnnotatedLP convert(final RelNode relNode, Config config, ErrorCollector errors) {
    ExecutionAnalysis exec = ExecutionAnalysis.of(config.getStage());
    SQRLLogicalPlanRewriter sqrl2sql = new SQRLLogicalPlanRewriter(relBuilder, exec,
        errors, config);
    RelNode converted = relNode.accept(sqrl2sql);
    AnnotatedLP alp = sqrl2sql.getRelHolder(converted);
    alp = alp.postProcess(relBuilder, config.getFieldNames(relNode), exec, errors);
    return alp;
  }

  public RelNode convertAPI(AnalyzedAPIQuery query, ExecutionStage stage, ErrorCollector errors) {
    AnnotatedLP alp = convert(query.getRelNode(), query.getBaseConfig().withStage(stage), errors);
    //Rewrite query to inline sort or use a default sort
    alp = alp.withDefaultSort().inlineSort(relBuilder, ExecutionAnalysis.of(stage));
    assert alp.getPullups().isEmpty();
    return alp.getRelNode();
  }

  public RelNode convert(PhysicalTable table, Config config, ErrorCollector errors) {
    return convert(table,config,true,errors);
  }

  public RelNode convert(PhysicalTable table, Config config,
                         boolean addWatermark, ErrorCollector errors) {
    ExecutionAnalysis exec = ExecutionAnalysis.of(config.getStage());
    if (table instanceof ProxyImportRelationalTable) {
      return convert((ProxyImportRelationalTable) table, exec, addWatermark);
    } else { //either QueryRelationalTable or QueryTableFunction
      QueryRelationalTable queryTable = (table instanceof QueryTableFunction)
          ?((QueryTableFunction)table).getQueryTable():(QueryRelationalTable) table;
      AnnotatedLP alp = convert(queryTable.getOriginalRelnode(), config, errors);
      RelBuilder builder = relBuilder.push(alp.getRelNode());
      addColumns(builder, queryTable.getAddedColumns(), alp.select, exec);
      return builder.build();
    }
  }

  private RelNode convert(ProxyImportRelationalTable table, ExecutionAnalysis exec,
      boolean addWatermark) {
    RelBuilder builder = relBuilder.scan(table.getBaseTable().getNameId());
    addColumns(builder, table.getAddedColumns(), SelectIndexMap.identity(table.getNumColumns(), table.getNumColumns()), exec);
    RelNode relNode = builder.build();
    if (addWatermark) {
      int timestampIdx = table.getTimestamp().getTimestampCandidate().getIndex();
      Preconditions.checkArgument(timestampIdx < relNode.getRowType().getFieldCount());
      WatermarkHint watermarkHint = new WatermarkHint(timestampIdx);
      relNode = ((Hintable) relNode).attachHints(List.of(watermarkHint.getHint()));
    }
    return relNode;
  }

  private RelBuilder addColumns(RelBuilder builder, Iterable<AddedColumn> columns,
                                SelectIndexMap select, ExecutionAnalysis exec) {
    for (AddedColumn column : columns) {
      exec.requireRex(column.getBaseExpression());
      int addedIndex = column.appendTo(builder, select);
      select = select.add(addedIndex);
    }
    return builder;
  }

  public static final int DEFAULT_SLIDING_WINDOW_PANES = 50;

  @Value
  @Builder(toBuilder = true)
  public static class Config {

    ExecutionStage stage;

    @Builder.Default
    Consumer<PhysicalRelationalTable> sourceTableConsumer = (t) -> {};
    @Builder.Default
    int slideWindowPanes = DEFAULT_SLIDING_WINDOW_PANES;

    @Builder.Default
    boolean setOriginalFieldnames = false;

    @Builder.Default
    boolean addTimestamp2NormalizedChildTable = true;

    @Builder.Default
    List<String> fieldNames = null;

    public Config withStage(ExecutionStage stage) {
      return toBuilder().stage(stage).build();
    }

    public List<String> getFieldNames(RelNode relNode) {
      return isSetOriginalFieldnames()?relNode.getRowType().getFieldNames(): getFieldNames();
    }
  }
}
