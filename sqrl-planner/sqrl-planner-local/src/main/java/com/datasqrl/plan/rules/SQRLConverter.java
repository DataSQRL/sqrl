package com.datasqrl.plan.rules;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.AnalyzedAPIQuery;
import com.datasqrl.plan.hints.WatermarkHint;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.table.AddedColumn;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.PhysicalTable;
import com.datasqrl.plan.table.ProxyImportRelationalTable;
import com.datasqrl.plan.table.QueryRelationalTable;
import com.datasqrl.plan.util.SelectIndexMap;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.List;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.tools.RelBuilder;

@AllArgsConstructor(onConstructor_=@Inject)
@Getter
public class SQRLConverter {

  RelBuilder relBuilder;

  public AnnotatedLP convert(final RelNode relNode, Config config, ErrorCollector errors) {
    ExecutionAnalysis exec = ExecutionAnalysis.of(config.getStage());
    SQRLLogicalPlanRewriter sqrl2sql = new SQRLLogicalPlanRewriter(relBuilder, exec,
        errors, config);
    RelNode converted = relNode.accept(sqrl2sql);
    AnnotatedLP alp = sqrl2sql.getRelHolder(converted);
    alp = alp.postProcess(relBuilder, relNode, exec, errors);
    Preconditions.checkArgument(alp.select.isIdentity(),"Invalid select: %s", alp.select);
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
    RelBuilder builder;
    ExecutionAnalysis exec = ExecutionAnalysis.of(config.getStage());
    PhysicalRelationalTable physicalTable;
    if (table instanceof ProxyImportRelationalTable) {
      physicalTable = (PhysicalRelationalTable)table;
      builder = relBuilder.scan(((ProxyImportRelationalTable)table).getBaseTable().getNameId());
    } else { //either QueryRelationalTable or QueryTableFunction
      QueryRelationalTable queryTable = (table instanceof QueryTableFunction)
          ?((QueryTableFunction)table).getQueryTable():(QueryRelationalTable) table;
      AnnotatedLP alp = convert(queryTable.getOriginalRelnode(), config, errors);
      builder = relBuilder.push(alp.getRelNode());
      physicalTable = queryTable;
      addWatermark = false; //watermarks only apply to imported tables
    }
    //Add any additional columns that were added to the table after definition
    List<AddedColumn> addedCols = physicalTable.getAddedColumns();
    int baseSelects = physicalTable.getNumSelects() - addedCols.size();
    SelectIndexMap select = SelectIndexMap.identity(baseSelects,baseSelects);
    for (int i = 0; i <addedCols.size(); i++) {
      AddedColumn column = addedCols.get(i);
      int index = baseSelects+i;
      exec.requireRex(column.getBaseExpression()); //Make sure the stage supports the column
      int addedIndex = column.appendTo(builder, index, select);
      select = select.add(addedIndex);
    }
    if (addWatermark) { //TODO: remove and handle in connector definition
      int timestampIdx = table.getTimestamp().getOnlyCandidate();
      Preconditions.checkArgument(timestampIdx < physicalTable.getNumColumns());
      WatermarkHint watermarkHint = new WatermarkHint(timestampIdx);
      watermarkHint.addTo(builder);
    }
    return builder.build();
  }

  public static final int DEFAULT_SLIDING_WINDOW_PANES = 50;

  @Builder(toBuilder = true)
  @AllArgsConstructor
  @Getter
  public static class Config {

    ExecutionStage stage;

    @Builder.Default
    Consumer<PhysicalRelationalTable> sourceTableConsumer = (t) -> {};
    @Builder.Default
    int slideWindowPanes = DEFAULT_SLIDING_WINDOW_PANES;

    @Builder.Default
    boolean setOriginalFieldnames = false;

    @Builder.Default
    List<String> fieldNames = null;

    public Config withStage(ExecutionStage stage) {
      return toBuilder().stage(stage).build();
    }

  }
}
