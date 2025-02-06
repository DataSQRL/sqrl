package com.datasqrl.plan.rules;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.AnalyzedAPIQuery;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.table.AddedColumn;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.PhysicalTable;
import com.datasqrl.plan.table.ProxyImportRelationalTable;
import com.datasqrl.plan.table.PullupOperator;
import com.datasqrl.plan.table.PullupOperator.Container;
import com.datasqrl.plan.table.QueryRelationalTable;
import com.datasqrl.plan.util.SelectIndexMap;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.immutables.value.Value;

@AllArgsConstructor(onConstructor_=@Inject)
@Getter
public class SQRLConverter {

  RelBuilder relBuilder;
  ExecutionPipeline pipeline;

  public boolean isInlinePullups() {
    return !pipeline.hasReadStages();
  }

  public AnnotatedLP convert(final RelNode relNode, SqrlConverterConfig config, ErrorCollector errors) {
    SQRLLogicalPlanRewriter sqrl2sql = new SQRLLogicalPlanRewriter(relBuilder, config,
        errors);
    RelNode converted = relNode.accept(sqrl2sql);
    AnnotatedLP alp = sqrl2sql.getRelHolder(converted);
    alp = alp.postProcess(relBuilder, relNode, config, isInlinePullups(), errors);
    Preconditions.checkArgument(alp.select.isIdentity(),"Invalid select: %s", alp.select);
    return alp;
  }

  public RelNode convertAPI(AnalyzedAPIQuery query, ExecutionStage stage, ErrorCollector errors) {
    AnnotatedLP alp = convert(query.getRelNode(), query.getBaseConfig().withStage(stage), errors);
    //Rewrite query to inline sort or use a default sort
    alp = alp.withDefaultSort().inlineAll(relBuilder, ExecutionAnalysis.of(stage));
    Preconditions.checkArgument(alp.getPullups().isEmpty());
    return alp.getRelNode();
  }

  @Value
  @Getter
  @AllArgsConstructor
  public static class TablePlan {
    RelNode relNode;
    PullupOperator.Container pullups;

    public static TablePlan of(AnnotatedLP alp) {
      return new TablePlan(alp.getRelNode(), alp.getPullups());
    }
  }

  public TablePlan convert(PhysicalTable table, SqrlConverterConfig config, ErrorCollector errors) {
    RelBuilder builder;
    ExecutionAnalysis exec = ExecutionAnalysis.of(config.getStage());
    PhysicalRelationalTable physicalTable;
    PullupOperator.Container pullups = Container.EMPTY;
    boolean addWatermark = true;
    if (table instanceof ProxyImportRelationalTable) {
      physicalTable = (PhysicalRelationalTable)table;
      builder = relBuilder.scan(((ProxyImportRelationalTable)table).getBaseTable().getNameId());
    } else { //either QueryRelationalTable or QueryTableFunction
      QueryRelationalTable queryTable = (table instanceof QueryTableFunction qtf)
          ?qtf.getQueryTable():(QueryRelationalTable) table;
      AnnotatedLP alp = convert(queryTable.getOriginalRelnode(), config, errors);
      builder = relBuilder.push(alp.getRelNode());
      physicalTable = queryTable;
      pullups = alp.getPullups();
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
    return new TablePlan(builder.build(), pullups);
  }

  public static final int DEFAULT_SLIDING_WINDOW_PANES = 50;

}
