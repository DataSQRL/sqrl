package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.PlannerResult;
import ai.dataeng.sqml.planner.RowToSchemaConverter;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ExpressionProcessorImpl implements ExpressionProcessor {
  HeuristicPlannerProvider plannerProvider;

  @Override
  public void process(ExpressionAssignment expr, Namespace namespace) {
    Optional<NamePath> tableName = expr.getNamePath().getPrefix();

    String sql = String.format("SELECT %s AS %s FROM _",
        expr.getSql(), expr.getNamePath().getLast().toString());

    System.out.println(sql);
    PlannerResult result = plannerProvider.createPlanner().plan(tableName, namespace, sql);
    System.out.println(result.getRoot().explain());
    Table table = namespace.lookup(tableName.get()).orElseThrow(()->new RuntimeException("Could not find table"));
    List<Column> fieldList = RowToSchemaConverter.convert(result.getRoot().getRowType());
    table.addField(fieldList.get(0));
  }
}
