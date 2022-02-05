package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.parser.processor.ImplicitKeyPropagator.GroupInformation;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.PlannerResult;
import ai.dataeng.sqml.planner.RelToSql;
import ai.dataeng.sqml.planner.RowToSchemaConverter;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;

@AllArgsConstructor
public class ExpressionProcessorImpl implements ExpressionProcessor {

  HeuristicPlannerProvider plannerProvider;

  @Override
  public void process(ExpressionAssignment expr, Namespace namespace) {
    Optional<NamePath> tableName = expr.getNamePath().getPrefix();
    if (tableName.isEmpty()) {
      throw new RuntimeException("Could not assign expression to root");
    }
    Table table = namespace.lookup(tableName.get())
        .orElseThrow(() -> new RuntimeException("Could not find table"));

    String sql = String.format("SELECT %s AS %s FROM _",
        expr.getSql(), expr.getNamePath().getLast().toString());

    PlannerResult result = plannerProvider.createPlanner().plan(tableName, namespace, sql);
    ImplicitKeyPropagator infoPropagator = new ImplicitKeyPropagator();
    GroupInformation info = infoPropagator.propagate(result.getRoot());
    RelNode added = info.getBuilder().build();

    System.out.println("Before: \n" +RelToSql.convertToSql(result.getRoot()));
    System.out.println("After: \n" +RelToSql.convertToSql(added));
    System.out.println("Plan Before:\n" +result.getRoot().explain());
    System.out.println("Plan After:\n" +added.explain());

//    System.out.println(result.getRoot().explain());
    List<Column> fieldList = RowToSchemaConverter.convert(result.getRoot());
    table.addField(fieldList.get(0));
  }
}