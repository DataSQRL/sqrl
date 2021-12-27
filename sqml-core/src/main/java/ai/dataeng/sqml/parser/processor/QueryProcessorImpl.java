package ai.dataeng.sqml.parser.processor;

import static ai.dataeng.sqml.planner.operator.ImportResolver.PARENT_RELATIONSHIP;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Dataset;
import ai.dataeng.sqml.planner.Planner;
import ai.dataeng.sqml.planner.PlannerResult;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.Relationship.Multiplicity;
import ai.dataeng.sqml.planner.Relationship.Type;
import ai.dataeng.sqml.planner.RowToSchemaConverter;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class QueryProcessorImpl implements QueryProcessor {
  private HeuristicPlannerProvider plannerProvider;

  @Override
  public void process(QueryAssignment statement, Namespace namespace) {
    Planner planner = plannerProvider.createPlanner();

    PlannerResult result = planner.plan(
        statement.getNamePath().getPrefix(),
        namespace,
        statement.getSql());



    Table destination = namespace.createTable(statement.getNamePath().getLast(), false);
    List<Column> fieldList = RowToSchemaConverter.convert(result.getRoot().getRowType());
    fieldList.forEach(destination::addField);

    //Assignment to root
    if (statement.getNamePath().getPrefix().isEmpty()) {
      List<Table> tables = new ArrayList<>();
      tables.add(destination);
      Dataset rootTable = new Dataset(Dataset.ROOT_NAMESPACE_NAME, tables);
      namespace.addDataset(rootTable);
      return;
    }

    Table source =
        namespace.lookup(statement.getNamePath().getPrefix().orElseThrow())
            .orElseThrow(()->new RuntimeException(String.format("Could not find table on field %s", statement.getNamePath())));

    source.addField(new Relationship(statement.getNamePath().getLast(),
        source, destination, Type.JOIN, Multiplicity.MANY));
    destination.addField(new Relationship(PARENT_RELATIONSHIP, destination, source, Type.PARENT, Multiplicity.ONE));

    System.out.println(result.getRoot().explain());
  }
}
