package ai.dataeng.sqml.parser.processor;


import static ai.dataeng.sqml.tree.name.Name.PARENT_RELATIONSHIP;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.parser.processor.ImplicitKeyPropagator.GroupInformation;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Dataset;
import ai.dataeng.sqml.planner.Planner;
import ai.dataeng.sqml.planner.PlannerResult;
import ai.dataeng.sqml.planner.RelToSql;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.Relationship.Multiplicity;
import ai.dataeng.sqml.planner.Relationship.Type;
import ai.dataeng.sqml.planner.RowToSchemaConverter;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;

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

    ImplicitKeyPropagator infoPropagator = new ImplicitKeyPropagator();
    GroupInformation info = infoPropagator.propagate(result.getRoot());
    RelNode added = info.getBuilder().build();

    System.out.println("Before: \n" +RelToSql.convertToSql(result.getRoot()));
    System.out.println("After: \n" + RelToSql.convertToSql(added));
    System.out.println("Plan Before:\n" +result.getRoot().explain());
    System.out.println("Plan After:\n" +added.explain());

//    ExpressionProcessorImpl.propagateGroupInformation(result.getRoot());
    List<Column> fieldList = RowToSchemaConverter.convert(added);

    //If there is no alias & it is a single column then add it as a column to the parent table
    //todo this is a hack. Unnamed columns in calcite have the form of expr$
    if (fieldList.size() == 1 && fieldList.get(0).getName().getCanonical().contains("$")) {
      NamePath namePath = statement.getNamePath().getPrefix()
          .orElseThrow(()->new RuntimeException("Unnamed queries cannot be assigned to root"));
      Table table = namespace.lookup(namePath)
          .orElseThrow(()->new RuntimeException("Could not find prefix table"));
      fieldList.get(0).setName(statement.getNamePath().getLast());
      table.addField(fieldList.get(0));
      return;
    }

    Table destination = namespace.createTable(statement.getNamePath().getLast(), statement.getNamePath(), false);
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
        source, destination, Type.JOIN, Multiplicity.MANY, null));
    destination.addField(new Relationship(PARENT_RELATIONSHIP, destination, source, Type.PARENT, Multiplicity.ONE,
        null));

  }
}
