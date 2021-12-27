package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.QueryAssignment;

public class QueryProcessorImpl implements QueryProcessor {

  @Override
  public void process(QueryAssignment statement, Namespace namespace) {

//        QueryAssignment query = (QueryAssignment) statement;
//        SqrlTable2 table = env.addQuery(query.getNamePath(), query.getSql());
//        catalogManager.addTable(table);
//
//        //Add a relationship column if present
//        Optional<NamePath> prefix = query.getNamePath().getPrefix();
//        if (prefix.isPresent()) {
//          SqrlTable2 prefixTable = catalogManager.getCurrentTable(prefix.get());
//          env.addRelationshipToSchema(prefixTable, query.getNamePath().getLast(), table);
////          SqrlTable modifiedTable = prefixTable.addRelColumn();
//          //TODO: add rel is in place, make immutable?
////          catalogManager.addTable(prefix.get(), modifiedTable);
//        }

//
//    Planner planner = plannerProvider.createPlanner();
//
//    PlannerResult result = planner.plan(Optional.empty(), namespace,
//        String.format("SELECT * FROM `%s`", "orders"));
//

//  }

  }
}
