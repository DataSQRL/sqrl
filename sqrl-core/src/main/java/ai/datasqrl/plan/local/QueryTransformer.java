package ai.datasqrl.plan.local;

import ai.datasqrl.parse.tree.DefaultTraversalVisitor;
import ai.datasqrl.plan.local.analyzer.QueryGenerator.Scope;
import org.apache.calcite.sql.SqlNode;

public class QueryTransformer extends DefaultTraversalVisitor<SqlNode, Scope> {

}
