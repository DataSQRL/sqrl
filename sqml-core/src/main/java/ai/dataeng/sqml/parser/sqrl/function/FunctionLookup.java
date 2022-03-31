package ai.dataeng.sqml.parser.sqrl.function;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import graphql.com.google.common.collect.ImmutableMap;
import graphql.com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class FunctionLookup {
  private final ImmutableMap<String, SqlOperator> opMap;
  private final Map<Name, RewritingFunction> rewritingFunctionMap;

  public FunctionLookup() {
    this.opMap = Maps.uniqueIndex(
        SqlStdOperatorTable.instance().getOperatorList(), e->e.getName());
    this.rewritingFunctionMap = new HashMap<>();
    this.rewritingFunctionMap.put(Name.system("roundToMonth"), new RoundToMonth());
  }

  public SqrlFunction lookup(NamePath name) {
    //todo: namespaced functions
    if (rewritingFunctionMap.containsKey(name.getLast())) {
      return rewritingFunctionMap.get(name.getLast());
    }
    SqlOperator op = opMap.get(name.getLast().getCanonical().toUpperCase());
    return new SqlNativeFunction(op);
  }
}
