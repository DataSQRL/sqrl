package ai.dataeng.sqml.parser.sqrl.function;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import graphql.com.google.common.collect.ImmutableMap;
import graphql.com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class FunctionLookup {
  private final Multimap<String, SqlOperator> opMap;
  private final Map<Name, RewritingFunction> rewritingFunctionMap;

  public FunctionLookup() {
    this.opMap = Multimaps.index(
        SqlStdOperatorTable.instance().getOperatorList(), e -> e.getName());
    this.rewritingFunctionMap = new HashMap<>();
    this.rewritingFunctionMap.put(Name.system("roundToMonth"), new RoundToMonth());
  }

  public SqrlFunction lookup(NamePath name) {
    //todo: namespaced functions
    if (rewritingFunctionMap.containsKey(name.getLast())) {
      return rewritingFunctionMap.get(name.getLast());
    }
    List<SqlOperator> l = new ArrayList<>(opMap.get(name.getLast().getCanonical().toUpperCase()));
    SqlOperator op = l.get(0);
    return new SqlNativeFunction(op);
  }
}
