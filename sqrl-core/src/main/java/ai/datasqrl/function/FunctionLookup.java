package ai.datasqrl.function;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
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
        SqrlOperatorTable.instance().getOperatorList(), e -> e.getName());
    this.rewritingFunctionMap = new HashMap<>();
    this.rewritingFunctionMap.put(Name.system("roundToMonth"), new RoundToMonth());
    this.rewritingFunctionMap.put(Name.system("now"), new Now());
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
