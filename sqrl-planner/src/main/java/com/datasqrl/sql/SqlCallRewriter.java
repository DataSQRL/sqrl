package com.datasqrl.sql;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

import com.datasqrl.canonicalizer.NamePath;

/**
 * Extracts modules and rewrites calls
 */
public class SqlCallRewriter extends SqlShuttle {
  Set<NamePath> fncModules = new LinkedHashSet<>();
  public void performCallRewrite(SqlCall sqlNode) {
    sqlNode.accept(this);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call.getOperator().getNameAsId().names.size() > 1) {
      var nameAsId = call.getOperator().getNameAsId();
      var array = nameAsId.names.toArray(String[]::new);
      var names = NamePath.of(array);
      fncModules.add(names);

      var component = nameAsId.getComponent(nameAsId.names.size() - 1);
      nameAsId.setNames(List.of(component.names.getFirst()), List.of(component.getComponentParserPosition(0)));
    }

    return super.visit(call);
  }

  public Set<NamePath> getFncModules() {
    return fncModules;
  }
}
