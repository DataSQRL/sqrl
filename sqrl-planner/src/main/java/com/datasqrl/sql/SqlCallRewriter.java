package com.datasqrl.sql;

import com.datasqrl.canonicalizer.NamePath;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlVisitor;

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
      SqlIdentifier nameAsId = call.getOperator().getNameAsId();
      String[] array = nameAsId.names.toArray(String[]::new);
      NamePath names = NamePath.of(array);
      fncModules.add(names);

      SqlIdentifier component = nameAsId.getComponent(nameAsId.names.size() - 1);
      nameAsId.setNames(List.of(component.names.get(0)), List.of(component.getComponentParserPosition(0)));
    }

    return super.visit(call);
  }

  public Set<NamePath> getFncModules() {
    return fncModules;
  }
}
