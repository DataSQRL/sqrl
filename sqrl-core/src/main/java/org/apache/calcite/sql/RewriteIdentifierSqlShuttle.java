package org.apache.calcite.sql;

import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

public class RewriteIdentifierSqlShuttle extends SqlShuttle {

    private final Map<String, String> aliasMap;

    public RewriteIdentifierSqlShuttle(Map<String, String> aliasMap) {

      this.aliasMap = aliasMap;
    }
    //todo: how to check if we're at an identifier & not somethign else?
    @Override
    public SqlNode visit(SqlIdentifier id) {
      if (id.names.size() == 2) {
        List<String> names = new ArrayList<>(id.names);
        String newAlias = aliasMap.get(names.get(0));
        Preconditions.checkNotNull(newAlias, "Could not find alias: %s %s", names.get(0), id);
        names.set(0, newAlias);
        return new SqlIdentifier(names, id.getParserPosition()).clone(SqlParserPos.ZERO);
      }
      return id.clone(SqlParserPos.ZERO);
    }
    @Override
    public SqlNode visit(SqlCall call) {
      if (call.getOperator() == SqrlOperatorTable.AS) {
        String alias = ((SqlIdentifier)call.getOperandList().get(1)).getSimple();
        SqlNode[] operands = new SqlNode[] {
            call.getOperandList().get(0).accept(this),
            new SqlIdentifier(aliasMap.get(alias), SqlParserPos.ZERO)
        };

        return new SqlBasicCall(call.getOperator(), operands, call.getParserPosition());
      }
      return super.visit(call);
    }
  }
