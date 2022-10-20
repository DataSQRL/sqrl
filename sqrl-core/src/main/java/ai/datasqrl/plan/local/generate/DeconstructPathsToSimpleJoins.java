package ai.datasqrl.plan.local.generate;

import java.util.List;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.AbsoluteTableNamespace;
import org.apache.calcite.sql.validate.RelativeTableNamespace;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlScopedShuttle;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqrlEmptyScope.AbsoluteSQRLTable;
import org.apache.calcite.sql.validate.SqrlEmptyScope.NestedSQRLTable;
import org.apache.calcite.sql.validate.SqrlValidatorImpl;

public class DeconstructPathsToSimpleJoins {

  private final SqrlValidatorImpl validator;

  public DeconstructPathsToSimpleJoins(SqrlValidatorImpl validator) {
    this.validator = validator;
  }

  public SqlNode accept(SqlNode node) {
    if (node instanceof SqlSelect) {
      SqlValidatorScope scope = validator.getSelectScope((SqlSelect) node);
      RewriteTables visitor = new RewriteTables(scope);
      SqlSelect select = (SqlSelect) node;
      select.setFrom(select.getFrom().accept(visitor));
      return node;
    }

    return node;
  }

  class RewriteTables extends SqlScopedShuttle {

    protected RewriteTables(SqlValidatorScope initialScope) {
      super(initialScope);
    }

    @Override
    protected SqlNode visitScoped(SqlCall call) {
      switch (call.getKind()) {
        case SELECT:
          SqlSelect select = (SqlSelect) call;
          //walk only from
          select.setFrom(select.getFrom().accept(this));
          return select;
        case AS:
          //is a table, do the expansion

          //Need to do some alias stuff so check this here
          if (call.getOperandList().get(0) instanceof SqlIdentifier &&
              ((SqlIdentifier) call.getOperandList().get(0)).names.size() > 1
          ) {
            return expandTable((SqlIdentifier) call.getOperandList().get(0),
                ((SqlIdentifier) call.getOperandList().get(1)).names.get(0));
          }
      }

      return super.visitScoped(call);
    }

    int idx = 0;

    private SqlNode expandTable(SqlIdentifier id, String finalAlias) {
      List<String> suffix;

      SqlIdentifier first;
      //Abso
      if (validator.getNamespace(id).getTable().unwrap(AbsoluteSQRLTable.class) != null) {
        suffix = id.names.subList(1, id.names.size());
        first = new SqlIdentifier(List.of(
            id.names.get(0)
        ), SqlParserPos.ZERO);
      } else if (validator.getNamespace(id).getTable().unwrap(NestedSQRLTable.class)!=null){

        first = new SqlIdentifier(List.of( id.names.get(0),
            id.names.get(1)
        ), SqlParserPos.ZERO);
        if ( id.names.size() > 2) {
          suffix =  id.names.subList(2, id.names.size());
        } else {
          suffix = List.of();
        }

      } else {
        throw new RuntimeException("");
      }

      SqlNode n =
          SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
              first,
              new SqlIdentifier(suffix.size() > 0 ? "_b" + (++idx) : finalAlias,
                  SqlParserPos.ZERO));
      if (suffix.size() >= 1) {
        String currentAlias = "_b" + (idx);

        for (int i = 0; i < suffix.size(); i++) {
          String nextAlias = i == suffix.size() - 1 ? finalAlias : "_b" + (++idx);
          n = new SqlJoin(
              SqlParserPos.ZERO,
              n,
              SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
              JoinType.DEFAULT.symbol(SqlParserPos.ZERO),
              SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
                  new SqlIdentifier(List.of(currentAlias, suffix.get(i)), SqlParserPos.ZERO),
                  new SqlIdentifier(nextAlias, SqlParserPos.ZERO)),
              JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
              null
          );
          currentAlias = nextAlias;
        }
        System.out.println();

        //

        return n;
      }

      return n;
    }
  }
}
