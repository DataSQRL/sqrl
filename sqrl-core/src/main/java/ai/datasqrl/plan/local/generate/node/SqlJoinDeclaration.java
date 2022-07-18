package ai.datasqrl.plan.local.generate.node;

import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.local.generate.node.util.SqlNodeUtil;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Holds a join declaration which can contain one or more joins or subqueries and an optional trailing condition.
 *
 * The join declaration can be rebased with a new set of aliases. This can be used for attaching
 * subqueries or using join declarations.
 *
 * Join declarations are also ego-centric, the final item in the join path is the table
 * it maps to.
 *
 */
@Getter
public class SqlJoinDeclaration {
  SqlNode rel;
  Optional<SqlNode> trailingCondition;

  private final Set<String> aliases = new HashSet<>();
  public static int aliasIncrementer = 0;

  public SqlJoinDeclaration(SqlNode rel, Optional<SqlNode> trailingCondition) {
    this.rel = rel;
    this.trailingCondition = trailingCondition;
    analyze(rel);
  }

  public SqlNode getRel() {
    //We need to duplicate some tokens so they stay unmodified during calcite validation
    return rel.accept(new SqlShuttle(){
      @Override
      public SqlNode visit(SqlIdentifier id) {
        return id.clone(SqlParserPos.ZERO);
      }

      @Override
      public SqlNode visit(SqlCall call) {
        return call.clone(SqlParserPos.ZERO);
      }
    });
  }

  private void analyze(SqlNode rel) {
    rel.accept(new SqlShuttle(){
      @Override
      public SqlNode visit(SqlCall call) {
        if (call.getOperator() == SqrlOperatorTable.AS) {
          String alias = ((SqlIdentifier)call.getOperandList().get(1)).names.get(0);
          aliases.add(alias);
        }

        return super.visit(call);
      }
    });

    //Ego centric traversal, we need to know the last alias to join later
//    if (rel instanceof SqlBasicCall) {
//      this.toTableAlias = ((SqlIdentifier) ((SqlBasicCall) rel).getOperandList().get(1)).names.get(
//          0);
//    } else {
//      this.toTableAlias = "_";
//    }
  }

  public SqlJoinDeclaration rewriteSelfAlias(String selfAlias, Optional<String> targetAlias) {
    Map<String, String> aliasMap = new HashMap<>();
    for (String alias : aliases) {
      aliasMap.put(alias, alias);//todo: fix
    }
    aliasMap.put("_", selfAlias);
    targetAlias.ifPresent(s -> aliasMap.put(getToTableAlias(), s));

    SqlNode newRel = getRel().accept(new RewriteIdentifierSqlShuttle(aliasMap));
    Optional<SqlNode> newCondition = trailingCondition.map(con->con.accept(new RewriteIdentifierSqlShuttle(aliasMap)));

    return new SqlJoinDeclaration(newRel, newCondition);
  }

  public String getToTableAlias() {
    SqlNode node = this.getRel();
    if (node instanceof SqlJoin) {
      SqlJoin join = (SqlJoin) node;
      SqlBasicCall call = ((SqlBasicCall)join.getRight());
      SqlIdentifier identifier = (SqlIdentifier)call.getOperandList().get(1);
      return identifier.names.get(0);
    } else if (node instanceof SqlBasicCall) {
      SqlBasicCall call = ((SqlBasicCall)node);

      SqlIdentifier identifier = (SqlIdentifier)call.getOperandList().get(1);
      return identifier.names.get(0);
    }

    throw new RuntimeException("Unknown type: "+ node);
  }

  public SqlJoinDeclaration rewriteTargetAlias(String a) {
    Map<String, String> aliasMap = new HashMap<>();
    for (String alias : aliases) {
      aliasMap.put(alias, alias);//todo: fix
    }
    aliasMap.put(getToTableAlias(), a);


    SqlNode newRel = getRel().accept(new RewriteIdentifierSqlShuttle(aliasMap));
    Optional<SqlNode> newCondition = trailingCondition.map(con->con.accept(new RewriteIdentifierSqlShuttle(aliasMap)));

    return new SqlJoinDeclaration(newRel, newCondition);
  }

  static class RewriteIdentifierSqlShuttle extends SqlShuttle {

    private final Map<String, String> aliasMap;

    public RewriteIdentifierSqlShuttle(Map<String, String> aliasMap) {

      this.aliasMap = aliasMap;
    }
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

  @Override
  public String toString() {
    return "SqlJoinDeclaration{" +
        "rel=" + SqlNodeUtil.printJoin(rel) +
        ", condition=" + trailingCondition +
        ", aliases=" + aliases +
        '}';
  }
}