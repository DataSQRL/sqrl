package ai.datasqrl.plan.local.generate.node;

import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.local.generate.node.util.SqlNodeUtil;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

@Getter
public class SqlJoinDeclaration {
  SqlNode rel;
  SqlNode condition;
  String toTableAlias;
  private final Set<String> aliases = new HashSet<>();
  public static int aliasIncrementer = 0;

  public SqlJoinDeclaration(SqlNode rel, SqlNode condition) {
    this.rel = rel;
    this.condition = condition;
    analyze(rel);
  }

  private SqlJoinDeclaration(SqlNode rel, SqlNode condition, String toTableAlias) {
    this.rel = rel;
    this.condition = condition;
    this.toTableAlias = toTableAlias;
  }

  public SqlNode getRel() {
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
    this.toTableAlias = ((SqlIdentifier)((SqlBasicCall)rel).getOperandList().get(1)).names.get(0);
  }

  //Rewrites all self aliases w/ incoming alias
  public SqlJoinDeclaration rewriteSelfAlias(String selfAlias) {
    //1. Create a new alias map
    //2. Rewrite with new alias map.
    Map<String, String> aliasMap = new HashMap<>();
    for (String alias : aliases) {
      if (alias.equalsIgnoreCase("_")) {
        continue;
      }
      aliasMap.put(alias, alias);//todo: fix
    }
    aliasMap.put("_", selfAlias);


    SqlNode newRel = getRel().accept(new RewriteIdentifierSqlShuttle(aliasMap));
    SqlNode newCondition = condition.accept(new RewriteIdentifierSqlShuttle(aliasMap));

    return new SqlJoinDeclaration(newRel, newCondition, aliasMap.get(toTableAlias));
  }

  static class RewriteIdentifierSqlShuttle extends SqlShuttle {

    private final Map<String, String> aliasMap;

    public RewriteIdentifierSqlShuttle(Map<String, String> aliasMap) {

      this.aliasMap = aliasMap;
    }
    @Override
    public SqlNode visit(SqlIdentifier id) {
//      if (id instanceof SqlAliasIdentifier) {
//        List<String> names = new ArrayList<>(id.names);
//        String newAlias = aliasMap.get(names.get(0));
//        Preconditions.checkNotNull(newAlias, "Could not find alias: {}", newAlias);
//        names.set(0, newAlias);
//        return new SqlIdentifier(names, id.getParserPosition());
//      }
      return id.clone(SqlParserPos.ZERO);
    }

    //todo: our own sqlNodes?
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
        ", condition=" + condition +
        ", toTableAlias='" + toTableAlias + '\'' +
        ", aliases=" + aliases +
        '}';
  }
}