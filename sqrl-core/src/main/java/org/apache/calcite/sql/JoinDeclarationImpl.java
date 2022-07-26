package org.apache.calcite.sql;

import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Value;
import org.apache.calcite.sql.util.SqlBasicVisitor;

@Value
public class JoinDeclarationImpl implements JoinDeclaration {
    Optional<SqlNode> pullupCondition;
    SqlNode joinTree;
    String firstAlias;
    String lastAlias;

    private final Set<String> aliases = new HashSet<>();

    public JoinDeclarationImpl(Optional<SqlNode> pullupCondition, SqlNode joinTree,
        String firstAlias,
        String lastAlias) {
      this.pullupCondition = pullupCondition;
      this.joinTree = joinTree;
      this.firstAlias = firstAlias;
      this.lastAlias = lastAlias;
      analyze();
    }

    private void analyze() {
      //extract all aliases with AS and adds them to a map
      joinTree.accept(new SqlBasicVisitor<>(){
        @Override
        public Object visit(SqlCall call) {
          if (call.getOperator() == SqrlOperatorTable.AS) {
            SqlIdentifier identifier = (SqlIdentifier) call.getOperandList().get(1);
            aliases.add(identifier.names.get(0));
          }
          return super.visit(call);
        }
      });

      aliases.add("_");

    }

    @Override
    public JoinDeclaration rewriteSqlNode(String newSelfAlias, Optional<String> endAlias, UniqueAliasGenerator aliasGenerator) {
      Map<String, String> aliasMap = new HashMap<>();
      String newLastAlias = null;
      for (String alias : aliases) {
        String newAlias;
        if (alias.equalsIgnoreCase("_")) {
          newAlias = newSelfAlias;
        } else if (alias.equalsIgnoreCase(this.lastAlias)) {
          newAlias = endAlias.orElseGet(() -> aliasGenerator.generate(alias));
          newLastAlias = newAlias;
        } else {
          newAlias = aliasGenerator.generate(alias);
        }
        aliasMap.put(alias, newAlias);
      }

      SqlNode newJoinTree = this.joinTree.accept(new RewriteIdentifierSqlShuttle(aliasMap));
      Optional<SqlNode> newCondition = this.pullupCondition.map(con->con.accept(new RewriteIdentifierSqlShuttle(aliasMap)));

      return new JoinDeclarationImpl(newCondition, newJoinTree, aliasMap.get(newSelfAlias), newLastAlias);
    }
  }