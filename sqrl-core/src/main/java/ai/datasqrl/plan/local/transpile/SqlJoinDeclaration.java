package ai.datasqrl.plan.local.transpile;


import ai.datasqrl.schema.Relationship;
import java.util.Optional;
import org.apache.calcite.sql.SqlNode;

public interface SqlJoinDeclaration {

  Optional<SqlNode> getPullupCondition();

  SqlNode getJoinTree();

  String getFirstAlias();

  String getLastAlias();

  Optional<Relationship> getRelationship();

  SqlJoinDeclaration rewriteSqlNode(String newSelfAlias, Optional<String> endAlias,
      UniqueAliasGenerator aliasGenerator);
}