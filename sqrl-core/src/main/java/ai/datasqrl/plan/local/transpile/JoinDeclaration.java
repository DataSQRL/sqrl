package ai.datasqrl.plan.local.transpile;


import ai.datasqrl.schema.Relationship;
import java.util.Optional;
import org.apache.calcite.sql.SqlNode;

public interface JoinDeclaration {
    Optional<SqlNode> getPullupCondition();
    SqlNode getJoinTree();
    String getFirstAlias();
    String getLastAlias();
    Optional<Relationship> getRelationship();

    JoinDeclaration rewriteSqlNode(String newSelfAlias, Optional<String> endAlias, UniqueAliasGenerator aliasGenerator);
  }