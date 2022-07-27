package org.apache.calcite.sql;


import ai.datasqrl.schema.Relationship;
import java.util.Optional;

public interface JoinDeclaration {
    Optional<SqlNode> getPullupCondition();
    SqlNode getJoinTree();
    String getFirstAlias();
    String getLastAlias();
    Optional<Relationship> getRelationship();

    JoinDeclaration rewriteSqlNode(String newSelfAlias, Optional<String> endAlias, UniqueAliasGenerator aliasGenerator);
  }