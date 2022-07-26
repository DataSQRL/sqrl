package org.apache.calcite.sql;


import java.util.Optional;

public interface JoinDeclaration {
    Optional<SqlNode> getPullupCondition();
    SqlNode getJoinTree();
    String getFirstAlias();
    String getLastAlias();

    JoinDeclaration rewriteSqlNode(String newSelfAlias, Optional<String> endAlias, UniqueAliasGenerator aliasGenerator);
  }