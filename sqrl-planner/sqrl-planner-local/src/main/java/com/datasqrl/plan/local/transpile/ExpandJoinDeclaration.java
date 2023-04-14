/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.transpile;

import static com.datasqrl.plan.local.transpile.ConvertJoinDeclaration.convertToBushyTree;

import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.plan.calcite.util.SqlNodeUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.SqrlJoinPath;
import org.apache.calcite.sql.UnboundJoin;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

@AllArgsConstructor
public class ExpandJoinDeclaration {

  String firstAlias;
  String lastAlias;
  AtomicInteger aliasCnt;

  public UnboundJoin expand(SqrlJoinDeclarationSpec node) {
    SqrlJoinPath from = (SqrlJoinPath) node.getRelation();
//    Preconditions.checkState(node.fetch.isEmpty(), "Limit on join declaration tbd");

    return expand(convertToBushyTree(from.getRelations(), from.getConditions()));
  }

  public UnboundJoin expand(SqlNode node) {
    //1. Extraction conditions Since we don't know where exactly on the node
    //tree it will belong and we don't want to do decomposition at this time
    ExtractConditions extractConditions = new ExtractConditions();
    node = node.accept(extractConditions);

    List<SqlNode> conditions = extractConditions.conditions;

    //2. Extract table alises
    ExtractTableAlias tableAlias = new ExtractTableAlias();
    node.accept(tableAlias);
    Map<SqlNode, String> aliasMap = tableAlias.tableAliasMap;
    Map<String, SqlNode> aliasMapInverse = tableAlias.inverse;

    //3. Remove left deep table so we can inline it
    RemoveLeftDeep removeLeftDeep = new RemoveLeftDeep();
    node = node.accept(removeLeftDeep);

    ExtractRightDeepAlias rightDeepAlias = new ExtractRightDeepAlias();
    String rightAlias = node.accept(rightDeepAlias);
    //4. Realias
    Map<String, String> newAliasMap = new HashMap<>();
    for (Map.Entry<String, SqlNode> aliases : aliasMapInverse.entrySet()) {
      if (aliases.getKey().equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical())) {
        newAliasMap.put(ReservedName.SELF_IDENTIFIER.getCanonical(), firstAlias);
        aliasMap.put(aliases.getValue(), firstAlias);
      } else if (aliases.getKey().equalsIgnoreCase(rightAlias)) {
        newAliasMap.put(aliases.getKey(), lastAlias);
        aliasMap.put(aliases.getValue(), lastAlias);
      } else {
        SqlNode existingRight = aliasMapInverse.get(aliases.getKey());
        //todo assure unique
        String newAlias = "_x" + aliases.getKey() + ReservedName.SELF_IDENTIFIER.getCanonical()
            + (aliasCnt.incrementAndGet());
        aliasMap.put(existingRight, newAlias);
        newAliasMap.put(aliases.getKey(), newAlias);
      }
    }

    //Replace all aliases
    ReplaceTableAlias replaceTableAlias = new ReplaceTableAlias(aliasMap);
    node = node.accept(replaceTableAlias);

    ReplaceIdentifierAliases replaceIdentifierAliases = new ReplaceIdentifierAliases(newAliasMap);
    node = node.accept(replaceIdentifierAliases);
    List<SqlNode> newConditions = conditions.stream()
        .map(c -> c.accept(replaceIdentifierAliases))
        .collect(Collectors.toList());

    return new UnboundJoin(SqlParserPos.ZERO, node,
        Optional.ofNullable(SqlNodeUtil.and(newConditions)));
  }


  public static class ExtractRightDeepAlias extends SqlBasicVisitor<String> {

    @Override
    public String visit(SqlCall call) {
      switch (call.getKind()) {
        case AS:
          return call.getOperandList().get(1).accept(this);
        case JOIN:
          SqlJoin join = (SqlJoin) call;
          return join.getRight().accept(this);

      }
      return super.visit(call);
    }

    @Override
    public String visit(SqlIdentifier id) {
      return id.names.get(0);
    }
  }


  public class ExtractConditions extends SqlShuttle {

    List<SqlNode> conditions = new ArrayList<>();

    @Override
    public SqlNode visit(SqlCall call) {
      switch (call.getKind()) {
        case JOIN:
          SqlJoin join = (SqlJoin) call;
          this.conditions.add(join.getCondition());
          return new SqlJoin(join.getParserPosition(),
              join.getLeft(),
              join.isNaturalNode(),
              join.getJoinTypeNode(),
              join.getRight(),
              JoinConditionType.NONE.symbol(join.getParserPosition()),
              null);
        case SELECT:
          return call;
      }

      return super.visit(call);
    }
  }

  private class ExtractTableAlias extends SqlBasicVisitor {

    Map<SqlNode, String> tableAliasMap = new HashMap<>();
    Map<String, SqlNode> inverse = new HashMap<>();

    @Override
    public Object visit(SqlCall call) {
      if (call.getKind() == SqlKind.AS) {
        tableAliasMap.put(call.getOperandList().get(0),
            ((SqlIdentifier) call.getOperandList().get(1)).names.get(0)
        );
        inverse.put(((SqlIdentifier) call.getOperandList().get(1)).names.get(0),
            call.getOperandList().get(0)
        );
      }

      return super.visit(call);
    }
  }

  private class RemoveLeftDeep extends SqlShuttle {

    @Override
    public SqlNode visit(SqlCall call) {
      switch (call.getKind()) {
        case JOIN:
          SqlJoin join = (SqlJoin) call;
          Optional<SqlNode> left = removeLeftDeep(join.getLeft());
          if (left.isEmpty()) {
            return join.getRight();
          }

          return new SqlJoin(join.getParserPosition(),
              left.get(),
              join.isNaturalNode(),
              join.getJoinTypeNode(),
              join.getRight(),
              join.getConditionTypeNode(),
              join.getCondition());
      }

      return super.visit(call);
    }

    private Optional<SqlNode> removeLeftDeep(SqlNode node) {
      if (node instanceof SqlIdentifier ||
          (node instanceof SqlCall && ((SqlCall) node).getOperandList()
              .get(0) instanceof SqlIdentifier)) {
        return Optional.empty();
      }

      return Optional.of(node.accept(this));
    }
  }

  private class ReplaceTableAlias extends SqlShuttle {

    private final Map<SqlNode, String> aliasMap;

    public ReplaceTableAlias(Map<SqlNode, String> aliasMap) {
      this.aliasMap = aliasMap;
    }

    @Override
    public SqlNode visit(SqlCall call) {
      switch (call.getKind()) {
        case AS:
          Preconditions.checkNotNull(aliasMap.get(call.getOperandList().get(0)));
          return SqlStdOperatorTable.AS
              .createCall(SqlParserPos.ZERO,
                  call.getOperandList().get(0),
                  new SqlIdentifier(aliasMap.get(call.getOperandList().get(0)), SqlParserPos.ZERO)
              );

      }
      return super.visit(call);
    }
  }

  private class ReplaceIdentifierAliases extends SqlShuttle {

    private final Map<String, String> newAliasMap;

    public ReplaceIdentifierAliases(Map<String, String> newAliasMap) {
      this.newAliasMap = newAliasMap;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      if (id.names.size() > 1) {
        Preconditions.checkState(newAliasMap.containsKey(id.names.get(0)));
        return new SqlIdentifier(List.of(
            newAliasMap.get(id.names.get(0)), id.names.get(1)
        ), id.getParserPosition());

      }
      return super.visit(id);
    }
  }
}
