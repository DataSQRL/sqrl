package ai.datasqrl.plan.local.generate;

import ai.datasqrl.plan.calcite.util.SqlNodeUtil;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.AbsoluteResolvedTable;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.RelativeResolvedTable;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.Resolved;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.SingleTable;
import ai.datasqrl.schema.Relationship;
import com.google.common.base.Preconditions;
import com.ibm.icu.impl.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.stream.Collectors;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;

public class ReplaceWithVirtualTable extends SqlShuttle {

  private final AnalyzeStatement analysis;

  Stack<SqlNode> pullup = new Stack<>();

  public ReplaceWithVirtualTable(AnalyzeStatement analysis) {

    this.analysis = analysis;
  }

  public SqlNode accept(SqlNode node) {

    SqlNode result = node.accept(this);
    Preconditions.checkState(pullup.isEmpty());
    return result;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    switch (call.getKind()) {
      case AS:
        SqlNode tbl = call.getOperandList().get(0);
        //aliased in inlined
        if (tbl instanceof SqlIdentifier) {
          Resolved resolved = analysis.getTableIdentifiers().get(tbl);
          if (resolved instanceof RelativeResolvedTable &&
              ((RelativeResolvedTable)resolved).getFields().get(0).getNode() != null){
            return tbl.accept(this);
          }
        }

        return super.visit(call);

      case SELECT:
        SqlSelect select = (SqlSelect) super.visit(call);
        while (!pullup.isEmpty()) {
          SqlNode condition = pullup.pop();

          appendToSelect(select, condition);
        }
        return select;
      case JOIN:
        //Any right joins that are bushy don't need to pull up identifiers.

        SqlJoin join = (SqlJoin) super.visit(call);
        //if right is a bushy tree, we don't need to add the conditions (done in flattentablepaths)
//        if (join.getRight() instanceof SqlJoin) {
//          pullup.clear();
//        }

//        while (!pullup.isEmpty()) {
        if (!pullup.isEmpty()) {
          SqlNode condition = pullup.pop();
          FlattenTablePaths.addJoinCondition(join, condition);
        }

        return join;
    }

    return super.visit(call);
  }

  private void appendToSelect(SqlSelect select, SqlNode condition) {
    SqlNode where = select.getWhere();
    if (where == null) {
      select.setWhere(condition);
    } else {
      select.setWhere(SqlNodeUtil.and(select.getWhere(), condition));
    }

  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (analysis.getTableIdentifiers().get(id) == null) {
      return super.visit(id);
    }

    Resolved resolved = analysis.getTableIdentifiers().get(id);
    if (resolved instanceof SingleTable) {
      SingleTable singleTable = (SingleTable) resolved;
      return new SqlIdentifier(singleTable.getToTable().getVt().getNameId(),
          id.getParserPosition());
    } else if (resolved instanceof AbsoluteResolvedTable) {
      throw new RuntimeException("unexpected type");
    } else if (resolved instanceof RelativeResolvedTable) {
      RelativeResolvedTable resolveRel = (RelativeResolvedTable) resolved;
      Preconditions.checkState(resolveRel.getFields().size() == 1);
      Relationship relationship = resolveRel.getFields().get(0);
      if (relationship.getNode() != null) {

        String alias = analysis.tableAlias.get(id);
        Pair<SqlNode, SqlNode> pair = expand(relationship.getNode(),
            resolveRel.getAlias(), alias
        );
        pullup.push(pair.second);
        return pair.first;
//        return expandJoinDeclaration(resolveRel, id, relationship, relationship.getNode());
      }
      //create join condition to pull up
//      FlattenTablePaths.createCondition()
      String alias = analysis.tableAlias.get(id);
      SqlNode condition = FlattenTablePaths.createCondition(alias, resolveRel.getAlias(),
          relationship.getFromTable(),
          relationship.getToTable()
      );
      pullup.push(condition);

      return new SqlIdentifier(relationship.getToTable().getVt().getNameId(),
          id.getParserPosition());
    }

    return super.visit(id);
  }

  int a = 0;

  private Pair<SqlNode, SqlNode> expand(SqlNode node, String firstAlias, String lastAlias) {
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
    //4. Realias the who
    Map<String, String> newAliasMap = new HashMap<>();
    for (Map.Entry<String, SqlNode> aliases : aliasMapInverse.entrySet()) {
      if (aliases.getKey().equalsIgnoreCase("_")) {
        newAliasMap.put("_", firstAlias);
        aliasMap.put(aliases.getValue(), firstAlias);
      } else if (aliases.getKey().equalsIgnoreCase(rightAlias)) {
        newAliasMap.put(aliases.getKey(), lastAlias);
        aliasMap.put(aliases.getValue(), lastAlias);
      } else {
        SqlNode existingRight = aliasMapInverse.get(rightAlias);
        //todo assure unique
        String newAlias = "_" + aliases.getKey() + "_" + (++a);
        aliasMap.put(existingRight, newAlias);
        newAliasMap.put(aliases.getKey(), newAlias);
      }
    }

    //Replace all aliases
    ReplaceTableAlias replaceTableAlias = new ReplaceTableAlias(aliasMap);
    node = node.accept(replaceTableAlias);

    ReplaceIdentifierAliases replaceIdentifierAliases = new ReplaceIdentifierAliases(newAliasMap);
    List<SqlNode> newConditions = conditions.stream()
        .map(c->c.accept(replaceIdentifierAliases))
        .collect(Collectors.toList());


    return Pair.of(node, SqlNodeUtil.and(newConditions));
  }

  public class ExtractRightDeepAlias extends SqlBasicVisitor<String> {

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
      }

      return super.visit(call);
    }
  }


  private SqlNode expandJoinDeclaration(RelativeResolvedTable resolveRel, SqlIdentifier id,
      Relationship relationship, SqlNode node) {
    //this could be made cleaner
    String alias = this.analysis.getTableAlias().get(id);
    if (node instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) node;

      //simple, return from condition
      if (select.getFetch() == null && (select.getHints() == null || select.getHints().getList()
          .isEmpty())) {
        //append condition
        SqlJoin join = (SqlJoin) select.getFrom();
        SqlIdentifier identifier = (SqlIdentifier) join.getRight();

        join.setRight(SqlStdOperatorTable.AS.createCall(
            SqlParserPos.ZERO,
            join.getRight(),
            new SqlIdentifier(alias, SqlParserPos.ZERO)
        ));

        String toReplace = identifier.names.get(0);
        join = (SqlJoin) join.accept(new SqlShuttle() {
          @Override
          public SqlNode visit(SqlIdentifier id) {
            if (id.names.size() == 2) {
              //todo: actually walk tree instead of just guessing
              if (id.names.get(0).equalsIgnoreCase(toReplace)) {
                List<String> names = new ArrayList<>(id.names);
                names.set(0, alias);
                return new SqlIdentifier(names, id.getParserPosition());
              }
            }
            return super.visit(id);
          }
        });

//        SqlCall call = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
//            new SqlIdentifier(List.of(alias,
//                SqlValidatorUtil.getAlias(select.getSelectList().get(i), i)
//            ), SqlParserPos.ZERO),
//            new SqlIdentifier(List.of(resolveRel.getAlias(), pkName), SqlParserPos.ZERO)
//        );
//        pullup.push(call);

        //also add a join

        return join;
      } else {
        return super.visit(id);
      }
//
//      //Is not simple
//      if (select.getFetch() != null) {
//        //todo: get name from first
//        List<SqlNode> conditions = new ArrayList<>();
//
//        for (int i = 0; i < relationship.getFromTable().getVt().getPrimaryKeyNames().size()
//            && i < relationship.getToTable().getVt().getPrimaryKeyNames().size(); i++) {
//
//          String pkName = relationship.getFromTable().getVt().getPrimaryKeyNames().get(i);
//
//          SqlCall call = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
//              new SqlIdentifier(List.of(alias,
//                  SqlValidatorUtil.getAlias(select.getSelectList().get(i), i)
//                  ), SqlParserPos.ZERO),
//              new SqlIdentifier(List.of(resolveRel.getAlias(), pkName), SqlParserPos.ZERO)
//          );
//          conditions.add(call);
//        }
//        SqlNode condition = SqlNodeUtil.and(conditions);
//
//        this.pullup.push(condition);
//        return node;
//      }

    }

    return node;
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
          (node instanceof SqlCall && ((SqlCall)node).getOperandList().get(0) instanceof SqlIdentifier )) {
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

  private class ReplaceIdentifierAliases extends SqlShuttle{

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
