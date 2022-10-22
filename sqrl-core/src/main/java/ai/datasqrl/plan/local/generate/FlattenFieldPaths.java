package ai.datasqrl.plan.local.generate;

import ai.datasqrl.plan.local.generate.AnalyzeStatement.ResolvedTableField;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Value;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.util.Litmus;

/**
 * Adds left joins for field paths
 */
public class FlattenFieldPaths extends SqlShuttle {

  private final AnalyzeStatement analyzeStatement;
  List<ToLeftJoin> left = new ArrayList<>();

  public FlattenFieldPaths(AnalyzeStatement analyzeStatement) {

    this.analyzeStatement = analyzeStatement;
  }

  public SqlNode accept(SqlNode node) {
    switch (node.getKind()) {
      case JOIN:
        SqlJoin join = (SqlJoin) node;
        join.setLeft(join.getLeft().accept(this));
        join.setRight(join.getRight().accept(this));
        break;
      case SELECT:
        SqlSelect select = (SqlSelect) node;

        List<SqlNode> expandedSelect = analyzeStatement.expandedSelect.get(select);
        SqlNodeList sel = (SqlNodeList) new SqlNodeList(expandedSelect,
            SqlParserPos.ZERO).accept(this);
        SqlNode where = select.getWhere() != null ? select.getWhere().accept(this) : null;
        SqlNodeList ord =
            select.getOrderList() != null ? (SqlNodeList) select.getOrderList().accept(this) : null;

        select.setSelectList(sel);
        select.setWhere(where);
        select.setOrderBy(ord);

        ReplaceGroupIdentifiers rep = new ReplaceGroupIdentifiers();
        SqlNodeList group =
            select.getGroup() != null ? (SqlNodeList) select.getGroup().accept(rep) : null;
        select.setGroupBy(group);

        //TODO: check to see if we've modified any select items so we can update the group or order aliases

        SqlNode from = select.getFrom();
        //add as left joins once extracted
        for (ToLeftJoin toJoin : left) {
          from = new SqlJoin(
              SqlParserPos.ZERO,
              from,
              SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
              JoinType.LEFT.symbol(SqlParserPos.ZERO),
              SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
                  new SqlIdentifier(toJoin.names, SqlParserPos.ZERO),
                  new SqlIdentifier(toJoin.alias, SqlParserPos.ZERO)),
              JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
              null
          );
        }
        select.setFrom(from);

        return select;
    }

    return node;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    switch (call.getKind()) {
      case SELECT:
      case UNION:
      case INTERSECT:
      case EXCEPT:
        FlattenFieldPaths flattenFieldPaths = new FlattenFieldPaths(this.analyzeStatement);
        return flattenFieldPaths.accept(call);
      case AS:
        return SqlStdOperatorTable.AS.createCall(call.getParserPosition(),
            call.getOperandList().get(0).accept(this),
            call.getOperandList().get(1)
        );
    }

    return super.visit(call);
  }

  AtomicInteger i = new AtomicInteger();

  public String createLeftJoin(List<String> names, SqlIdentifier oldIdentifier) {
    //Dedupe joins
    for (ToLeftJoin j : this.left) {
      if (j.names.equals(names)) {
        return j.alias;
      }
    }

    String alias = "__a" + i.incrementAndGet();
    this.left.add(new ToLeftJoin(names, alias, oldIdentifier));
    return alias;
  }


  @Value
  class ToLeftJoin {

    List<String> names;
    String alias;
    SqlIdentifier oldIdentifier;
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    ResolvedTableField tableField = analyzeStatement.getExpressions().get(id);
    //not all fields are qualified, such as COUNT(*)
    if (tableField == null) {
      return id;
    }
    if (tableField.getPath().size() > 1) {
      //add as left join, give it an alias
      //replace token with new one
      SqlIdentifier identifier = tableField.getAliasedIdentifier(id);

      String alias = createLeftJoin(identifier
          .names.subList(0, identifier.names.size() - 1), identifier);
      List<String> newName = List.of(alias,
          identifier.names.get(identifier.names.size() - 1));
      return new SqlIdentifier(newName, id.getParserPosition());
    }

    return super.visit(id);
  }


  /**
   * Now that we've changed the group identifier, rewrite them
   */
  private class ReplaceGroupIdentifiers extends SqlShuttle {

    private final ImmutableMap<SqlIdentifier,
        FlattenFieldPaths.ToLeftJoin> map;

    public ReplaceGroupIdentifiers() {
      this.map = Maps.uniqueIndex(left, i -> i.getOldIdentifier());
    }


    public SqlNode visit(SqlCall call) {
      switch (call.getKind()) {
        case AS:
          return SqlStdOperatorTable.AS.createCall(call.getParserPosition(),
              call.getOperandList().get(0).accept(this),
              call.getOperandList().get(1)
          );
      }

      return super.visit(call);
    }

    @Value
    class ToLeftJoin {

      List<String> names;
      String alias;
      SqlQualified oldIdentifier;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
//      SqlValidatorNamespace ns = getScope().fullyQualify(id).namespace;
//      ResolvedTableField tableField = analyzeStatement.getExpressions().get(id);
//      Preconditions.checkNotNull(tableField);
      for (FlattenFieldPaths.ToLeftJoin j : left) {
        if (j.getOldIdentifier()
            .equalsDeep(id, Litmus.IGNORE)) {
          List<String> newName = List.of(j.getAlias(),
              id.names.get(id.names.size() - 1));
          return new SqlIdentifier(newName, id.getParserPosition());
        }
      }

      return super.visit(id);
    }

  }
}
