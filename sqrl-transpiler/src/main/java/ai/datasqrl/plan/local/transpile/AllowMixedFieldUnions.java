package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.config.error.ErrorCode;
import ai.datasqrl.parse.SqrlAstException;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

@AllArgsConstructor
public class AllowMixedFieldUnions extends SqlShuttle {

  Analysis analysis;

  @Override
  public SqlNode visit(SqlCall node) {
    switch (node.getKind()) {
      case UNION:
        SqlBasicCall call = (SqlBasicCall) node;
        SqlNode[] operands = allowMixedFieldUnions(call.getOperandList());

        return new SqlBasicCall(call.getOperator(), operands, call.getParserPosition());
    }

    return super.visit(node);
  }

  private SqlNode[] allowMixedFieldUnions(List<SqlNode> operandList) {
    //walk all unions in operand list (could be nested), collect all names
    //walk again, update names
    Set<String> names = new LinkedHashSet<>();
    for (SqlNode node : operandList) {
      collectUnionNames(node, names);
    }

    List<SqlNode> newOperands = new ArrayList<>();
    for (SqlNode node : operandList) {
      newOperands.add(rearrangeProject(node, names));
    }

    return newOperands.toArray(new SqlNode[0]);
  }

  private SqlNode rearrangeProject(SqlNode node, Set<String> names) {
    return node.accept(new RearrangeProjectsToMatchNames(names));
  }

  private void collectUnionNames(SqlNode node, Set<String> names) {
    switch (node.getKind()) {
      case UNION:
        collectUnionNames(node, names);
        return;
      case SELECT:
        SqlSelect select = (SqlSelect) node;
        AtomicInteger i = new AtomicInteger(0);
        select.getSelectList().getList().stream()
            .map(s -> SqlValidatorUtil.getAlias(s, i.incrementAndGet()))
            .forEach(names::add);
        return;
    }
    throw new SqrlAstException(ErrorCode.GENERIC_ERROR, node.getParserPosition(),
        "unknown nested union field");
  }

  @AllArgsConstructor
  public class RearrangeProjectsToMatchNames extends SqlShuttle {

    Set<String> names;

    @Override
    public SqlNode visit(SqlCall call) {
      switch (call.getKind()) {
        case UNION:
          return super.visit(call);
        case SELECT:
          SqlSelect select = (SqlSelect) call;
          //create new select w/ aligned projects
          return new SqlSelect(select.getParserPosition(),
              (SqlNodeList) select.getOperandList().get(0),
              alignSelects(select.getSelectList(), names),
              select.getFrom(),
              select.getWhere(),
              select.getGroup(),
              select.getHaving(),
              select.getWindowList(),
              select.getOrderList(),
              select.getOffset(),
              select.getFetch(),
              select.getHints()
          );
      }

      throw new SqrlAstException(ErrorCode.GENERIC_ERROR, call.getParserPosition(),
          "Unknown node type");
    }

    private SqlNodeList alignSelects(SqlNodeList selectList, Set<String> names) {
      List<SqlNode> newList = new ArrayList<>();
      AtomicInteger idx = new AtomicInteger(0);
      Map<String, SqlNode> nodes = Maps.uniqueIndex(selectList.getList(), s ->
          SqlValidatorUtil.getAlias(s, idx.incrementAndGet()));
      for (String name : names) {
        SqlNode node = nodes.get(name);
        if (node == null) {
          SqlNode call = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
              SqlLiteral.createNull(selectList.getParserPosition()),
              new SqlIdentifier(name, SqlParserPos.ZERO));
          newList.add(call);
        } else {
          newList.add(node);
        }
      }

      return new SqlNodeList(newList, selectList.getParserPosition());
    }
  }
}
