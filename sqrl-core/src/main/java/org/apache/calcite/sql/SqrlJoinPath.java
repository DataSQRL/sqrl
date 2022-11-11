package org.apache.calcite.sql;

import ai.datasqrl.graphql.server.Model.SchemaVisitor;
import com.google.common.base.Preconditions;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.jetbrains.annotations.NotNull;

@Getter
public class SqrlJoinPath extends SqrlJoinTerm {

  public final List<SqlNode> relations;
  public final List<SqlNode> conditions;

  public SqrlJoinPath(SqlParserPos pos, List<SqlNode> relations,
      List<SqlNode> conditions) {
    super(pos);
    Preconditions.checkState(relations.size() == conditions.size());
    this.relations = relations;
    this.conditions = conditions;
  }

  @NotNull
  @Override
  public SqlOperator getOperator() {
    return SqrlJoinDeclarationSpec.OPERATOR;
  }

  @NotNull
  @Override
  public List<SqlNode> getOperandList() {
    return List.of();
  }

  public <R, C> R accept(SqrlJoinTermVisitor<R, C> visitor, C context) {
    return visitor.visitJoinPath(this, context);
  }
}
