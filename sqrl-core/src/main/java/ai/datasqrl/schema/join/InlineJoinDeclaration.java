package ai.datasqrl.schema.join;

import lombok.Value;
import org.apache.calcite.sql.SqlNode;

@Value
public class InlineJoinDeclaration implements JoinDeclaration {

  SqlNode query;
  public <R, C> R accept(JoinDeclarationVisitor<R, C> visitor, C context) {
    return visitor.visitInline(this, context);
  }
}
