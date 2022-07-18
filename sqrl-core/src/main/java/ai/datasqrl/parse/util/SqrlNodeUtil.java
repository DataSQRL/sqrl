package ai.datasqrl.parse.util;

import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QueryBody;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SingleColumn;

public class SqrlNodeUtil {

  /**
   * Unnamed columns are treated as expressions. It must not be an identifier
   * todo: must be aggregating
   * todo: cannot be root scope
   */
  public static boolean isExpression(Query query) {
    QueryBody body = query.getQueryBody();
    if (body instanceof QuerySpecification) {
      Select select = ((QuerySpecification) body).getSelect();
      if (select.getSelectItems().size() != 1) {
        return false;
      }
      //SELECT *
      if (!(select.getSelectItems().get(0) instanceof SingleColumn)) {
        return false;
      }
      SingleColumn column = (SingleColumn) select.getSelectItems().get(0);
      if (column.getAlias().isPresent()) {
        return false;
      }
      return !(column.getExpression() instanceof Identifier);
    }
    throw new RuntimeException("not yet implemented");
  }
}
