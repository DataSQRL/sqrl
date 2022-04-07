package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryBody;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SingleColumn;
import java.util.List;
import java.util.stream.Collectors;

public class SqrlNodeUtil {

  public static boolean hasOneUnnamedColumn(Query query) {
    QueryBody body = query.getQueryBody();
    if (body instanceof QuerySpecification) {
      Select select = ((QuerySpecification)body).getSelect();

      return select.getSelectItems().size() == 1 && select.getSelectItems().get(0) instanceof SingleColumn
          && ((SingleColumn) select.getSelectItems().get(0)).getAlias().isEmpty();
    }
    throw new RuntimeException("not yet implemented");
  }

  public static List<SingleColumn> getSelectList(Query query) {
    QueryBody body = query.getQueryBody();
    if (body instanceof QuerySpecification) {
      Select select = ((QuerySpecification)body).getSelect();
      return select.getSelectItems().stream()
          .map(c->(SingleColumn) c)
          .collect(Collectors.toList());
    }
    throw new RuntimeException("not yet implemented");

  }
}
