//package ai.datasqrl.schema.factory;
//
//import ai.datasqrl.parse.tree.Expression;
//import ai.datasqrl.parse.tree.Identifier;
//import ai.datasqrl.parse.tree.Query;
//import ai.datasqrl.parse.tree.QuerySpecification;
//import ai.datasqrl.parse.tree.SelectItem;
//import ai.datasqrl.parse.tree.SingleColumn;
//import ai.datasqrl.parse.tree.name.Name;
//import ai.datasqrl.schema.Column;
//import ai.datasqrl.schema.Field;
//import ai.datasqrl.schema.Table;
//import ai.datasqrl.validate.PrimaryKeyDeriver;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Optional;
//
//public class SubqueryTableFactory {
//
//  /**
//   * Creates a table based on a subquery and derives its ppk and pk
//   */
//  public Table create(Query query) {
//    List<Column> columns = new ArrayList<>();
//    for (SelectItem selectItem : ((QuerySpecification) query.getQueryBody())
//        .getSelect().getSelectItems()) {
//      SingleColumn col = (SingleColumn) selectItem;
//      Column column = getOrCreateColumn(col.getExpression(), col.getAlias());
//      columns.add(column);
//    }
//    Table table = new Table(-1, Name.system("subquery"), null, true);
//    for (Column column : columns) {
//      table.addField(column);
//    }
//
//    PrimaryKeyDeriver primaryKeyDeriver = new PrimaryKeyDeriver();
//    List<Integer> pos = primaryKeyDeriver.derive(query);
//
//    for (Integer index : pos) {
//      Column column = ((Column) table.getFields().get(index));
//      column.setPrimaryKey(true);
//    }
//
//    return table;
//  }
//
//  /**
//   * Creates a column while keeping track of column source
//   */
//  private Column getOrCreateColumn(Expression expression,
//      Optional<Identifier> alias) {
//    if (expression instanceof Identifier) {
//      Identifier i = (Identifier) expression;
//      Column column = new Column(
//          alias.map(Identifier::getNamePath).orElseGet(i::getNamePath).getLast(),
//          null, 0, 0, List.of(),
//          false, false, false);
//      column.setSource(i.getResolved());
//
//      return column;
//    }
//
//    Column column = new Column(alias.map(Identifier::getNamePath)
//        .orElseGet(() -> Name.system("unnamedColumn").toNamePath()).getLast(),
//        null, 0, 0, List.of(),
//        false, false, false);
//
//    return column;
//  }
//}
