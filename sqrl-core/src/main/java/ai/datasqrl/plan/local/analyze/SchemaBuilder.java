package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.parse.tree.Limit;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.BundleTableFactory;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedNamePath;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedTable;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.ShadowingContainer;
import ai.datasqrl.schema.Table;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SchemaBuilder {

  Analysis analysis;

  public Table createDistinctTable(ResolvedTable resolvedTable, List<ResolvedNamePath> pks) {
    ShadowingContainer<Field> container = new ShadowingContainer<>();
    Table table = new Table(BundleTableFactory.tableIdCounter.incrementAndGet(),
        resolvedTable.getNamePath(), container);
    resolvedTable.getToTable().getVisibleColumns().forEach(
        c -> table.addColumn(c.getName(), isPrimaryKey(c, pks), c.isParentPrimaryKey(),
            c.isVisible()));

    return table;
  }

  private boolean isPrimaryKey(Column c, List<ResolvedNamePath> pks) {
    for (ResolvedNamePath namePath : pks) {
      if (namePath.getLast() == c) {
        return true;
      }
    }
    return false;
  }

  public Relationship addJoinDeclaration(NamePath namePath, Table target, Optional<Limit> limit) {
    Table parentTable = analysis.getSchema().walkTable(namePath.popLast());

    //determine if
    Multiplicity multiplicity = limit.map(
        l -> l.getIntValue().filter(i -> i == 1).map(i -> Multiplicity.ONE)
            .orElse(Multiplicity.MANY)).orElse(Multiplicity.MANY);

    Relationship relationship = new Relationship(namePath.getLast(), parentTable, target,
        JoinType.JOIN, multiplicity);

    parentTable.getFields().add(relationship);
    return relationship;
  }

  public Column addExpression(NamePath namePath) {
    Table table = analysis.getSchema().walkTable(namePath.popLast());
    Name columnName = namePath.getLast();
    int nextVersion = table.getNextColumnVersion(columnName);

    Column column = new Column(columnName, nextVersion, table.getNextColumnIndex(), false, false,
        true);

    table.addExpressionColumn(column);
    return column;
  }

  public Column addQueryExpression(NamePath namePath) {
    //do not create table, add column
    Table table = analysis.getSchema().walkTable(namePath.popLast());
    Name columnName = namePath.getLast();
    int nextVersion = table.getNextColumnVersion(columnName);

    Column column = new Column(columnName, nextVersion, table.getNextColumnIndex(), false, false,
        true);
    table.getFields().add(column);
    return column;
  }

  public Table addQuery(NamePath namePath, List<Name> fieldNames) {

    double derivedRowCount = 1; //TODO: derive from optimizer

    final BundleTableFactory.TableBuilder builder = analysis.getSchema().getTableFactory()
        .build(namePath);

    //todo: column names:
    fieldNames.forEach(n -> builder.addColumn(n, false, false, true));

    //Creates a table that is not bound to the schema TODO: determine timestamp
    Table table = builder.createTable();

    if (namePath.getLength() == 1) {
      analysis.getSchema().add(table);
    } else {
      Table parentTable = analysis.getSchema().walkTable(namePath.popLast());
      Name relationshipName = namePath.getLast();
      Relationship.Multiplicity multiplicity = Multiplicity.MANY;
//        if (specNorm.getLimit().flatMap(Limit::getIntValue).orElse(2) == 1) {
//          multiplicity = Multiplicity.ONE;
//        }

      Optional<Relationship> parentRel = analysis.getSchema().getTableFactory()
          .createParentRelationship(table, parentTable);
      parentRel.map(rel -> table.getFields().add(rel));

      Relationship childRel = analysis.getSchema().getTableFactory()
          .createChildRelationship(relationshipName, table, parentTable, multiplicity);
      parentTable.getFields().add(childRel);
    }
    return table;
  }
}
