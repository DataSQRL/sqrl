package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.tree.Limit;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedNamePath;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedTable;
import ai.datasqrl.schema.*;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.Relationship.Multiplicity;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Triple;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@AllArgsConstructor
public class SchemaBuilder extends Schema {

  public List<Table> addImportTable(ImportManager.SourceTableImport importSource, Optional<Name> nameAlias) {
    List<Table> resolvedImport = tableFactory.importTables(importSource, nameAlias);
    add(resolvedImport.get(0));
    return resolvedImport;
  }

  public Table createDistinctTable(ResolvedTable resolvedTable, List<ResolvedNamePath> pks) {
    Table table = new Table(resolvedTable.getNamePath());
    resolvedTable.getToTable().getVisibleColumns().forEach(
        c -> table.addColumn(c.getName(), c.isVisible()));
    return table;
  }

  public Relationship addJoinDeclaration(NamePath namePath, Table target, Optional<Limit> limit) {
    Table parentTable = walkTable(namePath.popLast());

    //determine if
    Multiplicity multiplicity = limit.map(
        l -> l.getIntValue().filter(i -> i == 1).map(i -> Multiplicity.ONE)
            .orElse(Multiplicity.MANY)).orElse(Multiplicity.MANY);

    Relationship relationship = parentTable.addRelationship(namePath.getLast(), target,
        JoinType.JOIN, multiplicity);
    return relationship;
  }

  public Column addExpression(NamePath namePath) {
    Table table = walkTable(namePath.popLast());
    Name columnName = namePath.getLast();
    Column column = table.addColumn(columnName, true);
    return column;
  }

  public Column addQueryExpression(NamePath namePath) {
    //do not create table, add column
    Table table = walkTable(namePath.popLast());
    Name columnName = namePath.getLast();

    Column column = table.addColumn(columnName, true);
    return column;
  }

  public Triple<Optional<Relationship>, Table, List<Field>> addQuery(NamePath namePath, List<Name> fieldNames) {

    List<Field> fields = new ArrayList<>();
    Table table = new Table(namePath);

    //todo: column names:
    fieldNames.forEach(n -> {
      table.addColumn(n,  true);
    });

    if (namePath.getLength() == 1) {
      add(table);
    } else {
      Table parentTable = walkTable(namePath.popLast());
      Name relationshipName = namePath.getLast();
      Relationship.Multiplicity multiplicity = Multiplicity.MANY;
//        if (specNorm.getLimit().flatMap(Limit::getIntValue).orElse(2) == 1) {
//          multiplicity = Multiplicity.ONE;
//        }

      getTableFactory().createParentRelationship(table, parentTable);

      Relationship childRel = getTableFactory()
          .createChildRelationship(relationshipName, table, parentTable, multiplicity);
      return Triple.of(Optional.of(childRel), table, fields);
    }
    return Triple.of(Optional.empty(), table, fields);
  }
}
