package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.parse.tree.Limit;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedTable;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.ScriptTable;
import ai.datasqrl.schema.builder.AbstractTableFactory;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Triple;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@AllArgsConstructor
public class VariableFactory {

  public ScriptTable createDistinctTable(ResolvedTable resolvedTable) {
    ScriptTable table = new ScriptTable(resolvedTable.getNamePath());
    resolvedTable.getToTable().getVisibleColumns().forEach(
        c -> table.addColumn(c.getName(), c.isVisible()));
    return table;
  }

  public Relationship addJoinDeclaration(NamePath namePath, ScriptTable parentTable, ScriptTable target, Optional<Limit> limit) {
    //determine if
    Multiplicity multiplicity = limit.map(
        l -> l.getIntValue().filter(i -> i == 1).map(i -> Multiplicity.ONE)
            .orElse(Multiplicity.MANY)).orElse(Multiplicity.MANY);

    Relationship relationship = parentTable.addRelationship(namePath.getLast(), target,
        JoinType.JOIN, multiplicity);
    return relationship;
  }

  public Column addExpression(NamePath namePath, ScriptTable table) {
    Name columnName = namePath.getLast();
    Column column = table.addColumn(columnName, true);
    return column;
  }

  public Column addQueryExpression(NamePath namePath, ScriptTable table) {
    //do not create table, add column
    Name columnName = namePath.getLast();

    Column column = table.addColumn(columnName, true);
    return column;
  }

  public Triple<Optional<Relationship>, ScriptTable, List<Field>> addQuery(NamePath namePath, List<Name> fieldNames, Optional<ScriptTable> parentTable) {

    List<Field> fields = new ArrayList<>();
    ScriptTable table = new ScriptTable(namePath);

    //todo: column names:
    fieldNames.forEach(n -> {
      table.addColumn(n,  true);
    });

    if (namePath.size() == 1) {
      return Triple.of(Optional.empty(), table, fields);
    } else {
      Name relationshipName = namePath.getLast();
      Relationship.Multiplicity multiplicity = Multiplicity.MANY;
//        if (specNorm.getLimit().flatMap(Limit::getIntValue).orElse(2) == 1) {
//          multiplicity = Multiplicity.ONE;
//        }

      AbstractTableFactory.createParentRelationship(table, parentTable.get());

      Relationship childRel = AbstractTableFactory
              .createChildRelationship(relationshipName, table, parentTable.get(), multiplicity);
      return Triple.of(Optional.of(childRel), table, fields);
    }
//    return Triple.of(Optional.empty(), table, fields);
  }
}
