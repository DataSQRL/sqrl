package ai.datasqrl.plan.local.generate;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.SQRLTable;
import ai.datasqrl.schema.builder.AbstractTableFactory;
import lombok.AllArgsConstructor;

import java.util.Optional;

@AllArgsConstructor
public class VariableFactory {
  public Relationship addJoinDeclaration(NamePath namePath, SQRLTable parentTable, SQRLTable target, Multiplicity multiplicity) {
    Relationship relationship = parentTable.addRelationship(namePath.getLast(), target,
        JoinType.JOIN, multiplicity);
    parentTable.buildType();
    return relationship;
  }

  public Column addExpression(NamePath namePath, SQRLTable table) {
    Name columnName = namePath.getLast();
    Column column = table.addColumn(columnName, true);
    return column;
  }

  public Column addQueryExpression(NamePath namePath, SQRLTable table) {
    //do not create table, add column
    Name columnName = namePath.getLast();

    Column column = table.addColumn(columnName, true);
    return column;
  }

  public Optional<Relationship> linkParentChild(NamePath namePath, SQRLTable child, Optional<SQRLTable> parentTable) {
    if (namePath.size() > 1) {
      Relationship.Multiplicity multiplicity = Multiplicity.MANY;
      Name relationshipName = namePath.getLast();
//        if (specNorm.getLimit().flatMap(Limit::getIntValue).orElse(2) == 1) {
//          multiplicity = Multiplicity.ONE;
//        }

      AbstractTableFactory.createParentRelationship(child, parentTable.get());

      Relationship childRel = AbstractTableFactory
              .createChildRelationship(relationshipName, child, parentTable.get(), multiplicity);
      return Optional.of(childRel);
    } else return Optional.empty();
  }
}
