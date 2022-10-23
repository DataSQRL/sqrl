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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor
public class VariableFactory {
  public Relationship addJoinDeclaration(NamePath namePath, SQRLTable parentTable, SQRLTable target, Multiplicity multiplicity,
      SqlNode query) {
    Relationship relationship = parentTable.addRelationship(namePath.getLast(), target,
        JoinType.JOIN, multiplicity, query);
    return relationship;
  }

  public Column addColumn(Name columnName, SQRLTable table, RelDataType type) {
    return table.addColumn(columnName, true, type);
  }

  public Optional<Pair<Relationship, Relationship>> linkParentChild(NamePath namePath, SQRLTable child, Optional<SQRLTable> parentTable) {
    if (namePath.size() > 1) {
      Relationship.Multiplicity multiplicity = Multiplicity.MANY;
      Name relationshipName = namePath.getLast();
//        if (specNorm.getLimit().flatMap(Limit::getIntValue).orElse(2) == 1) {
//          multiplicity = Multiplicity.ONE;
//        }

      Optional<Relationship> parent = AbstractTableFactory.createParentRelationship(child, parentTable.get());

      Relationship childRel = AbstractTableFactory
              .createChildRelationship(relationshipName, child, parentTable.get(), multiplicity);
      return Optional.of(Pair.of(parent.get(), childRel));
    } else return Optional.empty();
  }
}
