package ai.dataeng.sqml.planner;

import static ai.dataeng.sqml.tree.name.Name.SELF_IDENTIFIER;

import ai.dataeng.sqml.schema.Namespace;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.tree.name.VersionedName;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SqrlCalciteTable;

@AllArgsConstructor
public class CalciteTableFactory {
  private final Optional<NamePath> contextPath;
  private final Namespace namespace;
  private final RelDataTypeFieldFactory fieldFactory;

  public Optional<org.apache.calcite.schema.Table> create(String name) {
    NamePath tablePath = NamePath.parse(name.split("\\$")[0]);

    boolean hasContextPath = tablePath.getFirst().equals(SELF_IDENTIFIER);

    if (hasContextPath) {
      // E.g:
      // _
      // _.orders.entries
      Optional<Table> self = contextPath.flatMap(namespace::lookup);
      if (self.isEmpty()) {
        throw new RuntimeException(String.format("Could not find context for table: %s", name));
      }

      Optional<FieldPath> fieldPath = FieldPath.walkPath(self.get(), tablePath.popFirst());
      Table destinationTable = fieldPath
          .map(f->((Relationship)f.getLastField()).getToTable())
          .orElse(self.get());
      SelfField selfField = new SelfField(self.get());


      return create(self.get(), addFieldPath(selfField, fieldPath), destinationTable, true, name,
          false, fieldPath
              .map(f->((Relationship)f.getFields().get(0))).orElse(null));
    } else {
      // E.g:
      // Orders
      // Orders.entries
      // entries$4

      Optional<Table> table;
      if (name.contains("$")) {
        VersionedName versionedName = VersionedName.parse(name);
        table = namespace.lookup(versionedName);
        if (table.isEmpty()) {
          throw new RuntimeException();
        }
      } else {
        table = namespace.lookup(tablePath.getFirst().toNamePath());
      }

      if (table.isEmpty()) {
        return Optional.empty();
      }
      Optional<FieldPath> fieldPath = FieldPath.walkPath(table.get(), tablePath.popFirst());
      Table destinationTable = fieldPath
          .map(f->((Relationship)f.getLastField()).getToTable())
          .orElse(table.get());
      TableField tableField = new TableField(table.get());


      return create(table.get(), addFieldPath(tableField, fieldPath), destinationTable, false, name,
          false, null);
    }
  }

  private FieldPath addFieldPath(Field field, Optional<FieldPath> fieldPath) {
    List<Field> fieldPath2 = fieldPath.map(FieldPath::getFields).orElse(List.of());
    List<Field> newFields = new ArrayList<>();
    newFields.add(field);
    newFields.addAll(fieldPath2);

    return new FieldPath(newFields);
  }

  /**
   * Used for join paths. Instead of looking up a table, we walk based on parameters
   *
   * e.g:
   * FROM _ s JOIN s.orders o JOIN o.entries e
   */
  public Optional<org.apache.calcite.schema.Table> createPath(
      Table table, String name) {
    NamePath path = NamePath.parse(name);

    Optional<FieldPath> fieldPath = FieldPath.walkPath(table, path);
    Table destinationTable = fieldPath
        .map(f->((Relationship)f.getLastField()).getToTable())
        .orElseThrow(() -> new RuntimeException("Could not walk table"));

    return create(table, fieldPath.get(), destinationTable, false, name,
        true, fieldPath
            .map(f->((Relationship)f.getFields().get(0))).orElse(null));
  }

  private Optional<org.apache.calcite.schema.Table> create(Table baseTable,
      FieldPath fieldPath,
      Table destinationTable, boolean isContext, String name,
      boolean isPath, Relationship relationship) {
    List<RelDataTypeField> fields = new ArrayList<>();
    fields.addAll(fieldFactory.create(destinationTable));

    return Optional.of(new SqrlCalciteTable(destinationTable, baseTable, fieldPath, fieldFactory,
        isContext, fields, name, isPath, relationship));
  }
}
