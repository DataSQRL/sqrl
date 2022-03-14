package ai.dataeng.sqml.planner;

import static ai.dataeng.sqml.tree.name.Name.SELF_IDENTIFIER;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.name.NamePath;
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
    NamePath tablePath = NamePath.parse(name);

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

      return create(self.get(), fieldPath, destinationTable, true, name,
          false, fieldPath
              .map(f->((Relationship)f.getFields().get(0))).orElse(null));
    } else {
      // E.g:
      // Orders
      // Orders.entries

      Optional<Table> table = namespace.lookup(tablePath.getFirst().toNamePath());
      if (table.isEmpty()) {
        return Optional.empty();
      }
      Optional<FieldPath> fieldPath = FieldPath.walkPath(table.get(), tablePath.popFirst());
      Table destinationTable = fieldPath
          .map(f->((Relationship)f.getLastField()).getToTable())
          .orElse(table.get());

      return create(table.get(), fieldPath, destinationTable, false, name,
          false, null);
    }
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

    Optional<FieldPath> additionalFields = fieldPath
        .filter(f->f.getFields().size() > 1)
        .map(f->new FieldPath(f.getFields().subList(1, f.getFields().size())));

    return create(table, additionalFields, destinationTable, false, name,
        true, fieldPath
            .map(f->((Relationship)f.getFields().get(0))).orElse(null));
  }

  private Optional<org.apache.calcite.schema.Table> create(Table baseTable,
      Optional<FieldPath> fieldPath,
      Table destinationTable, boolean isContext, String name,
      boolean isPath, Relationship relationship) {
    List<RelDataTypeField> fields = new ArrayList<>();
    fields.addAll(fieldFactory.create(destinationTable));

    return Optional.of(new SqrlCalciteTable(destinationTable, baseTable, fieldPath, fieldFactory,
        isContext, fields, name, isPath, relationship));
  }
}
