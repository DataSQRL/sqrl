package ai.datasqrl.plan.local.transpiler.nodes.relation;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Hint;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeLocation;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.schemaRef.TableOrRelationship;
import ai.datasqrl.plan.local.transpiler.nodes.schemaRef.TableRef;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Table;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * An normalized version of {@link ai.datasqrl.parse.tree.TableNode}
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class TableNodeNorm extends RelationNorm {

  private final NamePath name;
  private final Optional<Name> alias;
  private final TableOrRelationship ref;
  private final boolean isLocalTable;
  private final List<Hint> hints;

  public TableNodeNorm(Optional<NodeLocation> location, NamePath name,
      Optional<Name> alias, TableOrRelationship ref, boolean isLocalTable, List<Hint> hints) {
    super(location);
    this.name = name;
    this.alias = alias;
    this.ref = ref;
    this.isLocalTable = isLocalTable;
    this.hints = hints;
  }

  public static TableNodeNorm of(Table table) {
    return new TableNodeNorm(Optional.empty(), table.getPath(), Optional.empty(), new TableRef(table),
        false, List.of());
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitTableNorm(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return List.of();
  }

  @Override
  public RelationNorm getLeftmost() {
    return this;
  }

  @Override
  public RelationNorm getRightmost() {
    return this;
  }

  @Override
  public Name getFieldName(Expression references) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public List<Expression> getFields() {
    return ref.getTable().getVisibleColumns().stream()
        .map(c-> ResolvedColumn.of(this, c))
        .collect(Collectors.toList());
  }

  @Override
  public List<Expression> getPrimaryKeys() {
    return ref.getTable().getPrimaryKeys().stream()
        .map(c-> ResolvedColumn.of(this, c))
        .collect(Collectors.toList());
  }

  @Override
  public Optional<Expression> walk(NamePath namePath) {
    Optional<List<Field>> fields = this.getRef().getTable().walkFields(namePath);
    if (fields.isEmpty()) {
      return Optional.empty();
    }

    Field field = fields.get().get(fields.get().size() - 1);
    if (field instanceof Relationship) {
      Relationship rel = (Relationship) field;
      Column column = rel.getToTable().getPrimaryKeys().get(0);
      return Optional.of(ResolvedColumn.of(this, column));
    } else {
      Column column = (Column) field;
      return Optional.of(ResolvedColumn.of(this, column));
    }
  }
}
