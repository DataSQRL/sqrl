package ai.datasqrl.plan.local.transpiler;

import ai.datasqrl.parse.tree.AllColumns;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.SelectItem;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
class RelationScope {

  private final Schema schema;
  private final Optional<Table> contextTable;
  private final Optional<Boolean> isExpression;
  private final Optional<Name> targetName;

  /**
   * Fields available for resolution.
   */
  private final Map<Name, RelationNorm> joinScopes = new HashMap<>();
  //Used to create internal joins that cannot be unreferenced by alias
  private final AtomicInteger internalIncrementer = new AtomicInteger();
  private final AtomicBoolean hasExpandedSelf = new AtomicBoolean();
  private final List<JoinNorm> addlJoins = new ArrayList<>();
  private final Map<RelationNorm, Name> aliasRelationMap = new HashMap<>();

  /**
   * Query has a context table explicitly defined in the query. Self fields are always
   * available but do not cause ambiguity problems when fields are referenced without
   * aliases.
   *
   * e.g. FROM _ JOIN _.orders; vs FROM _.orders;
   */
  @Setter
  private boolean isSelfInScope = false;

  public RelationScope(Schema schema, Optional<Table> contextTable, boolean isExpression,
      Name targetName) {
    this(schema, contextTable, Optional.of(isExpression), Optional.of(targetName));
  }

  public RelationScope(Schema schema, Optional<Table> contextTable, Optional<Boolean> isExpression,
      Optional<Name> targetName) {
    this.schema = schema;
    this.contextTable = contextTable;
    this.isExpression = isExpression;
    this.targetName = targetName;
  }

  /**
   * Expands SELECT *
   */
  public List<SingleColumn> expandSelect(List<SelectItem> selectItems, RelationScope scope) {
    //     includeSelfWithSelectStar = false;
    List<SingleColumn> expressions = new ArrayList<>();
    for (SelectItem item : selectItems) {
      if (item instanceof AllColumns) {
        Optional<Name> alias = ((AllColumns) item).getPrefix()
            .map(e -> e.getFirst());
        List<Expression> fields = resolveFieldsWithPrefix(alias);
        for (Expression expr : fields) {
          expressions.add(new SingleColumn(Optional.empty(), expr, Optional.empty()));
        }
      } else {
        SingleColumn col = (SingleColumn) item;
        if (col.getAlias().isEmpty()) {
          if (col.getExpression() instanceof Identifier) {
            expressions.add(
                new SingleColumn(col.getExpression(), Optional.empty()));
          } else {
            //Try to name the column something reasonable so table headers are readable
            if (scope.isExpression.isPresent() && scope.getIsExpression().get()) {
              Name nextName = scope.getContextTable().get().getNextFieldId(scope.getTargetName().get());
              expressions.add(new SingleColumn(col.getExpression(), Optional.of(new Identifier(nextName))));
            } else {
              log.warn("Column missing a derivable alias:" + col.getExpression());
              expressions.add(new SingleColumn(col.getExpression(), Optional.empty()));
            }
          }
        } else {
          expressions.add(col);
        }
      }
    }
    return expressions;
  }

  private List<Expression> resolveFieldsWithPrefix(Optional<Name> alias) {
    if (alias.isPresent()) {
      RelationNorm norm = joinScopes.get(alias.get());
      Preconditions.checkNotNull(norm, "Could not find table %s",
          alias.get());
//      List<Expression> exprs = norm.getFields().stream()
//          .map(f -> new Identifier(Optional.empty(), f.getName().toNamePath()))
//          .collect(Collectors.toList());
      return new ArrayList<>(norm.getFields());
    }

    List<Expression> allFields = new ArrayList<>();
    for (Map.Entry<Name, RelationNorm> entry : joinScopes.entrySet()) {
      if (isSelfTable(entry.getValue()) && !isSelfInScope) {
        continue;
      }

//      Table table = entry.getValue().getRef().getTable();
//      List<Identifier> identifiers = table.getColumns().stream()
//          .map(f -> new Identifier(Optional.empty(),
//              entry.getKey().toNamePath().concat(f.getName())))
//          .collect(Collectors.toList());
      allFields.addAll(entry.getValue().getFields());
    }

    return allFields;
  }

  private boolean isSelfTable(RelationNorm norm) {
    return norm instanceof TableNodeNorm && ((TableNodeNorm)norm).isLocalTable();
  }

  public List<RelationNorm> resolve(NamePath name) {
    //TODO: Move to fields provided from the RelationNorm
    List<RelationNorm> resolved = new ArrayList<>();
    Optional<RelationNorm> explicitAlias = Optional.ofNullable(joinScopes.get(name.getFirst()));

    if (explicitAlias.isPresent() && explicitAlias.get().walk(name.popFirst()).isPresent()) { //Alias take priority
      resolved.add(explicitAlias.get());
      return resolved;
    }

    for (Map.Entry<Name, RelationNorm> entry : joinScopes.entrySet()) {
      if (entry.getKey().equals(ReservedName.SELF_IDENTIFIER) && !isSelfInScope()) {
        continue;
      }

      if (entry.getValue().walk(name).isPresent()) {
        resolved.add(entry.getValue());
      }
    }
    return resolved;
  }
}
