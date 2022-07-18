package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedNamePath;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedTable;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.RootTableField;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Setter;

@Getter
public class Scope {

  private final Schema schema;
  private final Optional<Table> contextTable;
  private final Optional<Boolean> isExpression;
  private final Optional<Name> targetName;

  /**
   * Fields available for resolution.
   */
  @Getter
  private final Map<Name, ResolvedNamePath> joinScopes = new HashMap<>();
  //Used to create internal joins that cannot be unreferenced by alias
  private final AtomicInteger internalIncrementer = new AtomicInteger();
  private final AtomicBoolean hasExpandedSelf = new AtomicBoolean();

  /**
   * Query has a context table explicitly defined in the query. Self fields are always available
   * but do not cause ambiguity problems when fields are referenced without aliases.
   * <p>
   * e.g. FROM _ JOIN _.orders; vs FROM _.orders;
   */
  @Setter
  private boolean isSelfInScope = false;
  @Setter
  private List<Name> fieldNames = new ArrayList<>();
  @Setter
  List<SingleColumn> selectItems = new ArrayList<>();
  @Setter
  private boolean isInGroupByOrSortBy = false;

  @Setter
  private boolean allowIdentifierPaths = true;
  public Scope(Schema schema, Optional<Table> contextTable, boolean isExpression, Name targetName) {
    this(schema, contextTable, Optional.of(isExpression), Optional.of(targetName));
  }

  public Scope(Schema schema, Optional<Table> contextTable, Optional<Boolean> isExpression,
      Optional<Name> targetName) {
    this.schema = schema;
    this.contextTable = contextTable;
    this.isExpression = isExpression;
    this.targetName = targetName;
  }

  public List<Name> getFieldNames() {
    return fieldNames;
  }

  public ResolvedNamePath resolveNamePathOrThrow(NamePath namePath) {
    Optional<List<ResolvedNamePath>> paths = resolveNamePath(namePath);
    List<ResolvedNamePath> path = paths.orElseThrow(
        () -> new RuntimeException("Cannot find path: " + namePath));
    if (path.size() == 0) {
      throw new RuntimeException("Cannot find path: " + namePath);
    } else if (path.size() > 1) {
      throw new RuntimeException("Ambiguous path: " + namePath);
    }
    return path.get(0);
  }

  //Tables have one extra requirement, they must have an alias for relative table names
  public Optional<List<ResolvedNamePath>> resolveTable(NamePath namePath) {
    return resolveNamePath(namePath, true);
  }

  public Optional<List<ResolvedNamePath>> resolveNamePath(NamePath namePath) {
    return resolveNamePath(namePath, false);
  }

  public Optional<List<ResolvedNamePath>> resolveNamePath(NamePath namePath,
      boolean requireAlias) {
    List<ResolvedNamePath> resolved = new ArrayList<>();
    Optional<ResolvedNamePath> explicitAlias = Optional.ofNullable(
        joinScopes.get(namePath.getFirst()));

    if (explicitAlias.isPresent() && (namePath.getLength() == 1 || walk(
        namePath.getFirst().getCanonical(), explicitAlias.get(),
        namePath.popFirst()).isPresent())) { //Alias take priority
      if (namePath.getLength() == 1) {
        return explicitAlias.map(List::of);
      } else {
        resolved.add(walk(namePath.getFirst().getCanonical(), explicitAlias.get(),
            namePath.popFirst()).get());
        return Optional.of(resolved);
      }
    }

    // allows resolution from join scopes
    if (!requireAlias) {
      for (Map.Entry<Name, ResolvedNamePath> entry : joinScopes.entrySet()) {
        //Avoid conditions where columns may be ambiguous if self table is not declared explicitly
        if (entry.getKey().equals(ReservedName.SELF_IDENTIFIER) && !isSelfInScope()) {
          continue;
        }

        Optional<ResolvedNamePath> resolvedPath = walk(entry.getKey().getCanonical(),
            entry.getValue(), namePath);
        resolvedPath.ifPresent(resolved::add);
      }
    }

    Optional<ResolvedNamePath> path = walkSchema(namePath);
    path.ifPresent(resolved::add);

    return resolved.isEmpty() ? Optional.empty() : Optional.of(resolved);
  }

  private Optional<ResolvedNamePath> walkSchema(NamePath namePath) {
    Optional<Table> table = schema.getTable(namePath.getFirst());
    List<Field> fields = new ArrayList<>();
    if (table.isPresent()) {
      Field root = new RootTableField(table.get());
      fields.add(root);
      for (Name name : namePath.popFirst()) {
        if (table.isEmpty()) {
          return Optional.empty();
        }
        Optional<Field> field = table.get().getField(name);
        if (field.isEmpty()) {
          return Optional.empty();
        }
        fields.add(field.get());
        table = field.flatMap(this::getTableOfField);
      }
    } else {
      return Optional.empty();
    }

    return Optional.of(
        new ResolvedNamePath(namePath.getFirst().getCanonical(), Optional.empty(), fields));
  }

  private Optional<ResolvedNamePath> walk(String alias, ResolvedNamePath resolvedNamePath,
      NamePath namePath) {
    Field field = resolvedNamePath.getPath().get(resolvedNamePath.getPath().size() - 1);
    Optional<Table> table = getTableOfField(field);
    if (table.isEmpty()) {
      return Optional.empty();
    }

    Optional<List<Field>> fields = walk(table.get(), namePath);
    if (fields.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(new ResolvedNamePath(alias, Optional.of(resolvedNamePath), fields.get()));
  }

  private Optional<List<Field>> walk(Table tbl, NamePath namePath) {
    Optional<Table> table = Optional.of(tbl);
    List<Field> fields = new ArrayList<>();
    for (Name name : namePath) {
      if (table.isEmpty()) {
        return Optional.empty();
      }
      Optional<Field> field = table.get().getField(name);
      if (field.isEmpty()) {
        return Optional.empty();
      }
      fields.add(field.get());
      table = field.flatMap(this::getTableOfField);
    }

    return Optional.of(fields);
  }

  private Optional<Table> getTableOfField(Field field) {
    if (field instanceof RootTableField) {
      return Optional.of(((RootTableField) field).getTable());
    } else if (field instanceof Relationship) {
      return Optional.of(((Relationship) field).getToTable());
    } else {
      return Optional.empty();
    }
  }


  protected static Scope createLocalScope(NamePath namePath, boolean isExpression, Schema schema, Optional<Table> contextTable) {
    Scope scope = new Scope(schema, contextTable, isExpression, namePath.getLast());
    addSelfToScope(scope);
    scope.setSelfInScope(true);
    return scope;
  }

  public static Scope createScope(NamePath namePath, boolean isExpression, Schema schema, Optional<Table> contextTable) {
    return new Scope(schema, contextTable, isExpression, namePath.getLast());
  }

  public static Scope createSingleTableScope(Schema schema, ResolvedTable resolvedTable) {
    Scope scope = new Scope(schema, Optional.empty(), Optional.empty(),
        Optional.empty());
    scope.setAllowIdentifierPaths(false);
    scope.getJoinScopes().put(resolvedTable.getToTable().getName(), resolvedTable);
    return scope;
  }

  public static void addSelfToScope(Scope scope) {
    ResolvedNamePath resolvedNamePath = new ResolvedNamePath("_", Optional.empty(),
        List.of(new RootTableField(scope.getContextTable().get())));
    scope.joinScopes.put(ReservedName.SELF_IDENTIFIER, resolvedNamePath);
  }

  public static Scope createGroupOrSortScope(Scope scope) {
    Scope newScope = new Scope(scope.schema, scope.contextTable, scope.isExpression,
        scope.targetName);
    newScope.setSelfInScope(scope.isSelfInScope);
    newScope.setFieldNames(scope.fieldNames);
    newScope.isInGroupByOrSortBy = true;
    newScope.joinScopes.putAll(scope.getJoinScopes());
    return newScope;
  }

  public boolean isNested() {
    return contextTable.isPresent();
  }
}