package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.parse.tree.Assignment;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.local.analyze.Analyzer.Scope;
import ai.datasqrl.schema.SourceTableImportMeta;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.RootTableField;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;

/**
 * The analysis holds all extra information about a query for the generator.
 * <p>
 * We may not know the primary keys of the parent relation at the time of analysis
 */
@Getter
public class Analysis {
  /**
   * The schema (as a metadata object for the analyzer)
   */
  private Schema schema = new Schema();

  /* Assignment statements that add a column rather than a new query */
  private Set<Assignment> expressionStatements = new HashSet<>();

  /**
   * Functions that should be evaluated as a local aggregate e.g. count(orders.entries).
   *  also part of a query: SELECT count(a.orders.entries) FROM x WHERE x.y > 5;
   */
  private Set<FunctionCall> isLocalAggregate = new HashSet<>();

  /**
   * Function calls that have been resolved
   */
  private Map<FunctionCall, ResolvedFunctionCall> resolvedFunctions = new HashMap<>();

  /**
   * Identifiers and table nodes resolved to columns
   */
  private Map<Node, ResolvedNamePath> resolvedNamePath = new HashMap<>();

  /**
   * Assignments create or modify a table
   */
  private Map<Assignment, Table> producedTable = new HashMap<>();

  /**
   * Some queries don't have a self query defined for nested queries so
   * one needs to be added.
   */
  private Set<QuerySpecification> needsSelfTableJoin = new HashSet<>();

  /**
   * Import resolution definitions
   */
  private Map<ImportDefinition, List<SourceTableImport>> importSourceTables = new HashMap<>();
  private Map<ImportDefinition, Map<ai.datasqrl.schema.Table, SourceTableImportMeta.RowType>> importTableTypes = new HashMap<>();

  /**
   * Store scopes for field lookups
   */
  private Map<Node, Scope> scopes = new HashMap<>();

  private Map<Node, Field> producedField = new HashMap<>();

  public Map<Node, Name> tableAliases = new HashMap<>();

  public Map<Node, String> fieldAlias = new HashMap<>();

  //TODO: Register subqueries as separate anonymous queries

  @Value
  public static class ResolvedFunctionCall {
    private final SqrlAwareFunction function;
  }

  @Getter
  public static class ResolvedNamedReference extends ResolvedNamePath {
    private final Name name;
    private final int ordinal;

    public ResolvedNamedReference(Name name, int ordinal) {
      super(name.getCanonical(), Optional.empty(), List.of());
      this.name = name;
      this.ordinal = ordinal;
    }
  }

  /**
   * Resolved name path must be:
   * Maybe a root table w/ version
   * Maybe a relationship w/ version
   * Maybe a field, no version
   */
  @Getter
  public static class ResolvedNamePath {

    private final String alias;
    Optional<ResolvedNamePath> base;
    List<Field> path;

    public ResolvedNamePath(String alias, Optional<ResolvedNamePath> base, List<Field> path) {
      this.alias = alias;
      this.base = base;
      this.path = path;
    }

    public Table getToTable() {
      Field field = path.get(path.size() - 1);
      if (field instanceof RootTableField) {
        return ((RootTableField) field).getTable();
      } else if (field instanceof Relationship) {
        return ((Relationship) field).getToTable();
      } else {
        throw new RuntimeException("No table on field");
      }
    }
  }
}