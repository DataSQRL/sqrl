package ai.datasqrl.plan.local.analyzer;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.parse.tree.Assignment;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.local.analyzer.Analyzer.Scope;
import ai.datasqrl.plan.local.operations.SourceTableImportOp;
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

  /* The parent table to the query/expression */
//  private Map<Query, Table> parentTable = new HashMap<>();

  /* Assignment statements that add a column rather than a new query */
  private Set<Assignment> expressionStatements = new HashSet<>();

  /* The final select list expressions, may be expanded from a SELECT * (star) */
  //Select * resolved through scope
//  private Map<QuerySpecification, List<Expression>> selectExpressions = new HashMap<>();

  /**
   * Order by statements not specified in the select list may need
   * to be added to push to the API layer as a query time concern.
   */
//  private Map<QuerySpecification, List<Expression>> uniqueOrderBy = new HashMap<>();

  /**
   * An inline path that needs expansion, e.g. orders.entries;
   */
//  private Map<Identifier, InlinePath> inlinePaths = new HashMap<>();

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
   * An explicitly defined table
   */
//  private Map<TableNode, ResolvedNamePath> resolvedTables = new HashMap<>();

  /**
   * Assignments create or modify a table
   */
  private Map<Assignment, TableVersion> producedTable = new HashMap<>();

  /**
   * Some queries don't have a self query defined for nested queries so
   * one needs to be added.
   */
  private Set<QuerySpecification> needsSelfTableJoin = new HashSet<>();

  /**
   * Import resolution definitions
   */
  private Map<ImportDefinition, List<SourceTableImport>> importSourceTables = new HashMap<>();
  private Map<ImportDefinition, Map<ai.datasqrl.schema.Table, SourceTableImportOp.RowType>> importTableTypes = new HashMap<>();

  /**
   * Store scopes for field lookups
   */
  private Map<Node, Scope> scopes = new HashMap<>();

  private Map<Node, Field> producedField = new HashMap<>();

  public Map<Node, Name> tableAliases = new HashMap<>();

  public Map<Node, String> fieldAlias = new HashMap<>();

  //TODO: Register subqueries as separate anonymous queries

  /**
   * Adding a new column to a table creates a new table version.
   */
  @AllArgsConstructor
  @Getter
  public static class TableVersion {
    Table table;
    int version;

    public Name getId() {
      return Name.system(table.getId().getCanonical());
    }

    //TODO: Returns a new table w/ calcite stuff
  }

  /**
   * An resolved column or expression
   */
  public interface ResolvedIdentifier { }

  /**
   * An column reference from a table.
   *
   * SELECT column FROM Table;
   *        ------
   */
//  @Value
//  public static class ResolvedColumn implements ResolvedIdentifier {
//    ResolvedTable origin;
//    Column column;
//  }

// Subqueries use anonymous tables
//  /**
//   * A reference from a subquery.
//   *
//   * SELECT sum(a.expr) FROM (SELECT expr) a;
//   *              ----
//   */
//  @Value
//  public static class ResolvedExpression implements ResolvedIdentifier {
//    ResolvedSubQuery origin;
//    Expression expression;
//  }
//
//  @AllArgsConstructor
//  @Getter
//  public static class ResolvedTable implements ResolvedReference {
//    /**
//     * Table path starts with an alias from this table.
//     */
//    Optional<ResolvedTable> base;
//    ResolvedNamePath tablePath;
//    TableVersion tableVersion; //?
//  }

  @Getter
  public static class ResolvedSubQuery implements ResolvedReference {
    //? Query query;
    Optional<Name> alias;
  }

  /**
   * A reference that can produce fields, like a table or subquery
   */
  public interface ResolvedReference { }

  @Value
  public static class ResolvedFunctionCall {
    private final SqrlAwareFunction function;
  }

  @Value
  public static class InlinePath {
    ResolvedReference base;
    ResolvedNamePath path;
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

    private final String alias2;
    Optional<ResolvedNamePath> base;
    List<Field> path;

    public ResolvedNamePath(String alias, Optional<ResolvedNamePath> base, List<Field> path) {
      this.alias2 = alias;
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

    public Optional<String> getAlias() {
      return null;
    }

    public ResolvedNamePath trimTrailingColumn() {
      return this;
    }
  }

//
//  /**
//   * SQRL allows constructing bushy join trees using a simpler syntax than conventional
//   * sql. This is desirable since certain queries can only be accomplished
//   * using join trees. For example:
//   *
//   * -- Customers orders with entries that have a product
//   * (assuming product is optional)
//   * FROM Customer.orders o LEFT JOIN o.entries.product p;
//   *
//   * We want to do our best to construct a good join tree:
//   *
//   *          /       \ (left + order->entries join condition or declaration expansion + user defined)
//   *         /     /     \
//   *        /\  entries  product
//   * Customer orders
//   *
//   * Join declarations are expanded by adding the join tree
//   */
//  public static class JoinTree {
//    /**
//     * Let's try to make it easy in this object to:
//     * 1. Compose join conditions, delay PK columns until later (b/c subqueries has unobservable pks)
//     * 2. Not have to introduce self joins to join tables
//     * 3. Can do join declaration expansion
//     * 4. Easily convert a tree
//     */
//
//    private final TableNode table;
//    private final List<JoinCondition> conditions;
//    private final List<JoinTree> children;
//
//    public JoinTree(TableNode table, List<JoinCondition> conditions, List<JoinTree> children) {
//      this.table = table;
//      this.conditions = conditions;
//      this.children = children;
//    }
//  }
//
//  public class JoinCondition {
//    private final TableNode table;
//    private final Column column;
//    private final Column foreignColumn;
//
//    public JoinCondition(TableNode table, Column column, Column foreignColumn) {
//      this.table = table;
//      this.column = column;
//      this.foreignColumn = foreignColumn;
//    }
//  }



  /**
   *  Join declaration:
   *  Assume: FROM _ (join declaration)
   *
   *        customer->orders condition
   *     /  |
   *  [base] tree
   *
   *  returns: Orders (the right-most table)
   */



}
