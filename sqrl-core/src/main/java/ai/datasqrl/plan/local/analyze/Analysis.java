package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.parse.tree.*;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.*;
import ai.datasqrl.schema.Relationship.Multiplicity;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;

import java.util.*;
import java.util.stream.Collectors;

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
  private VariableFactory schema = new VariableFactory();

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
  private Map<Assignment, VarTable> producedTable = new HashMap<>();

  /**
   * Created fields (in order)
   */
  private Map<Assignment, List<Field>> producedFieldList = new HashMap<>();

  /**
   * Some queries don't have a self query defined for nested queries so
   * one needs to be added.
   */
  private Set<QuerySpecification> needsSelfTableJoin = new HashSet<>();

  /**
   * Import resolution definitions
   */
  private Map<ImportDefinition, List<SourceTableImport>> importSourceTables = new HashMap<>();
  private Map<ImportDefinition, Map<VarTable, SourceTableImportMeta.RowType>> importTableTypes = new HashMap<>();

  private Map<Node, Field> producedField = new HashMap<>();

  public Map<Node, Name> tableAliases = new HashMap<>();

  public Map<Node, String> fieldAlias = new HashMap<>();
  public Map<Node, VarTable> parentTable = new HashMap<>();
  public Map<Node, List<DatasetTable>> importDataset = new HashMap<>();

  @Setter
  public List<Integer> groupByOrdinals = new ArrayList<>();

  @Setter
  public List<SortItem> orderByExpressions = new ArrayList<>();
  @Setter
  public List<Expression> uniqueOrderExpressions = new ArrayList<>();
  public Map<Select, List<SingleColumn>> selectItems = new HashMap<>();

  public Map<QuerySpecification, TableNode> selfTableNode = new HashMap();

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

  public static class ResolvedTable extends ResolvedNamePath {

    public ResolvedTable(String alias, Optional<ResolvedNamePath> base, List<Field> path) {
      super(alias, base, path);
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

    public VarTable getToTable() {
      Field field = path.get(path.size() - 1);
      if (field instanceof RootTableField) {
        return ((RootTableField) field).getTable();
      } else if (field instanceof Relationship) {
        return ((Relationship) field).getToTable();
      } else {
        throw new RuntimeException("No table on field");
      }
    }

    public NamePath getNamePath() {
      return NamePath.of(this.path.stream().map(e->e.getName()).collect(Collectors.toList()));
    }

    public Field getLast() {
      return this.path.get(this.path.size() - 1);
    }

    public boolean isToMany() {
      if (getPath().isEmpty()) {
        return false;
      } else if (getPath().size() == 1 && getPath().get(0) instanceof Column) {
        return false;
      }

      for (Field field : getPath()) {
        if (field instanceof Relationship
            && ((Relationship) field).getMultiplicity() != Multiplicity.MANY) {
          return false;
        }
      }
      return true;
    }

    public boolean isToOne() {
      if (getPath().isEmpty()) {
        return false;
      } else if (getPath().size() == 1 && getPath().get(0) instanceof Column) {
        return false;
      }

      for (Field field : getPath()) {
        if (field instanceof Relationship
            && ((Relationship) field).getMultiplicity() != Multiplicity.ONE) {
          return false;
        }
      }
      return true;
    }
  }
}