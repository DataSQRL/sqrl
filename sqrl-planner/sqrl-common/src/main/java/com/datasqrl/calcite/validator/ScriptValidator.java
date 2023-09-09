package com.datasqrl.calcite.validator;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.schema.op.LogicalOp;
import com.datasqrl.loaders.ModuleLoader;
import org.apache.calcite.sql.SqrlAssignTimestamp;
import org.apache.calcite.sql.SqrlDistinctQuery;
import org.apache.calcite.sql.SqrlExportDefinition;
import org.apache.calcite.sql.SqrlExpressionQuery;
import org.apache.calcite.sql.SqrlFromQuery;
import org.apache.calcite.sql.SqrlImportDefinition;
import org.apache.calcite.sql.SqrlJoinQuery;
import org.apache.calcite.sql.SqrlSqlQuery;
import org.apache.calcite.sql.SqrlStatementVisitor;
import org.apache.calcite.sql.SqrlStreamQuery;

/**
 * Graphql: sort of like inference but just all tokens
 *
 * Run the whole planner and validator w/ starwars schema.
 * Dump out all queries to snapshot: flink, postgres, and server
 *
 * Search(@text: String) :=
 * SELECT * FROM Human UNION ALL SELECT * FROM Droid UNION ALL SELECT * FROM Starship;
 * -- where textsearch(@text, coalesce(cols))
 *
 * Character := SELECT * FROM Human UNION ALL SELECT * FROM Droid;
 *
 * hero(@episode: String) :=
 * SELECT * FROM Character
 * WHERE name = 'luke' AND @episode = 'NEWHOPE'
 *    OR name = 'r2d2' AND @episode != 'NEWHOPE';
 *
 *
 */
public class ScriptValidator implements SqrlStatementVisitor<Void, Void> {


  private final SqrlFramework framework;
  private final ModuleLoader moduleLoader;

  public ScriptValidator(SqrlFramework framework, ModuleLoader moduleLoader) {

    this.framework = framework;
    this.moduleLoader = moduleLoader;
  }

  /**
   * Error cases:
   * IMPORT x.* AS t
   *
   * If import fails, add error <- Add to schema an unknown object?
   *
   * If import passes, collect namespace objects for statement?
   */
  @Override
  public Void visit(SqrlImportDefinition statement, Void context) {
    return null;
  }

  /**
   * Edge cases:
   * LHS not a table
   * RHS not a sink
   * Resolve all field names for export
   */
  @Override
  public Void visit(SqrlExportDefinition statement, Void context) {
    return null;
  }

  /**
   * Same as validate query
   * No args
   */
  @Override
  public Void visit(SqrlStreamQuery statement, Void context) {
    return null;
  }

  /**
   * lhs must exist and be assignable
   * Cannot be aggregating
   *
   * Warn if args are not used in query
   * Warn if AS is used to name an expression
   *
   * Cannot shadow a table's primary key
   *
   */
  @Override
  public Void visit(SqrlExpressionQuery statement, Void context) {
    return null;
  }

  /**
   * Rewrite table to be plannable: Take the table def and convert it to a function w/ lateral joins
   * We could retain this query and convert it back to sql for later processing. We can then expand
   * the function in a simpler way.
   *
   * Can be plannable
   * Can be assignable
   * If nested, has a '@' table as the first table
   * Validate args
   *
   * Find all SELECT * on direct tables, these are possible Aliases.
   *
   * Assure columns are named and no collisions.
   *
   * We need a global PK incrementer to allow for multiple select * and have it still work (UUID collisions)
   *
   * If it errors completely:
   * Derive type field names, give ANY
   *
   * Allow all function resolution during validation, no need to plan, give any type
   *
   * Allow UNION access tables
   *
   */
  @Override
  public Void visit(SqrlSqlQuery statement, Void context) {
    return null;
  }

  /**
   * if JOIN, it should be nested. if FROM, can be anywhere
   *
   * Append an alias.* to the end so we can use the same logic to determine
   * what it points to.
   *
   */
  @Override
  public Void visit(SqrlJoinQuery statement, Void context) {
    return null;
  }

  @Override
  public Void visit(SqrlFromQuery statement, Void context) {
    return null;
  }

  /**
   * Validate as query, cannot be nested (probably can be but disallow). No joins.
   *
   * Must have Order by statement
   */
  @Override
  public Void visit(SqrlDistinctQuery statement, Void context) {
    return null;
  }

  /**
   * Warning cases:
   * IMPORT t TIMESTAMP expression <- should have alias
   */
  @Override
  public Void visit(SqrlAssignTimestamp statement, Void context) {
    return null;
  }

  public void validate(String script) {

  }
}
