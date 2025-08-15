/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.planner;

import static com.datasqrl.planner.parser.StatementParserException.checkFatal;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.graphql.server.MutationComputedColumnType;
import com.datasqrl.graphql.server.MutationInsertType;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.loaders.FlinkTableNamespaceObject;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ScriptSqrlModule.ScriptNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.plan.global.StageAnalysis.Cost;
import com.datasqrl.plan.global.StageAnalysis.MissingCapability;
import com.datasqrl.plan.rules.EngineCapability;
import com.datasqrl.plan.rules.EngineCapability.Feature;
import com.datasqrl.plan.table.RelDataTypeTableSchema;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator.MutationBuilder;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.analyzer.cost.CostModel;
import com.datasqrl.planner.dag.DAGBuilder;
import com.datasqrl.planner.dag.nodes.ExportNode;
import com.datasqrl.planner.dag.nodes.TableFunctionNode;
import com.datasqrl.planner.dag.nodes.TableNode;
import com.datasqrl.planner.dag.plan.MutationComputedColumn;
import com.datasqrl.planner.dag.plan.MutationQuery;
import com.datasqrl.planner.hint.EngineHint;
import com.datasqrl.planner.hint.MutationInsertHint;
import com.datasqrl.planner.hint.NoQueryHint;
import com.datasqrl.planner.hint.PlannerHints;
import com.datasqrl.planner.hint.QueryByAnyHint;
import com.datasqrl.planner.hint.TestHint;
import com.datasqrl.planner.parser.AccessModifier;
import com.datasqrl.planner.parser.FlinkSQLStatement;
import com.datasqrl.planner.parser.ParsePosUtil;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SQLStatement;
import com.datasqrl.planner.parser.SqlScriptStatementSplitter;
import com.datasqrl.planner.parser.SqrlAddColumnStatement;
import com.datasqrl.planner.parser.SqrlCreateTableStatement;
import com.datasqrl.planner.parser.SqrlDefinition;
import com.datasqrl.planner.parser.SqrlExportStatement;
import com.datasqrl.planner.parser.SqrlImportStatement;
import com.datasqrl.planner.parser.SqrlStatement;
import com.datasqrl.planner.parser.SqrlStatementParser;
import com.datasqrl.planner.parser.SqrlTableDefinition;
import com.datasqrl.planner.parser.SqrlTableFunctionStatement;
import com.datasqrl.planner.parser.SqrlTableFunctionStatement.ParsedArgument;
import com.datasqrl.planner.parser.StackableStatement;
import com.datasqrl.planner.parser.StatementParserException;
import com.datasqrl.planner.tables.AccessVisibility;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.util.SqlNameUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.SqlAlterView;
import org.apache.flink.sql.parser.ddl.SqlAlterViewAs;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlDropView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.UserDefinedFunction;

/**
 * This is the main class for planning SQRL scripts. It relies on the {@link SqrlStatementParser}
 * for parsing and uses the {@link Sqrl2FlinkSQLTranslator} to access Flink's parser and planner for
 * the actual SQL parsing and planning.
 *
 * <p>In planning the SQRL statements, it uses produces a {@link TableAnalysis} that has the
 * information needed to build the computation DAG via {@link DAGBuilder}.
 */
public class SqlScriptPlanner {

  public static final String EXPORT_SUFFIX = "_ex";
  public static final String ACCESS_FUNCTION_SUFFIX = "__access";

  private final ErrorCollector errorCollector;

  private final ModuleLoader moduleLoader;
  private final SqrlStatementParser sqrlParser;
  private final PackageJson packageJson;
  private final ExecutionPipeline pipeline;
  private final ExecutionGoal executionGoal;

  private final CostModel costModel;
  @Getter private final DAGBuilder dagBuilder;
  @Getter private final ExecutionStage streamStage;
  private final List<ExecutionStage> tableStages;
  private final List<ExecutionStage> queryStages;
  private final List<ExecutionStage> subscriptionStages;
  private final AtomicInteger exportTableCounter = new AtomicInteger(0);

  // TODO: set this to false when processing a script import to another database
  private boolean generateAccessFunctions = true;

  @Inject
  public SqlScriptPlanner(
      ErrorCollector errorCollector,
      ModuleLoader moduleLoader,
      SqrlStatementParser sqrlParser,
      PackageJson packageJson,
      ExecutionPipeline pipeline,
      ExecutionGoal executionGoal) {
    this.errorCollector = errorCollector;
    this.moduleLoader = moduleLoader;
    this.sqrlParser = sqrlParser;
    this.packageJson = packageJson;
    this.pipeline = pipeline;
    this.executionGoal = executionGoal;

    this.costModel = packageJson.getCompilerConfig().getCostModel();
    this.dagBuilder = new DAGBuilder();
    // Extract the various types of stages supported by the configured pipeline
    var streamStage = pipeline.getStageByType(EngineType.STREAMS);
    errorCollector.checkFatal(
        streamStage.isPresent(), "Need to configure a stream execution engine");
    this.streamStage = streamStage.get();
    /* to support server execution in the future, we add server_query to tables and query Stages
    and server_subscribe to tables and subscription stages.
     */
    this.tableStages =
        pipeline.stages().stream()
            .filter(
                stage ->
                    stage.getType() == EngineType.DATABASE || stage.getType() == EngineType.STREAMS)
            .collect(Collectors.toList());
    this.queryStages =
        pipeline.stages().stream()
            .filter(stage -> stage.getType() == EngineType.DATABASE)
            .collect(Collectors.toList());
    this.subscriptionStages =
        pipeline.stages().stream()
            .filter(stage -> stage.getType() == EngineType.LOG)
            .collect(Collectors.toList());
  }

  /**
   * Main entry method for parsing a SQRL script. The bulk of this method ensure that exceptions and
   * errors are correctly mapped to the source so that users can easily understand what the issue is
   * and what's causing it.
   *
   * @param mainScript
   * @param sqrlEnv
   */
  public void planMain(MainScript mainScript, Sqrl2FlinkSQLTranslator sqrlEnv) {
    var scriptErrors = errorCollector.withScript(mainScript.getPath(), mainScript.getContent());
    var statements = sqrlParser.parseScript(mainScript.getContent(), scriptErrors);
    List<StackableStatement> statementStack = new ArrayList<>();
    for (ParsedObject<SQLStatement> statement : statements) {
      var lineErrors = scriptErrors.atFile(statement.getFileLocation());
      var sqlStatement = statement.get();
      try {
        planStatement(sqlStatement, statementStack, sqrlEnv, lineErrors);
      } catch (CollectedException e) {
        throw e;
      } catch (Throwable e) {
        // Map errors from the Flink parser/planner by adjusting the line numbers
        var converted = ParsePosUtil.convertFlinkParserException(e);
        if (converted.isPresent()) {
          var msgLocation = converted.get();
          e.printStackTrace();
          scriptErrors
              .atFile(
                  statement
                      .getFileLocation()
                      .add(sqlStatement.mapSqlLocation(msgLocation.getLocation())))
              .fatal(msgLocation.getMessage());
        }
        if (e instanceof ValidationException) {
          if (e.getCause() != null && e.getCause() != e) {
            e = e.getCause();
          }
        }

        // Print stack trace for unknown exceptions
        if (e.getMessage() == null
            || e instanceof IllegalStateException
            || e instanceof NullPointerException) {
          e.printStackTrace();
        }
        // Use registered error handlers
        throw lineErrors.handle(e);
      }
      /*Some SQRL statements extend previous statements, so we stack them to keep
      of the lineage as needed for planning
       */
      if (sqlStatement instanceof StackableStatement stackableStatement) {
        if (stackableStatement.isRoot()) {
          statementStack = new ArrayList<>();
        }
        statementStack.add(stackableStatement);
      } else {
        statementStack = new ArrayList<>();
      }
    }
  }

  /**
   * Plans an individual statement.
   *
   * @param stmt
   * @param statementStack
   * @param sqrlEnv
   * @param errors
   * @throws SqlParseException
   */
  private void planStatement(
      SQLStatement stmt,
      List<StackableStatement> statementStack,
      Sqrl2FlinkSQLTranslator sqrlEnv,
      ErrorCollector errors)
      throws SqlParseException {
    // Process hints & documentation
    var hints = PlannerHints.EMPTY;
    Optional<String> documentation = Optional.empty();
    if (stmt instanceof SqrlStatement statement) {
      var comments = statement.getComments();
      hints = PlannerHints.fromHints(comments);
      if (!comments.getDocumentation().isEmpty()) {
        documentation =
            Optional.of(
                comments.getDocumentation().stream()
                    .map(ParsedObject::get)
                    .map(String::trim)
                    .collect(Collectors.joining("\n")));
      }
    }
    var hintsAndDocs = new HintsAndDocs(hints, documentation);
    if (stmt instanceof SqrlImportStatement statement) {
      addImport(statement, hintsAndDocs, sqrlEnv, errors);
    } else if (stmt instanceof SqrlExportStatement statement) {
      addExport(statement, sqrlEnv);
    } else if (stmt instanceof SqrlCreateTableStatement statement) {
      sqrlEnv
          .createTable(statement.toSql(), getLogEngineBuilder(hintsAndDocs))
          .ifPresent(tableAnalysis -> addSourceToDag(tableAnalysis, hintsAndDocs, sqrlEnv));
    } else if (stmt instanceof SqrlDefinition sqrlDef) {
      var access = sqrlDef.getAccess();
      var tablePath = sqrlDef.getPath();
      if (sqrlDef instanceof SqrlAddColumnStatement) {
        // These require special treatment because they extend a previous table definition
        tablePath = sqrlDef.getPath().popLast();
        StatementParserException.checkFatal(
            !statementStack.isEmpty()
                && statementStack.get(0) instanceof SqrlTableDefinition
                && ((SqrlTableDefinition) statementStack.get(0)).getPath().equals(tablePath),
            sqrlDef.getTableName().getFileLocation(),
            ErrorCode.INVALID_SQRL_ADD_COLUMN,
            "Column expression must directly follow the definition of table [%s]",
            tablePath);
        access = ((SqrlTableDefinition) statementStack.get(0)).getAccess();
      }
      var nameIsHidden = tablePath.getLast().isHidden();
      // Ignore hidden tables and test tables (that are not queries) when we are not running tests
      if ((nameIsHidden || (executionGoal != ExecutionGoal.TEST && hints.isTest()))
          && !hints.isWorkload()) {
        // Test tables should not have access unless we are running tests or they are also workloads
        access = AccessModifier.NONE;
      }
      var isHidden =
          (nameIsHidden || hints.isWorkload())
              && !(hints.isTest() && executionGoal == ExecutionGoal.TEST);
      access = adjustAccess(access);

      var originalSql = sqrlDef.toSql(sqrlEnv, statementStack);
      // Relationships and Table functions require special handling
      if (sqrlDef instanceof SqrlTableFunctionStatement tblFctStmt) {
        var identifier =
            SqlNameUtil.toIdentifier(
                tblFctStmt
                    .getPath()
                    .getFirst()); // TODO: this should be resolved against the current catalog and
        // database
        final var arguments = new LinkedHashMap<Name, ParsedArgument>();
        if (!tblFctStmt.getSignature().isEmpty()) {
          var parsedArgs = sqrlEnv.parse2RelDataType(tblFctStmt.getSignature());
          parsedArgs.forEach(
              parsedField -> {
                var field = parsedField.field();
                var metadata =
                    parsedField
                        .metadata()
                        .map(
                            metaStr -> {
                              var resolvedMetadata =
                                  SqrlTableFunctionStatement.parseMetadata(
                                      metaStr, !field.getType().isNullable());
                              errors.checkFatal(
                                  resolvedMetadata.isPresent(),
                                  ErrorCode.INVALID_TABLE_FUNCTION_ARGUMENTS,
                                  "Invalid metadata key provided: %s",
                                  metaStr);
                              return resolvedMetadata.get();
                            });
                arguments.put(
                    Name.system(field.getName()),
                    new ParsedArgument(
                        new ParsedObject(field.getName(), FileLocation.START),
                        field.getType(),
                        metadata,
                        false,
                        arguments.size()));
              });
        }
        TableAnalysis parentTbl = null;
        if (tblFctStmt.isRelationship()) {
          /* To resolve the arguments and get their type, we first need to look up the parent table
           */
          var parentNode = dagBuilder.getNode(identifier);
          checkFatal(
              parentNode.isPresent(),
              sqrlDef.getTableName().getFileLocation(),
              ErrorCode.INVALID_TABLE_FUNCTION_ARGUMENTS,
              "Could not find parent table for relationship: %s",
              tblFctStmt.getPath().getFirst());
          checkFatal(
              parentNode.get() instanceof TableNode,
              sqrlDef.getTableName().getFileLocation(),
              ErrorCode.INVALID_TABLE_FUNCTION_ARGUMENTS,
              "Relationships can only be added to tables (not functions): %s [%s]",
              tblFctStmt.getPath().getFirst(),
              parentNode.get().getClass());
          identifier = SqlNameUtil.toIdentifier(tablePath.toString());
          parentTbl = ((TableNode) parentNode.get()).getTableAnalysis();
          checkFatal(
              parentTbl.getOptionalBaseTable().isEmpty(),
              ErrorCode.BASETABLE_ONLY_ERROR,
              "Relationships can only be added to the base table [%s]",
              parentTbl.getBaseTable().getIdentifier());
        }
        // Resolve arguments, map indexes, and check for errors
        Map<Integer, Integer> argumentIndexMap = new HashMap<>();
        for (ParsedArgument argIndex : tblFctStmt.getArgumentsByIndex()) {
          if (argIndex.isParentField()) {
            // Check if we need to add this argument when encountered for the first time
            RelDataTypeField field =
                parentTbl.getRowType().getField(argIndex.getName().get(), false, false);
            checkFatal(
                field != null,
                argIndex.getName().getFileLocation(),
                ErrorLabel.GENERIC,
                "Could not find field on parent table: %s",
                argIndex.getName().get());
            var fieldName = Name.system(field.getName());
            if (!arguments.containsKey(fieldName)) {
              arguments.put(
                  fieldName, argIndex.withResolvedType(field.getType(), arguments.size()));
            }
          }
          var signatureArg = arguments.get(Name.system(argIndex.getName().get()));
          checkFatal(
              signatureArg != null,
              argIndex.getName().getFileLocation(),
              ErrorCode.INVALID_TABLE_FUNCTION_ARGUMENTS,
              "Argument [%s] is not defined in the signature of the function",
              argIndex.getName().get());
          argumentIndexMap.put(argIndex.getIndex(), signatureArg.getIndex());
        }

        var fctBuilder =
            sqrlEnv.resolveSqrlTableFunction(
                identifier,
                originalSql,
                new ArrayList<>(arguments.values()),
                argumentIndexMap,
                hints,
                errors);
        fctBuilder.fullPath(tblFctStmt.getPath());
        var visibility =
            new AccessVisibility(access, hints.isTest(), tblFctStmt.isRelationship(), isHidden);
        fctBuilder.visibility(visibility);
        fctBuilder.documentation(documentation);
        var fct = fctBuilder.build();
        errors.checkFatal(
            dagBuilder.getNode(fct.getIdentifier()).isEmpty(),
            ErrorCode.FUNCTION_EXISTS,
            "Function or relationship [%s] already exists in catalog",
            tablePath);
        addFunctionToDag(fct, hintsAndDocs);
        if (!fct.getVisibility().isAccessOnly()) {
          sqrlEnv.registerSqrlTableFunction(fct);
        }
      } else {
        var visibility = new AccessVisibility(access, hints.isTest(), true, isHidden);
        addTableToDag(
            sqrlEnv.addView(originalSql, hints, errors), hintsAndDocs, visibility, false, sqrlEnv);
      }
    } else if (stmt instanceof FlinkSQLStatement flinkStmt) {
      var node = sqrlEnv.parseSQL(flinkStmt.getSql().get());
      if (node instanceof SqlCreateView || node instanceof SqlAlterViewAs) {
        // plan like other definitions from above
        var visibility =
            new AccessVisibility(adjustAccess(AccessModifier.QUERY), false, true, false);
        addTableToDag(
            sqrlEnv.addView(flinkStmt.getSql().get(), hints, errors),
            hintsAndDocs,
            visibility,
            false,
            sqrlEnv);
      } else if (node instanceof SqlCreateTable) {
        sqrlEnv
            .createTable(flinkStmt.getSql().get(), getLogEngineBuilder(hintsAndDocs))
            .ifPresent(tableAnalysis -> addSourceToDag(tableAnalysis, hintsAndDocs, sqrlEnv));
      } else if (node instanceof RichSqlInsert insert) {
        /*TODO: We are not currently adding these to the DAG (and hence no analysis/visualization based on the DAG)
        We would need to analyze the query and pull out the sources to make that happen. However, for now
        we are only doing this for FlinkSQL compatibility, so this may be fine. */
        sqrlEnv.insertInto(insert);
      } else if (node instanceof SqlAlterTable || node instanceof SqlAlterView) {
        errors.fatal(
            "Renaming or altering tables is not supported. Rename them directly in the script or IMPORT AS.");
      } else if (node instanceof SqlDropTable || node instanceof SqlDropView) {
        errors.fatal(
            "Removing tables is not supported. The DAG planner automatically removes unused tables.");
      } else {
        // just pass through
        sqrlEnv.executeSQL(flinkStmt.getSql().get());
      }
    }
  }

  /**
   * Adjusts the access for functions and tables based on the available stages and configuration We
   * might consider throwing an exception for SUBSCRIPTION access when no subscription stages are
   * present since the user explicitly defined the SUBSCRIBE.
   *
   * @param access
   * @return
   */
  private AccessModifier adjustAccess(AccessModifier access) {
    Preconditions.checkArgument(access != AccessModifier.INHERIT);
    if (!generateAccessFunctions || (access == AccessModifier.QUERY && queryStages.isEmpty())) {
      return AccessModifier.NONE;
    }
    if (access == AccessModifier.SUBSCRIPTION && subscriptionStages.isEmpty()) {
      return AccessModifier.NONE;
    }
    return access;
  }

  public static final Name STAR = Name.system("*");

  private Map<ExecutionStage, StageAnalysis> getSourceSinkStageAnalysis() {
    return Map.of(streamStage, new Cost(streamStage, costModel.getSourceSinkCost(), true));
  }

  /**
   * Computes the stage analysis for each of the given stages by analyzing whether a stage supports
   * the feature and functions of a table/function definitions.
   *
   * @param tableAnalysis
   * @param availableStages
   * @return
   */
  private Map<ExecutionStage, StageAnalysis> getStageAnalysis(
      TableAnalysis tableAnalysis, List<ExecutionStage> availableStages) {
    Map<ExecutionStage, StageAnalysis> stageAnalysis = new HashMap<>();
    for (ExecutionStage executionStage : availableStages) {
      List<EngineCapability> unsupported =
          tableAnalysis.getRequiredCapabilities().stream()
              .filter(
                  capability -> {
                    if (capability instanceof Feature feature) {
                      return !executionStage.supportsFeature(feature.getFeature());
                    } else if (capability instanceof EngineCapability.Function function) {
                      return !executionStage.supportsFunction(function.getFunction());
                    } else {
                      throw new UnsupportedOperationException(capability.getName());
                    }
                  })
              .collect(Collectors.toList());
      if (unsupported.isEmpty()) {
        stageAnalysis.put(
            executionStage,
            new Cost(executionStage, costModel.getCost(executionStage, tableAnalysis), true));
      } else {
        stageAnalysis.put(executionStage, new MissingCapability(executionStage, unsupported));
      }
    }
    return stageAnalysis;
  }

  /**
   * Determine which stages are applicable based on the configured stages for the type of
   * table/function and user-provided hints.
   *
   * @param availableStages
   * @param hints
   * @return
   */
  private List<ExecutionStage> determineStages(
      List<ExecutionStage> availableStages, PlannerHints hints) {
    Optional<EngineHint> executionHint = hints.getHint(EngineHint.class);
    if ((hints.isTest() && executionGoal == ExecutionGoal.TEST) || hints.isWorkload()) {
      // Tests and hints always get executed in the database
      availableStages =
          availableStages.stream()
              .filter(
                  stage ->
                      stage.getType() == EngineType.DATABASE
                          || stage.getType() == EngineType.SERVER)
              .collect(Collectors.toList());
      if (availableStages.isEmpty()) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            hints.getHint(TestHint.class).get().getSource().getFileLocation(),
            "Could not find suitable database stage to execute tests or workloads: %s",
            availableStages);
      }
    }
    if (executionHint.isPresent()) { // User provided a hint which takes precedence
      var execHint = executionHint.get();
      availableStages =
          availableStages.stream()
              .filter(
                  stage ->
                      execHint.getStageNames().stream()
                          .anyMatch(
                              name ->
                                  stage.name().equalsIgnoreCase(name)
                                      || stage.engine().getType().name().equalsIgnoreCase(name)))
              .collect(Collectors.toList());
      if (availableStages.isEmpty()) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            execHint.getSource().getFileLocation(),
            "Provided execution stages could not be found or are not configured: %s",
            execHint.getStageNames());
      }
    }
    assert !availableStages.isEmpty();
    return availableStages;
  }

  private List<ExecutionStage> determineViableStages(AccessModifier access) {
    if (access == AccessModifier.QUERY) {
      return queryStages;
    } else if (access == AccessModifier.SUBSCRIPTION) {
      return subscriptionStages;
    } else {
      return tableStages;
    }
  }

  /**
   * Adds a source table (i.e. IMPORTed or CREATEd) to the DAG. This requires some special handling
   * because source table are planned as two tables: the original definition of the table and a view
   * that we create on top for subsequent planning.
   *
   * @param tableAnalysis
   * @param sqrlEnv
   */
  private void addSourceToDag(
      TableAnalysis tableAnalysis, HintsAndDocs hintsAndDocs, Sqrl2FlinkSQLTranslator sqrlEnv) {
    Preconditions.checkArgument(tableAnalysis.getFromTables().size() == 1);
    var source = (TableAnalysis) tableAnalysis.getFromTables().get(0);
    Preconditions.checkArgument(source.isSourceOrSink());
    var sourceNode = new TableNode(source, getSourceSinkStageAnalysis());
    dagBuilder.add(sourceNode);
    var isHidden = tableAnalysis.getIdentifier().isHidden();
    var visibility =
        new AccessVisibility(
            isHidden ? AccessModifier.NONE : adjustAccess(AccessModifier.QUERY),
            false,
            true,
            isHidden);
    addTableToDag(tableAnalysis, hintsAndDocs, visibility, true, sqrlEnv);
  }

  /**
   * Adds a table to the DAG and plans the table access function based on the determined visibility
   * and provided hints.
   *
   * @param tableAnalysis
   * @param hintsAndDocs
   * @param visibility
   * @param sqrlEnv
   */
  private void addTableToDag(
      TableAnalysis tableAnalysis,
      HintsAndDocs hintsAndDocs,
      AccessVisibility visibility,
      boolean isSource,
      Sqrl2FlinkSQLTranslator sqrlEnv) {
    var availableStages = determineStages(tableStages, hintsAndDocs.hints());
    var tableNode =
        new TableNode(
            tableAnalysis,
            isSource
                ? getSourceSinkStageAnalysis()
                : getStageAnalysis(tableAnalysis, availableStages));
    dagBuilder.add(tableNode);

    // Figure out if and what type of access function we should add for this table
    var queryByHint = hintsAndDocs.hints().getQueryByHint();
    if (visibility.isEndpoint()) { // only add function if this table is an endpoint
      var relBuilder = sqrlEnv.getTableScan(tableAnalysis.getObjectIdentifier());
      List<FunctionParameter> parameters = List.of();
      if (queryByHint.isPresent()) { // hint takes precendence for defining the access function
        var hint = queryByHint.get();
        if (hint instanceof NoQueryHint) { // Don't add an access function
          return;
        }
        parameters =
            SqlScriptPlannerUtil.addFilterByColumn(
                relBuilder, hint.getColumnIndexes(), hint instanceof QueryByAnyHint);
      }
      relBuilder.project(
          IntStream.range(0, tableAnalysis.getFieldLength())
              .mapToObj(relBuilder::field)
              .collect(Collectors.toList()),
          tableAnalysis.getRowType().getFieldNames(),
          true); // Identity projection
      // TODO: should we add a default sort if the user didn't specify one to have predictable
      // result sets for testing?
      var tableName = tableAnalysis.getObjectIdentifier().getObjectName();
      var fctName = tableName + ACCESS_FUNCTION_SUFFIX;
      var fctBuilder =
          sqrlEnv.addSqrlTableFunction(
              SqlNameUtil.toIdentifier(fctName), relBuilder.build(), parameters, tableAnalysis);
      fctBuilder.fullPath(NamePath.of(tableName));
      fctBuilder.visibility(visibility);
      fctBuilder.documentation(hintsAndDocs.documentation());
      addFunctionToDag(
          fctBuilder.build(), HintsAndDocs.EMPTY); // hints don't apply to the function access
    } else if (queryByHint.isPresent()) {
      throw new StatementParserException(
          ErrorLabel.GENERIC,
          queryByHint.get().getSource().getFileLocation(),
          "query_by hints are only supported on tables that are queryable");
    }
  }

  private void addFunctionToDag(SqrlTableFunction function, HintsAndDocs hintsAndDocs) {
    var availableStages =
        determineStages(
            determineViableStages(function.getVisibility().getAccess()), hintsAndDocs.hints());
    dagBuilder.add(
        new TableFunctionNode(
            function, getStageAnalysis(function.getFunctionAnalysis(), availableStages)));
  }

  /**
   * Handles IMPORT statements which require loading via the {@link ModuleLoader} and planning the
   * loaded objects.
   *
   * @param importStmt
   * @param sqrlEnv
   * @param errors
   */
  private void addImport(
      SqrlImportStatement importStmt,
      HintsAndDocs hintsAndDocs,
      Sqrl2FlinkSQLTranslator sqrlEnv,
      ErrorCollector errors) {
    var path = importStmt.getPackageIdentifier().get();
    var isStar = path.getLast().equals(STAR);

    // Handling of the name alias if set
    Optional<Name> alias = Optional.empty();
    if (importStmt.getAlias().isPresent()) {
      var aliasPath = importStmt.getAlias().get();
      checkFatal(
          aliasPath.size() == 1,
          ErrorCode.INVALID_IMPORT,
          "Invalid table name - paths not supported");
      alias = Optional.of(aliasPath.getFirst());
    }

    var module = moduleLoader.getModule(path.popLast()).orElse(null);
    checkFatal(
        module != null,
        importStmt.getPackageIdentifier().getFileLocation(),
        ErrorLabel.GENERIC,
        "Could not find module [%s] at path: [%s]",
        path,
        String.join("/", path.toStringList()));

    if (isStar) {
      if (module.getNamespaceObjects().isEmpty()) {
        errors.warn("Module is empty: %s", path);
      }
      for (NamespaceObject namespaceObject : module.getNamespaceObjects()) {
        // For multiple imports, the alias serves as a prefix.
        addImport(
            namespaceObject,
            alias.map(x -> x.append(namespaceObject.getName()).getDisplay()),
            hintsAndDocs,
            sqrlEnv);
      }
    } else {
      var namespaceObject = module.getNamespaceObject(path.getLast());
      errors.checkFatal(
          namespaceObject.isPresent(), "Object [%s] not found in module: %s", path.getLast(), path);

      addImport(
          namespaceObject.get(),
          Optional.of(alias.orElse(path.getLast()).getDisplay()),
          hintsAndDocs,
          sqrlEnv);
    }
  }

  /**
   * Imports an individual {@link NamespaceObject}
   *
   * @param nsObject
   * @param alias
   * @param sqrlEnv
   */
  private void addImport(
      NamespaceObject nsObject,
      Optional<String> alias,
      HintsAndDocs hintsAndDocs,
      Sqrl2FlinkSQLTranslator sqrlEnv) {
    if (nsObject instanceof FlinkTableNamespaceObject object) { // import a table
      // TODO: for a create table statement without options (connector), we manage it internally
      // add pass it to Log engine for augmentation after validating/adding event-id and event-time
      // metadata columns & checking no watermark/partition/constraint is present
      var flinkTable = ExternalFlinkTable.fromNamespaceObject(object, alias, errorCollector);
      try {
        var tableAnalysis =
            sqrlEnv.createTableWithSchema(
                flinkTable.tableName.getDisplay(),
                flinkTable.sqlCreateTable,
                flinkTable.schema,
                getLogEngineBuilder(hintsAndDocs));
        hintsAndDocs.hints().updateColumnNamesHints(tableAnalysis::getField);
        addSourceToDag(tableAnalysis, hintsAndDocs, sqrlEnv);
      } catch (Throwable e) {
        throw flinkTable.errorCollector.handle(e);
      }
    } else if (nsObject instanceof FlinkUdfNsObject fnsObject) {
      Preconditions.checkArgument(
          fnsObject.getFunction() instanceof UserDefinedFunction,
          "Expected UDF: " + fnsObject.getFunction());
      Class<?> udfClass = fnsObject.getFunction().getClass();
      var name =
          alias.orElseGet(() -> FlinkUdfNsObject.getFunctionNameFromClass(udfClass).getDisplay());
      sqrlEnv.addUserDefinedFunction(name, udfClass.getName(), false);
    } else if (nsObject instanceof ScriptNamespaceObject scriptObject) {
      checkFatal(
          scriptObject.getTableName().isEmpty(),
          ErrorLabel.GENERIC,
          "Cannot import an individual table from SQRL script. Use * to import entire script: %s",
          scriptObject.getName());
      planMain(scriptObject.getScript(), sqrlEnv);
    } else {
      throw new UnsupportedOperationException("Unexpected object imported: " + nsObject);
    }
  }

  /**
   * For CREATE TABLE statements without connectors which are mutations, we provide this
   * MutationBuilder to fill in the connector settings based on the configured log engine.
   *
   * @return
   */
  private MutationBuilder getLogEngineBuilder(HintsAndDocs hintsAndDocs) {
    var logStage = pipeline.getMutationStage();
    if (logStage.isEmpty()) {
      return (t, d) -> {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            FileLocation.START,
            "CREATE TABLE requires that a log engine is configured that supports mutations");
      };
    }
    var engine = (LogEngine) logStage.get().engine();
    return (tableBuilder, datatype) -> {
      var originalTableName = tableBuilder.getTableName();
      var mutationBuilder = MutationQuery.builder();
      mutationBuilder.stage(logStage.get());
      MutationInsertType insertType =
          hintsAndDocs
              .hints()
              .getHint(MutationInsertHint.class)
              .map(MutationInsertHint::getInsertType)
              .orElse(MutationInsertType.SINGLE);
      mutationBuilder.createTopic(
          engine.createMutation(
              logStage.get(), originalTableName, tableBuilder, datatype, insertType));
      mutationBuilder.name(Name.system(originalTableName));
      mutationBuilder.insertType(insertType);
      mutationBuilder.documentation(hintsAndDocs.documentation());
      tableBuilder
          .extractMetadataColumns(MutationComputedColumn.UUID_METADATA, true)
          .forEach(
              colName ->
                  mutationBuilder.computedColumn(
                      new MutationComputedColumn(colName, MutationComputedColumnType.UUID)));
      tableBuilder
          .extractMetadataColumns(MutationComputedColumn.TIMESTAMP_METADATA, false)
          .forEach(
              colName ->
                  mutationBuilder.computedColumn(
                      new MutationComputedColumn(colName, MutationComputedColumnType.TIMESTAMP)));
      return mutationBuilder;
    };
  }

  /**
   * Plans EXPORT statements
   *
   * @param exportStmt
   * @param sqrlEnv
   */
  private void addExport(SqrlExportStatement exportStmt, Sqrl2FlinkSQLTranslator sqrlEnv) {
    var sinkPath = exportStmt.getPackageIdentifier().get();
    var sinkName = sinkPath.getLast();
    var tablePath = exportStmt.getTableIdentifier().get();

    // Lookup the table that is being exported
    var tableNode = dagBuilder.getNode(SqlNameUtil.toIdentifier(tablePath.getLast()));
    var inputNode =
        tableNode
            .orElseThrow(
                () ->
                    new StatementParserException(
                        ErrorLabel.GENERIC,
                        exportStmt.getTableIdentifier().getFileLocation(),
                        "Could not find table: %s",
                        tablePath.toString()))
            .unwrap(TableNode.class);

    // Set a unique id for the exported table
    var exportTableId =
        sinkName.getDisplay() + EXPORT_SUFFIX + exportTableCounter.incrementAndGet();
    var stageAnalysis = getSourceSinkStageAnalysis();
    ExportNode exportNode;

    // First, we check if the export is to a built-in sink, if so, resolve it
    var builtInSink =
        SystemBuiltInConnectors.forExport(sinkPath.getFirst()).filter(x -> sinkPath.size() == 2);
    if (builtInSink.isPresent()) {
      var connector = builtInSink.get();
      ExecutionStage exportStage;
      if (connector == SystemBuiltInConnectors.LOG_ENGINE) {
        var logStage = pipeline.getStageByType(EngineType.LOG);
        errorCollector.checkFatal(
            logStage.isPresent(), "Cannot export to log since no log engine has been configured");
        exportStage = logStage.get();
      } else {
        var engineName = connector.getName().getCanonical();
        if (connector == SystemBuiltInConnectors.LOGGER) {
          engineName = packageJson.getCompilerConfig().getLogger();
          if (engineName.equalsIgnoreCase("none")) {
            return; // simply ignore
          }
        }
        var optStage = pipeline.getStage(engineName);
        errorCollector.checkFatal(
            optStage.isPresent(),
            "The configured logger `%s` under 'compiler.logger' is not a configured engine.",
            engineName);
        exportStage = optStage.get();
      }
      exportNode =
          new ExportNode(stageAnalysis, sinkPath, Optional.of(exportStage), Optional.empty());
    } else { // the export is to a user-defined sink: load it
      var module = moduleLoader.getModule(sinkPath.popLast()).orElse(null);
      checkFatal(
          module != null,
          exportStmt.getPackageIdentifier().getFileLocation(),
          ErrorLabel.GENERIC,
          "Could not find module [%s] at path: [%s]",
          sinkPath,
          String.join("/", sinkPath.toStringList()));

      var sinkObj = module.getNamespaceObject(sinkName);
      checkFatal(
          sinkObj.isPresent(),
          exportStmt.getPackageIdentifier().getFileLocation(),
          ErrorLabel.GENERIC,
          "Could not find table [%s] in module [%s]",
          sinkName,
          module);
      checkFatal(
          sinkObj.get() instanceof FlinkTableNamespaceObject,
          exportStmt.getPackageIdentifier().getFileLocation(),
          ErrorLabel.GENERIC,
          "Not a valid sink table [%s] in module [%s]",
          sinkName,
          module);
      var sinkTable = (FlinkTableNamespaceObject) sinkObj.get();

      var flinkTable =
          ExternalFlinkTable.fromNamespaceObject(
              sinkTable, Optional.of(sinkName.getDisplay()), errorCollector);
      var schema =
          flinkTable.schema.or(() -> Optional.of(inputNode.getTableAnalysis().getRowType()));
      try {
        var tableId = sqrlEnv.addExternalExport(exportTableId, flinkTable.sqlCreateTable, schema);
        exportNode =
            new ExportNode(stageAnalysis, sinkPath, Optional.empty(), Optional.of(tableId));
      } catch (Throwable e) {
        throw flinkTable.errorCollector.handle(e);
      }
    }
    dagBuilder.addExport(exportNode, inputNode);
  }

  /**
   * Represents an externally defined table in FlinkSQL with an optional schema definition in a
   * separate file.
   *
   * <p>This is used for both imports and exports.
   */
  @Value
  public static class ExternalFlinkTable {

    Name tableName;
    String sqlCreateTable;
    Optional<RelDataType> schema;
    ErrorCollector errorCollector;

    public static ExternalFlinkTable fromNamespaceObject(
        FlinkTableNamespaceObject nsObject, Optional<String> alias, ErrorCollector errorCollector) {
      var flinkTable = nsObject.getTable();
      var tableName = alias.map(Name::system).orElse(flinkTable.getName());

      // Parse SQL
      var tableSql = flinkTable.getFlinkSQL();
      var tableError = errorCollector.withScript(flinkTable.getFlinkSqlFile(), tableSql);
      tableSql = SqlScriptStatementSplitter.formatEndOfSqlFile(tableSql);

      // Schema conversion
      Optional<RelDataType> schema =
          flinkTable
              .getSchema()
              .map(
                  tableSchema -> {
                    if (tableSchema instanceof RelDataTypeTableSchema typeTableSchema) {
                      return typeTableSchema.getRelDataType();
                    } else {
                      return SchemaToRelDataTypeFactory.load(tableSchema)
                          .map(tableSchema, tableName, errorCollector);
                    }
                  });

      return new ExternalFlinkTable(tableName, tableSql, schema, tableError);
    }
  }

  static record HintsAndDocs(PlannerHints hints, Optional<String> documentation) {

    public static final HintsAndDocs EMPTY = new HintsAndDocs(PlannerHints.EMPTY, Optional.empty());
  }
}
