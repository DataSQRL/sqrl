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

import static com.datasqrl.config.SqrlConstants.FLINK_DEFAULT_CATALOG;
import static com.datasqrl.config.SqrlConstants.FLINK_DEFAULT_DATABASE;
import static com.datasqrl.planner.parser.SqlScriptStatementSplitter.removeStatementDelimiter;
import static com.datasqrl.planner.parser.StatementParserException.checkFatal;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.engine.stream.flink.FlinkEngineFactory;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.graphql.server.MutationInsertType;
import com.datasqrl.io.schema.SchemaConversionResult;
import com.datasqrl.loaders.FlinkTableNamespaceObject;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.loaders.NamespaceObject;
import com.datasqrl.loaders.ScriptSqrlModule.ScriptNamespaceObject;
import com.datasqrl.loaders.TableWriter;
import com.datasqrl.loaders.schema.SchemaLoader;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.plan.global.StageAnalysis.Cost;
import com.datasqrl.plan.global.StageAnalysis.MissingCapability;
import com.datasqrl.plan.rules.EngineCapability;
import com.datasqrl.plan.rules.EngineCapability.Feature;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator.AddTableResult;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator.MutationBuilder;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.analyzer.TableOrFunctionAnalysis;
import com.datasqrl.planner.analyzer.cost.CostModel;
import com.datasqrl.planner.dag.DAGBuilder;
import com.datasqrl.planner.dag.nodes.ExportNode;
import com.datasqrl.planner.dag.nodes.TableFunctionNode;
import com.datasqrl.planner.dag.nodes.TableNode;
import com.datasqrl.planner.dag.plan.MutationMetadataExtractor;
import com.datasqrl.planner.dag.plan.MutationQuery;
import com.datasqrl.planner.hint.CacheHint;
import com.datasqrl.planner.hint.EngineHint;
import com.datasqrl.planner.hint.MutationInsertHint;
import com.datasqrl.planner.hint.NoQueryHint;
import com.datasqrl.planner.hint.PlannerHints;
import com.datasqrl.planner.hint.QueryByAnyHint;
import com.datasqrl.planner.hint.TestHint;
import com.datasqrl.planner.hint.TtlHint;
import com.datasqrl.planner.parser.AccessModifier;
import com.datasqrl.planner.parser.FlinkSQLStatement;
import com.datasqrl.planner.parser.ParsePosUtil;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.ParsedStatement;
import com.datasqrl.planner.parser.SQLStatement;
import com.datasqrl.planner.parser.SqlScriptStatementSplitter;
import com.datasqrl.planner.parser.SqrlAddColumnStatement;
import com.datasqrl.planner.parser.SqrlCreateTableStatement;
import com.datasqrl.planner.parser.SqrlDefinition;
import com.datasqrl.planner.parser.SqrlExportStatement;
import com.datasqrl.planner.parser.SqrlImportStatement;
import com.datasqrl.planner.parser.SqrlNextBatch;
import com.datasqrl.planner.parser.SqrlPassthroughTableFunctionStatement;
import com.datasqrl.planner.parser.SqrlStatement;
import com.datasqrl.planner.parser.SqrlStatementParser;
import com.datasqrl.planner.parser.SqrlTableDefinition;
import com.datasqrl.planner.parser.SqrlTableFunctionStatement;
import com.datasqrl.planner.parser.SqrlTableFunctionStatement.ParsedArgument;
import com.datasqrl.planner.parser.StackableStatement;
import com.datasqrl.planner.parser.StatementParserException;
import com.datasqrl.planner.tables.AccessVisibility;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.planner.util.SqlScriptWriter;
import com.datasqrl.planner.util.SqlTableNameExtractor;
import com.datasqrl.util.FunctionUtil;
import com.datasqrl.util.StringUtil;
import com.google.common.base.Preconditions;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.SqlAlterView;
import org.apache.flink.sql.parser.ddl.SqlAlterViewAs;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlDropView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * This is the main class for planning SQRL scripts. It relies on the {@link SqrlStatementParser}
 * for parsing and uses the {@link Sqrl2FlinkSQLTranslator} to access Flink's parser and planner for
 * the actual SQL parsing and planning.
 *
 * <p>In planning the SQRL statements, it uses produces a {@link TableAnalysis} that has the
 * information needed to build the computation DAG via {@link DAGBuilder}.
 */
@Component
@Lazy
public class SqlScriptPlanner {

  public static final String EXPORT_SUFFIX = "_ex";
  public static final String ACCESS_FUNCTION_SUFFIX = "__access";

  private final ErrorCollector errorCollector;

  /** Used to assemble the full script with imports as a string */
  @Getter private final SqlScriptWriter completeScript;

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

  /** Manages the context of the script that is processed, adjusted for imports */
  private ScriptContext scriptContext;

  @Inject
  public SqlScriptPlanner(
      ErrorCollector errorCollector,
      ModuleLoader moduleLoader,
      SqrlStatementParser sqrlParser,
      PackageJson packageJson,
      ExecutionPipeline pipeline,
      ExecutionGoal executionGoal) {
    this.errorCollector = errorCollector;
    this.completeScript = new SqlScriptWriter();
    this.scriptContext = new ScriptContext(moduleLoader, FLINK_DEFAULT_DATABASE, true);
    this.sqrlParser = sqrlParser;
    this.packageJson = packageJson;
    this.pipeline = pipeline;
    this.executionGoal = executionGoal;

    this.costModel = packageJson.getCompilerConfig().getCostModel();
    this.dagBuilder = new DAGBuilder();
    // Extract the various types of stages supported by the configured pipeline
    var streamStage = pipeline.getStageByType(EngineType.PROCESS);
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
                    stage.getType() == EngineType.DATABASE || stage.getType() == EngineType.PROCESS)
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
    for (ParsedStatement sourceStmt : statements) {
      var statement = sourceStmt.statement();
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
                      .add(sqlStatement.mapSqlLocation(msgLocation.location())))
              .fatal(msgLocation.message());
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
      if (!(sqlStatement instanceof SqrlImportStatement)) {
        completeScript.append(sourceStmt.source());
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
   */
  private void planStatement(
      SQLStatement stmt,
      List<StackableStatement> statementStack,
      Sqrl2FlinkSQLTranslator sqrlEnv,
      ErrorCollector errors) {
    // Process hints & documentation
    var hints = PlannerHints.EMPTY;
    Optional<String> documentation = Optional.empty();
    if (stmt instanceof SqrlStatement statement) {
      var comments = statement.getComments();
      hints = PlannerHints.fromHints(comments);
      if (!comments.documentation().isEmpty()) {
        documentation =
            Optional.of(
                comments.documentation().stream()
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
          .createTable(
              statement.toSql(),
              getLogEngineBuilder(hintsAndDocs),
              scriptContext.moduleLoader().getSchemaLoader())
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
        var identifier = scriptContext.toIdentifier(tablePath.getFirst());
        var tblNodeOpt = dagBuilder.getNode(identifier);
        Preconditions.checkArgument(
            tblNodeOpt.isPresent() && tblNodeOpt.get() instanceof TableNode,
            "Could not find table [%s] but located the stack",
            tablePath);
        ((SqrlAddColumnStatement) sqrlDef)
            .setColumnNames(
                ((TableNode) tblNodeOpt.get()).getAnalysis().getRowType().getFieldNames());
      }

      var isHidden =
          tablePath.getLast().isHidden()
              || (shouldExcludeTestTable(hints))
              || (hints.isWorkload() && !(hints.isTest() && executionGoal == ExecutionGoal.TEST));

      // Ignore any hidden table
      access = adjustAccess(isHidden ? AccessModifier.NONE : access);

      var originalSql = sqrlDef.toSql(sqrlEnv, statementStack);
      // Relationships and Table functions require special handling
      if (sqrlDef instanceof SqrlTableFunctionStatement tblFnStmt) {
        // TODO: should be resolved against the current catalog and database
        var identifier = scriptContext.toIdentifier(tblFnStmt.getPath().getFirst());
        final var arguments = new LinkedHashMap<Name, ParsedArgument>();
        if (!tblFnStmt.getSignature().isEmpty()) {
          var parsedArgs = sqrlEnv.parse2RelDataType(tblFnStmt.getSignature());
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
                        parsedField.function(),
                        false,
                        arguments.size()));
              });
        }
        TableAnalysis parentTbl = null;
        if (tblFnStmt.isRelationship()) {
          /* To resolve the arguments and get their type, we first need to look up the parent table
           */
          var parentNode = dagBuilder.getNode(identifier);
          checkFatal(
              parentNode.isPresent(),
              sqrlDef.getTableName().getFileLocation(),
              ErrorCode.INVALID_TABLE_FUNCTION_ARGUMENTS,
              "Could not find parent table for relationship: %s",
              tblFnStmt.getPath().getFirst());
          checkFatal(
              parentNode.get() instanceof TableNode,
              sqrlDef.getTableName().getFileLocation(),
              ErrorCode.INVALID_TABLE_FUNCTION_ARGUMENTS,
              "Relationships can only be added to tables (not functions): %s [%s]",
              tblFnStmt.getPath().getFirst(),
              parentNode.get().getClass());
          identifier = scriptContext.toIdentifier(tablePath.toString());
          parentTbl = ((TableNode) parentNode.get()).getTableAnalysis();
          checkFatal(
              parentTbl.getOptionalBaseTable().isEmpty(),
              ErrorCode.BASETABLE_ONLY_ERROR,
              "Relationships can only be added to the base table [%s]",
              parentTbl.getBaseTable().getIdentifier());
        }
        // Resolve arguments, map indexes, and check for errors
        Map<Integer, Integer> argumentIndexMap = new HashMap<>();
        for (ParsedArgument argIndex : tblFnStmt.getArgumentsByIndex()) {
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

        SqrlTableFunction.SqrlTableFunctionBuilder fnBuilder;
        boolean passthroughFn = false;
        if (tblFnStmt instanceof SqrlPassthroughTableFunctionStatement passthroughStmt) {
          passthroughFn = true;
          originalSql = removeStatementDelimiter(passthroughStmt.getDefinitionBody().get().trim());
          for (Map.Entry<Integer, Integer> mapping : argumentIndexMap.entrySet()) {
            originalSql =
                originalSql.replace(
                    SqrlStatementParser.POSITIONAL_ARGUMENT_PREFIX + mapping.getKey(),
                    SqrlStatementParser.POSITIONAL_ARGUMENT_PREFIX + mapping.getValue());
          }
          // extract from tables using simple-regex
          var fromTableNames = SqlTableNameExtractor.findTableNames(originalSql);
          List<TableOrFunctionAnalysis> fromTables =
              fromTableNames.stream()
                  .map(
                      tblName -> {
                        var node = dagBuilder.getNode(scriptContext.toIdentifier(tblName));
                        errors.checkFatal(
                            node.isPresent(),
                            "Could not find table %s referenced in query",
                            tblName);
                        errors.checkFatal(
                            node.get() instanceof TableNode, "Referenced table %s is", tblName);
                        return (TableOrFunctionAnalysis)
                            ((TableNode) node.get()).getTableAnalysis();
                      })
                  .toList();
          fnBuilder =
              sqrlEnv.resolveSqrlPassThroughTableFunction(
                  identifier,
                  originalSql,
                  new ArrayList<>(arguments.values()),
                  passthroughStmt.getReturnType(),
                  fromTables,
                  hints,
                  errors);
        } else {
          fnBuilder =
              sqrlEnv.resolveSqrlTableFunction(
                  identifier,
                  originalSql,
                  new ArrayList<>(arguments.values()),
                  argumentIndexMap,
                  hints,
                  errors);
        }
        fnBuilder.fullPath(tblFnStmt.getPath());
        var visibility =
            new AccessVisibility(
                access, hints.isTest(), tblFnStmt.isRelationship() || passthroughFn, isHidden);
        fnBuilder.visibility(visibility);
        fnBuilder.documentation(documentation);
        fnBuilder.cacheDuration(getCacheDuration(hintsAndDocs));
        var fn = fnBuilder.build();
        errors.checkFatal(
            dagBuilder.getNode(fn.getIdentifier()).isEmpty(),
            ErrorCode.FUNCTION_EXISTS,
            "Function or relationship [%s] already exists in catalog",
            tablePath);
        addFunctionToDag(fn, hintsAndDocs);
        if (!fn.getVisibility().isAccessOnly()) {
          sqrlEnv.registerSqrlTableFunction(fn);
        }
      } else {
        var visibility = new AccessVisibility(access, hints.isTest(), true, isHidden);

        if (!shouldExcludeTestTable(hints)) {
          addTableToDag(
              sqrlEnv.addView(originalSql, hints, errors),
              hintsAndDocs,
              visibility,
              false,
              sqrlEnv);
        }
      }
    } else if (stmt instanceof SqrlNextBatch) {
      var enabledEngines = packageJson.getEnabledEngines();
      errors.checkFatal(
          enabledEngines.size() == 1
              && enabledEngines.get(0).equals(FlinkEngineFactory.ENGINE_NAME),
          ErrorCode.INVALID_NEXT_BATCH,
          "NEXT_BATCH usage with an unsupported engine setup: %s",
          enabledEngines);
      sqrlEnv.nextBatch();

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
            .createTable(
                flinkStmt.getSql().get(),
                getLogEngineBuilder(hintsAndDocs),
                scriptContext.moduleLoader().getSchemaLoader())
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

  private boolean shouldExcludeTestTable(PlannerHints hints) {
    return hints.isTest() && executionGoal != ExecutionGoal.TEST;
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
    if (!scriptContext.generateAccess
        || (access == AccessModifier.QUERY && queryStages.isEmpty())) {
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
                      return !executionStage.supportsFeature(feature.feature());
                    } else if (capability instanceof EngineCapability.Function function) {
                      return !executionStage.supportsFunction(function.function());
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
      var fnName = tableName + ACCESS_FUNCTION_SUFFIX;
      var fnBuilder =
          sqrlEnv.addSqrlTableFunction(
              scriptContext.toIdentifier(fnName), relBuilder.build(), parameters, tableAnalysis);
      fnBuilder.fullPath(NamePath.of(tableName));
      fnBuilder.visibility(visibility);
      fnBuilder.documentation(hintsAndDocs.documentation());
      fnBuilder.cacheDuration(getCacheDuration(hintsAndDocs));
      addFunctionToDag(
          fnBuilder.build(), HintsAndDocs.EMPTY); // hints don't apply to the function access
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
            determineViableStages(function.getVisibility().access()), hintsAndDocs.hints());
    dagBuilder.add(
        new TableFunctionNode(
            function, getStageAnalysis(function.getFunctionAnalysis(), availableStages)));
  }

  private static Duration getCacheDuration(HintsAndDocs hintsAndDocs) {
    return hintsAndDocs
        .hints()
        .getHint(CacheHint.class)
        .map(CacheHint::getDuration)
        .orElse(Duration.ZERO);
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
    NamePath aliasPath = null;
    if (importStmt.getAlias().isPresent()) {
      aliasPath = importStmt.getAlias().get();
      checkFatal(
          aliasPath.size() == 1,
          ErrorCode.INVALID_IMPORT,
          "Invalid table name - paths not supported");
    }
    Optional<Name> alias = Optional.ofNullable(aliasPath).map(NamePath::getFirst);

    var module = scriptContext.moduleLoader.getModule(path.popLast()).orElse(null);
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
            alias.isPresent() ? (name -> alias.get() + name) : Function.identity(),
            hintsAndDocs,
            sqrlEnv);
      }
    } else {
      var namespaceObject = module.getNamespaceObject(path.getLast());
      errors.checkFatal(
          namespaceObject.isPresent(), "Object [%s] not found in module: %s", path.getLast(), path);

      addImport(
          namespaceObject.get(),
          alias.isPresent() ? (name -> alias.get().getDisplay()) : Function.identity(),
          hintsAndDocs,
          sqrlEnv);
    }
  }

  /**
   * Imports an individual {@link NamespaceObject}
   *
   * @param nsObject
   * @param importNameModifier
   * @param sqrlEnv
   */
  private void addImport(
      NamespaceObject nsObject,
      Function<String, String> importNameModifier,
      HintsAndDocs hintsAndDocs,
      Sqrl2FlinkSQLTranslator sqrlEnv) {
    if (nsObject instanceof FlinkTableNamespaceObject object) { // import a table
      // TODO: for a create table statement without options (connector), we manage it internally
      // add pass it to Log engine for augmentation after validating/adding event-id and event-time
      // metadata columns & checking no watermark/partition/constraint is present
      var flinkTable = ExternalFlinkTable.fromNamespaceObject(object, errorCollector);
      try {
        var tableAnalysis =
            sqrlEnv.createTableWithSchema(
                importNameModifier,
                flinkTable.sqlCreateTable,
                flinkTable.schemaLoader(),
                getLogEngineBuilder(hintsAndDocs));
        hintsAndDocs.hints().updateColumnNamesHints(tableAnalysis::getField);
        addSourceToDag(tableAnalysis, hintsAndDocs, sqrlEnv);
        completeScript.append(tableAnalysis.getOriginalSql());
      } catch (Throwable e) {
        throw flinkTable.errorCollector.handle(e);
      }
    } else if (nsObject instanceof FlinkUdfNsObject fnsObject) {
      Preconditions.checkArgument(
          fnsObject.function() instanceof UserDefinedFunction,
          "Expected UDF: " + fnsObject.function());
      Class<?> udfClass = fnsObject.function().getClass();
      var name = importNameModifier.apply(FunctionUtil.getFunctionName(udfClass).getDisplay());
      var sqlNode = sqrlEnv.addUserDefinedFunction(name, udfClass.getName(), false);
      completeScript.append(sqlNode);
    } else if (nsObject instanceof ScriptNamespaceObject scriptObject) {
      final ScriptContext priorContext = scriptContext;
      scriptContext =
          priorContext.fromImport(
              scriptObject.getModuleLoader(),
              scriptObject.isInline(),
              importNameModifier.apply(scriptObject.name().getDisplay()));

      if (priorContext.hasDifferentDatabase(scriptContext)) {
        // Switch DB
        var stmts = sqrlEnv.setDatabase(scriptContext.databaseName(), false);
        completeScript.append(stmts);
      }

      planMain(scriptObject.getScript(), sqrlEnv);

      if (priorContext.hasDifferentDatabase(scriptContext)) {
        // Switch it back
        var stmts = sqrlEnv.setDatabase(priorContext.databaseName(), true);
        completeScript.append(stmts);
      }
      scriptContext = priorContext;
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
      return (n, t, d) -> {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            FileLocation.START,
            "CREATE TABLE requires that a log engine is configured that supports mutations");
      };
    }
    var engine = (LogEngine) logStage.get().engine();
    return (origTableName, tableBuilder, dataType) -> {
      var mutationBuilder = MutationQuery.builder();
      mutationBuilder.generateAccess(scriptContext.generateAccess);
      mutationBuilder.stage(logStage.get());
      MutationInsertType insertType =
          hintsAndDocs
              .hints()
              .getHint(MutationInsertHint.class)
              .map(MutationInsertHint::getInsertType)
              .orElse(MutationInsertType.SINGLE);
      mutationBuilder.createTopic(
          engine.createMutation(
              logStage.get(),
              origTableName,
              tableBuilder,
              dataType,
              insertType,
              hintsAndDocs.hints().getHint(TtlHint.class).flatMap(TtlHint::getTtl)));
      mutationBuilder.name(Name.system(origTableName));
      mutationBuilder.insertType(insertType);
      mutationBuilder.documentation(hintsAndDocs.documentation());
      // UUID and TIMESTAMP are special cases
      tableBuilder
          .extractMetadataColumns(new MutationMetadataExtractor())
          .forEach(mutationBuilder::computedColumn);
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
    var tableNode = dagBuilder.getNode(scriptContext.toIdentifier(tablePath.getLast()));
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
    Function<String, String> tableNameModifier =
        s -> s + EXPORT_SUFFIX + exportTableCounter.incrementAndGet();
    var stageAnalysis = getSourceSinkStageAnalysis();
    ExportNode exportNode;

    ObjectIdentifier sinkTableId = scriptContext.toIdentifier(sinkPath);
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
          new ExportNode(
              stageAnalysis,
              sinkPath,
              sqrlEnv.currentBatch(),
              Optional.of(exportStage),
              Optional.empty());
    } else if (sqrlEnv.getTableLookup().lookupSourceTable(sinkTableId) != null) {
      // 2nd: the sink path resolves to a table source (i.e. CREATE TABLE)
      exportNode =
          new ExportNode(
              stageAnalysis,
              sinkPath,
              sqrlEnv.currentBatch(),
              Optional.empty(),
              Optional.of(sinkTableId));
    } else { // the export is to a user-defined sink: load it
      var module = scriptContext.moduleLoader.getModule(sinkPath.popLast()).orElse(null);
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

      var flinkTable = ExternalFlinkTable.fromNamespaceObject(sinkTable, errorCollector);
      AtomicReference<RelDataType> exportSchemaRef = new AtomicReference<>(null);
      SchemaLoader schemaLoader =
          (tableName, schemaReference, tableProps) -> {
            var exportSchema = schemaReference.equalsIgnoreCase(".");
            if (schemaReference.equalsIgnoreCase("*") || exportSchema) {
              if (exportSchema) exportSchemaRef.set(inputNode.getTableAnalysis().getRowType());
              return Optional.of(
                  new SchemaConversionResult(inputNode.getTableAnalysis().getRowType(), Map.of()));
            }
            return flinkTable.schemaLoader.loadSchema(tableName, schemaReference, tableProps);
          };

      AddTableResult addTableResult;
      try {
        addTableResult =
            sqrlEnv.addExternalExport(tableNameModifier, flinkTable.sqlCreateTable, schemaLoader);
        var tableId = addTableResult.baseTableIdentifier();
        exportNode =
            new ExportNode(
                stageAnalysis,
                sinkPath,
                sqrlEnv.currentBatch(),
                Optional.empty(),
                Optional.of(tableId));
      } catch (Throwable e) {
        throw flinkTable.errorCollector.handle(e);
      }
      if (exportSchemaRef.get() != null) {
        FlinkTableBuilder tblBuilder = FlinkTableBuilder.toBuilder(addTableResult.createdTable());
        tblBuilder.addColumns(exportSchemaRef.get());
        flinkTable.writeExportWithSchema(tblBuilder);
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
  public record ExternalFlinkTable(
      String sqlCreateTable,
      SchemaLoader schemaLoader,
      Path sourcePath,
      ErrorCollector errorCollector) {

    public static ExternalFlinkTable fromNamespaceObject(
        FlinkTableNamespaceObject nsObject, ErrorCollector errorCollector) {
      var flinkTable = nsObject.table();

      // Parse SQL
      var tableSql = flinkTable.flinkSql();
      var tableError = errorCollector.withScript(flinkTable.flinkSqlFile(), tableSql);
      tableSql = SqlScriptStatementSplitter.formatEndOfSqlFile(tableSql);

      return new ExternalFlinkTable(
          tableSql, nsObject.schemaLoader(), flinkTable.flinkSqlFile(), tableError);
    }

    public void writeExportWithSchema(FlinkTableBuilder table) {
      String exportFilename =
          StringUtil.removeFromEnd(
                  sourcePath.getFileName().toString(), ModuleLoaderImpl.TABLE_FILE_SUFFIX)
              + "_export";
      TableWriter.writeToFile(sourcePath.getParent(), exportFilename, table);
    }
  }

  record HintsAndDocs(PlannerHints hints, Optional<String> documentation) {

    public static final HintsAndDocs EMPTY = new HintsAndDocs(PlannerHints.EMPTY, Optional.empty());
  }

  private record ScriptContext(
      ModuleLoader moduleLoader, String databaseName, boolean generateAccess) {

    ScriptContext fromImport(ModuleLoader moduleLoader, boolean isInline, String databaseName) {
      return isInline
          ? new ScriptContext(moduleLoader, this.databaseName, generateAccess)
          : new ScriptContext(moduleLoader, databaseName, false);
    }

    boolean hasDifferentDatabase(ScriptContext other) {
      return !databaseName.equals(other.databaseName);
    }

    public ObjectIdentifier toIdentifier(String name) {
      return ObjectIdentifier.of(FLINK_DEFAULT_CATALOG, this.databaseName, name);
    }

    public ObjectIdentifier toIdentifier(NamePath namePath) {
      var size = namePath.size();
      if (size == 0 || size > 3) {
        return null;
      }

      var catalogName = size > 2 ? namePath.get(0).getDisplay() : FLINK_DEFAULT_CATALOG;
      var databaseName = size > 1 ? namePath.get(size - 2).getDisplay() : this.databaseName;
      var tableName = namePath.get(size - 1).getDisplay();

      return ObjectIdentifier.of(catalogName, databaseName, tableName);
    }

    public ObjectIdentifier toIdentifier(Name name) {
      return toIdentifier(name.getDisplay());
    }
  }
}
