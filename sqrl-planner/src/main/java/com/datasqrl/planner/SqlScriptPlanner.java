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
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.log.MutationEngine;
import com.datasqrl.engine.log.MutationEngine.MutationSettings;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.engine.stream.flink.FlinkEngineFactory;
import com.datasqrl.engine.stream.flink.FlinkSqlNodes;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.io.schema.SchemaConversionResult;
import com.datasqrl.loaders.FlinkTableNamespaceObject;
import com.datasqrl.loaders.FlinkTableNamespaceObject.FlinkTable;
import com.datasqrl.loaders.IncludedScript;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.loaders.ModuleLoaders;
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
import com.datasqrl.planner.dag.nodes.PipelineNode;
import com.datasqrl.planner.dag.nodes.TableFunctionNode;
import com.datasqrl.planner.dag.nodes.TableNode;
import com.datasqrl.planner.dag.plan.MutationMetadataExtractor;
import com.datasqrl.planner.dag.plan.MutationTable;
import com.datasqrl.planner.hint.CacheHint;
import com.datasqrl.planner.hint.EngineHint;
import com.datasqrl.planner.hint.HintsAndDoc;
import com.datasqrl.planner.hint.MutationInsertHint;
import com.datasqrl.planner.hint.NoQueryHint;
import com.datasqrl.planner.hint.PlannerHints;
import com.datasqrl.planner.hint.QueryByAnyHint;
import com.datasqrl.planner.hint.TestHint;
import com.datasqrl.planner.hint.TtlHint;
import com.datasqrl.planner.parser.AccessModifier;
import com.datasqrl.planner.parser.FlinkSQLStatement;
import com.datasqrl.planner.parser.NoLocationStatementParserException;
import com.datasqrl.planner.parser.ParsePosUtil;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.ParsedStatement;
import com.datasqrl.planner.parser.SQLStatement;
import com.datasqrl.planner.parser.ScriptStatement;
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
import com.datasqrl.planner.parser.UnparsedStatement;
import com.datasqrl.planner.tables.AccessVisibility;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import com.datasqrl.planner.tables.SourceSinkTableAnalysis;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.planner.util.SqlScriptPlannerUtil;
import com.datasqrl.planner.util.SqlScriptWriter;
import com.datasqrl.planner.util.SqlTableNameExtractor;
import com.datasqrl.server.MutationInsertType;
import com.datasqrl.util.FunctionUtil;
import com.google.common.base.Preconditions;
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
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.lang3.Strings;
import org.apache.flink.sql.parser.ddl.catalog.SqlCreateCatalog;
import org.apache.flink.sql.parser.ddl.table.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.table.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.table.SqlDropTable;
import org.apache.flink.sql.parser.ddl.view.SqlAlterView;
import org.apache.flink.sql.parser.ddl.view.SqlAlterViewAs;
import org.apache.flink.sql.parser.ddl.view.SqlCreateView;
import org.apache.flink.sql.parser.ddl.view.SqlDropView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
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

  private static final String INSERT_SUFFIX = "_in";
  private static final String EXPORT_SUFFIX = "_ex";
  private static final String ACCESS_FUNCTION_SUFFIX = "__access";

  private final AtomicInteger exportTableCounter = new AtomicInteger(0);
  private final AtomicInteger insertTableCounter = new AtomicInteger(0);

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

  /** Manages the context of the script that is processed, adjusted for imports */
  private ScriptContext scriptContext;

  /** The hints and doc string of the last table definition, which column expressions extend */
  private HintsAndDoc tableDefinitionHintsAndDoc = HintsAndDoc.EMPTY;

  public SqlScriptPlanner(
      ErrorCollector errorCollector,
      ModuleLoaders moduleLoaders,
      SqrlStatementParser sqrlParser,
      PackageJson packageJson,
      ExecutionPipeline pipeline,
      ExecutionGoal executionGoal) {
    this.errorCollector = errorCollector;
    this.completeScript = new SqlScriptWriter();
    this.scriptContext =
        new ScriptContext(
            moduleLoaders, FLINK_DEFAULT_DATABASE, true, Optional.empty(), new FailedObjects());
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
   * Main entry method for parsing a SQRL script. The bulk of this method ensures that exceptions
   * and errors are correctly mapped to the source so that users can easily understand what the
   * issue is and what's causing it.
   *
   * @param mainScript SQRL script to plan
   * @param inheritedHints inherited hints if the planned script is coming from an IMPORT
   * @param sqrlEnv the SQRL compilation environment
   */
  public void planMain(
      MainScript mainScript,
      Optional<PlannerHints> inheritedHints,
      Sqrl2FlinkSQLTranslator sqrlEnv) {

    var scriptErrors = errorCollector.withScript(mainScript.getPath(), mainScript.getContent());
    var parsedScript = sqrlParser.parseScript(mainScript.getContent(), scriptErrors);
    var failures = scriptContext.failures();
    List<StackableStatement> statementStack = new ArrayList<>();
    // A statement that fails to parse or plan is skipped and the rest are still planned, so that
    // all errors are reported. The script fails with the first one after the loop
    RuntimeException firstError = parsedScript.firstError().orElse(null);

    for (ScriptStatement scriptStatement : parsedScript.statements()) {
      if (scriptStatement instanceof UnparsedStatement unparsed) {
        // Its error is already reported. Like a statement that failed to plan, it ends the
        // statement stack, unless it adds a column
        var objects = StatementObjects.ofUnparsed(unparsed.source().get());
        failures.record(objects);
        if (!objects.addsColumn()) {
          statementStack = new ArrayList<>();
        }
        continue;
      }
      var sourceStmt = (ParsedStatement) scriptStatement;
      var statement = sourceStmt.statement();
      var lineErrors = scriptErrors.atFile(statement.getFileLocation());
      var sqlStatement = statement.get();
      var objects = StatementObjects.of(sqlStatement, sqrlEnv::parseSQL);

      if (failures.isContextFailed()
          || failures.isDependent(objects, path -> isDefined(path, sqrlEnv))) {
        // Its errors would only be knock-on errors of a failed object or USE statement, which is
        // already reported
        failures.record(objects);
        statementStack = stackAfterFailure(sqlStatement, statementStack);
        continue;
      }
      var failuresBefore = failures.mark();
      try {
        planStatement(sqlStatement, statementStack, inheritedHints, sqrlEnv, lineErrors);
      } catch (Throwable e) {
        var error = recordFailure(e, statement, objects, failuresBefore, lineErrors);
        if (firstError == null) {
          firstError = error.orElse(null);
        }
        statementStack = stackAfterFailure(sqlStatement, statementStack);
        continue;
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

    if (firstError != null) {
      throw firstError;
    }
  }

  /**
   * Records a statement that failed to plan. Its error is reported, unless it is a knock-on error
   * of an object that failed, whose error is already reported.
   *
   * @param failuresBefore the mark of the failures before the statement was planned
   * @param errors the errors at the location of the statement
   * @return the reported error, or empty for a knock-on error
   */
  private Optional<RuntimeException> recordFailure(
      Throwable e,
      ParsedObject<SQLStatement> statement,
      StatementObjects objects,
      int failuresBefore,
      ErrorCollector errors) {
    if (e instanceof VirtualMachineError vmError) {
      // E.g. out of memory, so stop planning instead of planning the remaining statements
      throw vmError;
    }
    var failures = scriptContext.failures();
    if (!(e instanceof CollectedException) && failures.isKnockOnError(e)) {
      failures.record(objects);
      return Optional.empty();
    }

    var error = reportStatementError(e, statement, errors);
    if (!failures.recordedSince(failuresBefore)) {
      // Unless planning it recorded which of its objects failed, e.g. those of an imported script
      failures.record(objects);
    }
    return Optional.of(error);
  }

  /**
   * Reports an error of planning a statement at its position in the script.
   *
   * @param errors the errors at the location of the statement
   * @return the reported error
   */
  private static RuntimeException reportStatementError(
      Throwable e, ParsedObject<SQLStatement> statement, ErrorCollector errors) {
    var sqlStatement = statement.get();
    if (e instanceof NoLocationStatementParserException noLocation) {
      var location =
          sqlStatement instanceof SqrlStatement sqrlStmt
              ? sqrlStmt.getDefaultLocation()
              : statement.getFileLocation();
      return errors.handle(new StatementParserException(location, noLocation));
    }
    return ParsePosUtil.reportError(errors, e, sqlStatement::mapSqlLocation);
  }

  /** Whether a table, as written in a statement (e.g. {@code sources.Orders}), is defined */
  private boolean isDefined(List<String> path, Sqrl2FlinkSQLTranslator sqrlEnv) {
    var identifier = scriptContext.toIdentifier(NamePath.of(path.toArray(String[]::new)));
    return identifier != null
        && (dagBuilder.getNode(identifier).isPresent()
            || sqrlEnv.getTableLookup().lookupSourceTable(identifier) != null);
  }

  /**
   * The statement stack after a statement that failed or was skipped. A column keeps it, so that
   * the next column of the table still directly follows its definition, while any other statement
   * ends it, so that a column after it does not.
   */
  private static List<StackableStatement> stackAfterFailure(
      SQLStatement statement, List<StackableStatement> statementStack) {
    return statement instanceof StackableStatement stackable && !stackable.isRoot()
        ? statementStack
        : new ArrayList<>();
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
      Optional<PlannerHints> inheritedHints,
      Sqrl2FlinkSQLTranslator sqrlEnv,
      ErrorCollector errors) {
    var hintsAndDocs = getStatementHintsAndDoc(stmt, inheritedHints, sqrlEnv, errors);
    var hints = hintsAndDocs.hints();
    if (stmt instanceof SqrlImportStatement statement) {
      addImport(statement, hintsAndDocs, sqrlEnv, errors);
    } else if (stmt instanceof SqrlExportStatement statement) {
      addExport(statement, sqrlEnv, errors);
    } else if (stmt instanceof SqrlCreateTableStatement statement) {
      sqrlEnv
          .createTable(
              statement.toSql(),
              getMutationBuilder(hintsAndDocs),
              scriptContext.mainModuleLoader().getSchemaLoader(),
              hintsAndDocs,
              errors)
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

      // The deployment of a standalone include writes the subscriptions that it exposes, also when
      // this project does not expose them, e.g. for a script that it does not import inline
      var subscribingInclude =
          !isHidden && access == AccessModifier.SUBSCRIPTION
              ? scriptContext
                  .includedScript()
                  .filter(IncludedScript::exposesSubscriptions)
                  .map(IncludedScript::include)
              : Optional.<String>empty();

      // Ignore any hidden table
      access = adjustAccess(isHidden ? AccessModifier.NONE : access);
      // Tables in the API must declare their tags, but test tables are never deployed
      var mustDeclareTags = !isHidden && !hints.isTest();

      var originalSql = sqrlDef.toSql(sqrlEnv, statementStack);
      // Relationships and Table functions require special handling
      if (sqrlDef instanceof SqrlTableFunctionStatement tblFnStmt) {
        var identifier = scriptContext.toIdentifier(tblFnStmt.getPath().getFirst());
        final var arguments = new LinkedHashMap<Name, ParsedArgument>();
        if (!tblFnStmt.getSignature().isEmpty()) {
          var parsedArgs =
              sqrlEnv.getRelDataTypeParser().parseToRelDataType(tblFnStmt.getSignature());
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
                  hintsAndDocs,
                  mustDeclareTags && !tblFnStmt.isRelationship(),
                  errors);
        } else {
          fnBuilder =
              sqrlEnv.resolveSqrlTableFunction(
                  identifier,
                  originalSql,
                  new ArrayList<>(arguments.values()),
                  argumentIndexMap,
                  hintsAndDocs,
                  mustDeclareTags && !tblFnStmt.isRelationship(),
                  errors);
        }
        fnBuilder.fullPath(tblFnStmt.getPath());
        var visibility =
            new AccessVisibility(
                access, hints.isTest(), tblFnStmt.isRelationship() || passthroughFn, isHidden);
        fnBuilder.visibility(visibility);
        fnBuilder.documentation(hintsAndDocs.getDocumentation());
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
        // The log engine serves a subscription function from the table that it reads
        markSubscribedBy(
            subscribingInclude,
            fn.getFunctionAnalysis().getFromTables().stream()
                .flatMap(table -> dagBuilder.getNode(table.getIdentifier()).stream()));
      } else {
        var visibility = new AccessVisibility(access, hints.isTest(), true, isHidden);

        if (!shouldExcludeTestTable(hints)) {
          addViewToDag(
              originalSql,
              hintsAndDocs,
              mustDeclareTags,
              visibility,
              subscribingInclude,
              errors,
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
      var node = sqrlEnv.parseSQL(flinkStmt.sql().get());
      if (node instanceof SqlCreateView || node instanceof SqlAlterViewAs) {
        // plan like other definitions from above, but a Flink SQL statement has no doc string to
        // declare its tags in, so the view only inherits them
        var visibility =
            new AccessVisibility(adjustAccess(AccessModifier.QUERY), false, true, false);
        addViewToDag(
            flinkStmt.sql().get(),
            hintsAndDocs,
            false,
            visibility,
            Optional.empty(),
            errors,
            sqrlEnv);
      } else if (node instanceof SqlCreateTable) {
        sqrlEnv
            .createTable(
                flinkStmt.sql().get(),
                getMutationBuilder(hintsAndDocs),
                scriptContext.mainModuleLoader().getSchemaLoader(),
                hintsAndDocs,
                errors)
            .ifPresent(tableAnalysis -> addSourceToDag(tableAnalysis, hintsAndDocs, sqrlEnv));
      } else if (node instanceof RichSqlInsert insert) {
        addInsert(insert, hintsAndDocs, sqrlEnv, errors);
      } else if (node instanceof SqlAlterTable || node instanceof SqlAlterView) {
        errors.fatal(
            "Renaming or altering tables is not supported. Rename them directly in the script or IMPORT AS.");
      } else if (node instanceof SqlDropTable || node instanceof SqlDropView) {
        errors.fatal(
            "Removing tables is not supported. The DAG planner automatically removes unused tables.");
      } else if (node instanceof SqlCreateCatalog && scriptContext.isRootContext()) {
        errors.fatal(
            "Catalog creation is not supported in the main script or in a script imported inline.");
      } else {
        // just pass through
        sqrlEnv.executeSQL(flinkStmt.sql().get());
      }
    }
  }

  /**
   * The hints and doc string of a statement. A column expression extends the table definition it
   * follows, so it keeps the hints and doc string of the definition.
   */
  private HintsAndDoc getStatementHintsAndDoc(
      SQLStatement stmt,
      Optional<PlannerHints> inheritedHints,
      Sqrl2FlinkSQLTranslator sqrlEnv,
      ErrorCollector errors) {

    var hintsAndDoc = HintsAndDoc.EMPTY;
    if (stmt instanceof SqrlAddColumnStatement statement) {
      hintsAndDoc =
          getHintsAndDoc(
                  statement, Optional.of(tableDefinitionHintsAndDoc.hints()), sqrlEnv, errors)
              .updateDocsIfAbsent(tableDefinitionHintsAndDoc.doc());

    } else if (stmt instanceof SqrlStatement statement) {
      hintsAndDoc = getHintsAndDoc(statement, inheritedHints, sqrlEnv, errors);
    }

    if (stmt instanceof StackableStatement) {
      tableDefinitionHintsAndDoc = hintsAndDoc;
    }

    return hintsAndDoc;
  }

  /**
   * Adds a view to Flink and its table to the DAG. If the table fails before it is in the DAG, its
   * view is removed again, so that the statements that use it fail as knock-on errors instead of
   * being planned on a table that is not in the DAG.
   */
  private void addViewToDag(
      String viewSql,
      HintsAndDoc hintsAndDoc,
      boolean mustDeclareTags,
      AccessVisibility visibility,
      Optional<String> subscribingInclude,
      ErrorCollector errors,
      Sqrl2FlinkSQLTranslator sqrlEnv) {

    var tableAnalysis = sqrlEnv.addView(viewSql, hintsAndDoc, mustDeclareTags, errors);
    try {
      addTableToDag(tableAnalysis, hintsAndDoc, visibility, subscribingInclude, false, sqrlEnv);
    } catch (RuntimeException e) {
      // Unlike a new table, a column replaces the view of a table that is in the DAG
      if (dagBuilder.getNode(tableAnalysis.getIdentifier()).isEmpty()) {
        sqrlEnv.removeFailedView(tableAnalysis.getObjectIdentifier(), e);
      }
      throw e;
    }
  }

  /** Parses the hints and doc string of a statement and validates the tags in the doc string. */
  private HintsAndDoc getHintsAndDoc(
      SqrlStatement statement,
      Optional<PlannerHints> inheritedHints,
      Sqrl2FlinkSQLTranslator sqrlEnv,
      ErrorCollector errors) {

    var comments = statement.getComments();
    var hints = PlannerHints.from(comments, inheritedHints, errors);
    var documentation =
        comments.documentation().isEmpty()
            ? Optional.<String>empty()
            : Optional.of(
                comments.documentation().stream()
                    .map(ParsedObject::get)
                    .map(String::trim)
                    .collect(Collectors.joining("\n")));

    documentation.ifPresent(doc -> sqrlEnv.getTagAnalyzer().validateTags(doc, errors));

    return new HintsAndDoc(hints, documentation);
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
    if (!scriptContext.isRootContext()
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
    Map<ExecutionStage, StageAnalysis> stageAnalysis = new LinkedHashMap<>();
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
      TableAnalysis tableAnalysis, HintsAndDoc hintsAndDoc, Sqrl2FlinkSQLTranslator sqrlEnv) {
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
    addTableToDag(tableAnalysis, hintsAndDoc, visibility, Optional.empty(), true, sqrlEnv);
  }

  /**
   * Adds a table to the DAG and plans the table access function based on the determined visibility
   * and provided hints.
   *
   * @param tableAnalysis
   * @param hintsAndDoc
   * @param visibility
   * @param subscribingInclude the standalone include whose script declares the table as a
   *     subscription, or empty
   * @param sqrlEnv
   */
  private void addTableToDag(
      TableAnalysis tableAnalysis,
      HintsAndDoc hintsAndDoc,
      AccessVisibility visibility,
      Optional<String> subscribingInclude,
      boolean isSource,
      Sqrl2FlinkSQLTranslator sqrlEnv) {
    // Sources must be processed by the stream stage
    var availableStages =
        isSource ? List.of(streamStage) : determineStages(tableStages, hintsAndDoc.hints());
    var tableNode =
        new TableNode(
            tableAnalysis,
            isSource
                ? getSourceSinkStageAnalysis()
                : getStageAnalysis(tableAnalysis, availableStages));
    dagBuilder.add(tableNode);

    // Figure out if and what type of access function we should add for this table
    var queryByHint = hintsAndDoc.hints().getQueryByHint();
    if (queryByHint.filter(NoQueryHint.class::isInstance).isEmpty()) {
      // Without an access function, the include does not subscribe to the table either
      markSubscribedBy(subscribingInclude, Stream.of(tableNode));
    }
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
      fnBuilder.documentation(hintsAndDoc.getDocumentation());
      fnBuilder.cacheDuration(getCacheDuration(hintsAndDoc));
      addFunctionToDag(
          fnBuilder.build(), hintsAndDoc.dropHints()); // hints don't apply to the function access
    } else if (queryByHint.isPresent() && !(queryByHint.get() instanceof NoQueryHint)) {
      throw new StatementParserException(
          ErrorLabel.GENERIC,
          queryByHint.get().getSource().getFileLocation(),
          "query_by hints are only supported on tables that are queryable");
    }
  }

  private void addFunctionToDag(SqrlTableFunction function, HintsAndDoc hintsAndDoc) {
    var availableStages =
        determineStages(
            determineViableStages(function.getVisibility().access()), hintsAndDoc.hints());
    dagBuilder.add(
        new TableFunctionNode(
            function, getStageAnalysis(function.getFunctionAnalysis(), availableStages)));
  }

  private static Duration getCacheDuration(HintsAndDoc hintsAndDoc) {
    return hintsAndDoc
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
      HintsAndDoc hintsAndDoc,
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

    var alias = Optional.ofNullable(aliasPath).map(NamePath::getFirst);

    var loadedModule =
        scriptContext
            .moduleLoaders()
            .loadImportModule(path, importStmt.getPackageIdentifier().getFileLocation());
    var module = loadedModule.module();
    var finalPath = loadedModule.finalPath();

    if (isStar) {
      if (module.getNamespaceObjects().isEmpty()) {
        errors.warn("Module is empty: %s", path);
      }

      // For multiple imports, the alias serves as a prefix.
      Function<String, String> nameModifier =
          alias.isPresent() ? name -> alias.get() + name : Function.identity();
      var failures = scriptContext.failures();
      RuntimeException firstError = null;
      for (var namespaceObject : module.getNamespaceObjects()) {
        var failuresBefore = failures.mark();
        try {
          addImport(namespaceObject, nameModifier, hintsAndDoc, sqrlEnv);
        } catch (RuntimeException e) {
          // Import the other objects, so that the statements that use them are still planned
          if (!failures.recordedSince(failuresBefore)) {
            failures.recordImported(nameModifier.apply(namespaceObject.name().getDisplay()));
          }
          var error = ParsePosUtil.reportError(errors, e, UnaryOperator.identity());
          if (firstError == null) {
            firstError = error;
          }
        }
      }
      if (firstError != null) {
        throw firstError;
      }
    } else {
      var namespaceObject = module.getNamespaceObject(finalPath.getLast());
      errors.checkFatal(
          namespaceObject.isPresent(),
          "Object [%s] not found in module: %s",
          finalPath.getLast(),
          finalPath);

      addImport(
          namespaceObject.get(),
          alias.isPresent() ? (name -> alias.get().getDisplay()) : Function.identity(),
          hintsAndDoc,
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
      HintsAndDoc hintsAndDoc,
      Sqrl2FlinkSQLTranslator sqrlEnv) {
    if (nsObject instanceof FlinkTableNamespaceObject object) { // import a table
      var flinkTable = ExternalFlinkTable.fromNamespaceObject(object, errorCollector);
      var importedHintsAndDoc =
          object
              .table()
              .sqrlStatement()
              .map(
                  statement ->
                      getHintsAndDoc(
                          statement,
                          Optional.of(hintsAndDoc.hints()),
                          sqrlEnv,
                          flinkTable.errorCollector()))
              .orElse(hintsAndDoc);
      try {
        var tableAnalysis =
            sqrlEnv.createTableWithSchema(
                importNameModifier,
                flinkTable.sqlCreateTable,
                flinkTable.schemaLoader(),
                getMutationBuilder(
                    importedHintsAndDoc,
                    scriptContext.isRootContext(),
                    getStandaloneInclude(object.table())),
                importedHintsAndDoc,
                flinkTable.errorCollector());
        importedHintsAndDoc.hints().updateColumnNamesHints(tableAnalysis::getField);
        addSourceToDag(tableAnalysis, importedHintsAndDoc, sqrlEnv);
        completeScript.append(tableAnalysis.getOriginalSql());
      } catch (Throwable e) {
        throw ParsePosUtil.reportError(flinkTable.errorCollector, e, UnaryOperator.identity());
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
      var script = scriptObject.getScript();
      scriptContext =
          priorContext.fromImport(
              scriptObject.getModuleLoader(),
              scriptObject.isInline(),
              importNameModifier.apply(scriptObject.name().getDisplay()),
              script.getPath());

      if (priorContext.hasDifferentDatabase(scriptContext)) {
        // Switch DB
        var stmts = sqrlEnv.setDatabase(scriptContext.databaseName(), false);
        completeScript.append(stmts);
      }

      var hints =
          hintsAndDoc.hints().isEmpty()
              ? Optional.<PlannerHints>empty()
              : Optional.of(hintsAndDoc.hints());
      try {
        planMain(script, hints, sqrlEnv);
      } finally {
        // Restore the context even on errors, since planning continues with the next statement
        if (priorContext.hasDifferentDatabase(scriptContext)) {
          // Switch it back
          var stmts = sqrlEnv.setDatabase(priorContext.databaseName(), true);
          completeScript.append(stmts);
        }
        scriptContext = priorContext;
      }
    } else {
      throw new UnsupportedOperationException("Unexpected object imported: " + nsObject);
    }
  }

  private Optional<String> getStandaloneInclude(FlinkTable table) {
    return scriptContext.moduleLoaders().getStandaloneInclude(table.scriptPath());
  }

  /** Provides connector settings for CREATE TABLE statements managed by a configured engine. */
  private Optional<MutationBuilder> getMutationBuilder(HintsAndDoc hintsAndDoc) {
    return getMutationBuilder(
        hintsAndDoc,
        scriptContext.isRootContext(),
        scriptContext.includedScript().map(IncludedScript::include));
  }

  private Optional<ExecutionStage> getMutationStage(HintsAndDoc hintsAndDoc) {
    return hintsAndDoc
        .hints()
        .getHint(EngineHint.class)
        .flatMap(engineHint -> pipeline.getStage(engineHint.getStageNames().get(0)));
  }

  /**
   * Records that the deployment of a standalone include subscribes to the given tables, so that the
   * DAG planner reads them where that deployment writes them, instead of writing them again.
   */
  private static void markSubscribedBy(
      Optional<String> standaloneInclude, Stream<? extends PipelineNode> tables) {

    standaloneInclude.ifPresent(
        include ->
            tables
                .filter(TableNode.class::isInstance)
                .forEach(table -> ((TableNode) table).markSubscribedBy(include)));
  }

  /**
   * Returns whether the deployment of the standalone include that the current script belongs to
   * runs a statement of the script that writes into a sink that the deployments share, and reports
   * a notice if so. The current project then skips the statement, so that the sink is not written
   * twice.
   */
  private boolean isRunByStandaloneInclude(boolean sharedSink, ErrorCollector errors) {
    var include =
        scriptContext
            .includedScript()
            .filter(script -> sharedSink && script.runsStatements())
            .map(IncludedScript::include);

    include.ifPresent(
        name ->
            errors.notice(
                "This project skips the statement, since the deployment of the standalone include"
                    + " '%s' runs it and writes into the same sink",
                name));

    return include.isPresent();
  }

  /** The deployments share the tables of an engine that supports it, e.g. Kafka topics. */
  private static boolean isShared(ExecutionStage stage) {
    return stage.supportsFeature(EngineFeature.SHARED_WITH_STANDALONE_INCLUDES);
  }

  /**
   * The deployments share an external table, i.e. a table with a connector, and an internal table
   * of an engine whose tables they share.
   */
  private static boolean isShared(SourceSinkTableAnalysis table) {
    var mutation = table.mutationDefinition();
    return mutation == null || isShared(mutation.getStage());
  }

  private Optional<MutationBuilder> getMutationBuilder(
      HintsAndDoc hintsAndDoc, boolean generateAccess, Optional<String> standaloneInclude) {
    var mutationStage = getMutationStage(hintsAndDoc);

    if (mutationStage.isEmpty()) {
      return Optional.empty();
    }

    if (!(mutationStage.get().engine() instanceof MutationEngine mutationEngine)) {
      throw new StatementParserException(
          "Configured engine %s is not a mutation engine required for CREATE TABLE statements"
              .formatted(mutationStage));
    }

    return Optional.of(
        (origTableName, tableBuilder, dataType) -> {
          var mutationBuilder = MutationTable.builder();
          mutationBuilder.generateAccess(generateAccess);
          mutationBuilder.stage(mutationStage.get());
          MutationInsertType insertType =
              hintsAndDoc
                  .hints()
                  .getHint(MutationInsertHint.class)
                  .map(MutationInsertHint::getInsertType)
                  .orElse(MutationInsertType.SINGLE);
          mutationBuilder.partitionKey(tableBuilder.getPartition());
          mutationBuilder.createTable(
              mutationEngine.createMutation(
                  mutationStage.get(),
                  origTableName,
                  tableBuilder,
                  dataType,
                  new MutationSettings(
                      insertType,
                      hintsAndDoc.hints().getHint(TtlHint.class).flatMap(TtlHint::getTtl),
                      standaloneInclude)));
          mutationBuilder.name(Name.system(origTableName));
          mutationBuilder.insertType(insertType);
          mutationBuilder.documentation(hintsAndDoc.getDocumentation());
          mutationBuilder.tableBuilder(tableBuilder);
          // UUID and TIMESTAMP are special cases
          tableBuilder
              .extractMetadataColumns(new MutationMetadataExtractor())
              .forEach(mutationBuilder::computedColumn);
          return mutationBuilder;
        });
  }

  /**
   * Plans EXPORT statements
   *
   * @param exportStmt
   * @param sqrlEnv
   * @param errors the errors of the statement
   */
  private void addExport(
      SqrlExportStatement exportStmt, Sqrl2FlinkSQLTranslator sqrlEnv, ErrorCollector errors) {
    var sinkPath = exportStmt.getPackageIdentifier().get();
    var sinkName = sinkPath.getLast();
    var tablePath = exportStmt.getTableIdentifier().get();

    // Lookup the table that is being exported
    var tableNode = dagBuilder.getNode(scriptContext.toIdentifier(tablePath));
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

    TableAnalysis existingTable;
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
      if (isRunByStandaloneInclude(isShared(exportStage), errors)) {
        return;
      }
      exportNode =
          new ExportNode(
              stageAnalysis,
              sinkPath,
              sqrlEnv.currentBatch(),
              Optional.of(exportStage),
              Optional.empty(),
              Optional.empty());
    } else if ((existingTable = sqrlEnv.getTableLookup().lookupSourceTable(sinkTableId)) != null) {
      // 2nd: the sink path resolves to a table source (i.e. CREATE TABLE)
      if (isRunByStandaloneInclude(
          existingTable.getSourceSinkTable().map(SqlScriptPlanner::isShared).orElse(false),
          errors)) {
        return;
      }
      sqrlEnv.checkWriteTags(sinkTableId, List.of(inputNode.getTableAnalysis()), errors);
      exportNode =
          new ExportNode(
              stageAnalysis,
              sinkPath,
              sqrlEnv.currentBatch(),
              Optional.empty(),
              Optional.of(sinkTableId),
              existingTable.getSourceSinkTable());
    } else { // the export is to a user-defined sink: load it
      var moduleCtx =
          scriptContext
              .moduleLoaders()
              .loadExportModule(sinkPath, exportStmt.getPackageIdentifier().getFileLocation());
      var module = moduleCtx.module();
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
      var sinkHintsAndDoc =
          sinkTable
              .table()
              .sqrlStatement()
              .map(
                  statement ->
                      getHintsAndDoc(
                          statement, Optional.empty(), sqrlEnv, flinkTable.errorCollector()))
              .orElse(HintsAndDoc.EMPTY);
      var mutationBuilder =
          sinkTable.table().external()
              ? Optional.<MutationBuilder>empty()
              : getMutationBuilder(sinkHintsAndDoc, false, getStandaloneInclude(sinkTable.table()));
      flinkTable
          .errorCollector()
          .checkFatal(
              sinkTable.table().external() || mutationBuilder.isPresent(),
              ErrorCode.INVALID_INDIVIDUAL_SCRIPT_TABLE,
              "Referenced table '%s' must specify a connector or an engine hint",
              sinkName);
      // Checked before the sink is added to Flink
      var sharedSink =
          sinkTable.table().external()
              || getMutationStage(sinkHintsAndDoc).map(SqlScriptPlanner::isShared).orElse(false);
      if (isRunByStandaloneInclude(sharedSink, errors)) {
        return;
      }
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
            sqrlEnv.addExternalExport(
                tableNameModifier, flinkTable.sqlCreateTable, schemaLoader, mutationBuilder);
        var tableId = addTableResult.baseTableIdentifier();
        exportNode =
            new ExportNode(
                stageAnalysis,
                sinkPath,
                sqrlEnv.currentBatch(),
                Optional.empty(),
                Optional.of(tableId),
                addTableResult.tableAnalysis().getSourceSinkTable());
      } catch (Throwable e) {
        throw ParsePosUtil.reportError(flinkTable.errorCollector, e, UnaryOperator.identity());
      }
      if (exportSchemaRef.get() != null) {
        FlinkTableBuilder tblBuilder = FlinkTableBuilder.toBuilder(addTableResult.createdTable());
        tblBuilder.addColumns(exportSchemaRef.get());
        flinkTable.writeExportWithSchema(tblBuilder);
      }
    }
    dagBuilder.addExport(exportNode, inputNode);
  }

  private void addInsert(
      RichSqlInsert insert,
      HintsAndDoc hintsAndDocs,
      Sqrl2FlinkSQLTranslator sqrlEnv,
      ErrorCollector errors) {

    var sinkTableId = sqrlEnv.getInsertTarget(insert);
    var sinkTable = sqrlEnv.getTableLookup().lookupSourceTable(sinkTableId);
    errors.checkFatal(sinkTable != null, "Could not find INSERT target table: %s", sinkTableId);

    if (isRunByStandaloneInclude(
        sinkTable.getSourceSinkTable().map(SqlScriptPlanner::isShared).orElse(false), errors)) {
      return;
    }

    var inputTable =
        sqrlEnv.analyzeInsertQuery(
            insert.getSource(),
            scriptContext.toIdentifier(
                sinkTableId.getObjectName() + INSERT_SUFFIX + insertTableCounter.incrementAndGet()),
            hintsAndDocs,
            errors);
    sqrlEnv.checkWriteTags(sinkTableId, inputTable.getFromTables(), errors);
    FlinkSqlNodes.qualifyIdentifier(
        (SqlIdentifier) insert.getTargetTableID(), sinkTableId.toList());
    var inputNode =
        new TableNode(
            inputTable,
            getStageAnalysis(inputTable, determineStages(tableStages, hintsAndDocs.hints())));
    dagBuilder.add(inputNode);
    dagBuilder.addExport(
        new ExportNode(
            getSourceSinkStageAnalysis(),
            NamePath.of(sinkTableId.toList().toArray(String[]::new)),
            sqrlEnv.currentBatch(),
            Optional.empty(),
            Optional.of(sinkTableId),
            sinkTable.getSourceSinkTable(),
            Optional.of(insert)),
        inputNode);
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
      var originalTableSql = flinkTable.sql();
      var tableError =
          flinkTable
              .scriptContent()
              .flatMap(
                  scriptContent ->
                      flinkTable
                          .sourceLocation()
                          .map(
                              sourceLocation ->
                                  errorCollector
                                      .withScript(flinkTable.scriptPath(), scriptContent)
                                      .atFile(sourceLocation)))
              .orElseGet(
                  () -> errorCollector.withScript(flinkTable.scriptPath(), originalTableSql));
      var tableSql = SqlScriptStatementSplitter.formatEndOfSqlFile(originalTableSql);

      return new ExternalFlinkTable(
          tableSql, nsObject.schemaLoader(), flinkTable.scriptPath(), tableError);
    }

    public void writeExportWithSchema(FlinkTableBuilder table) {
      String exportFilename =
          Strings.CS.removeEnd(
                  sourcePath.getFileName().toString(), ModuleLoaderImpl.TABLE_FILE_SUFFIX)
              + "_export";
      TableWriter.writeToFile(sourcePath.getParent(), exportFilename, table);
    }
  }

  /**
   * Internal object that keeps the necessary context for the current script during planning.
   *
   * @param moduleLoaders loads modules related to the script
   * @param databaseName corresponding database name for the script context
   * @param isRootContext {@code true} if the context refers to the main script or an inline import,
   *     {@code false} for regular imports
   * @param includedScript the standalone included project that the script belongs to, with how its
   *     own deployment plans the script, or empty if the project owns the script's tables
   * @param failures the objects that failed, as seen from the script, whose knock-on errors are not
   *     reported
   */
  private record ScriptContext(
      ModuleLoaders moduleLoaders,
      String databaseName,
      boolean isRootContext,
      Optional<IncludedScript> includedScript,
      FailedObjects failures) {

    ScriptContext fromImport(
        ModuleLoader moduleLoader, boolean inline, String databaseName, Optional<Path> scriptPath) {

      var importLoaders = moduleLoaders.withMainLoader(moduleLoader);
      var includedScript = scriptPath.flatMap(moduleLoaders::getIncludedScript);

      return inline
          ? new ScriptContext(
              importLoaders, this.databaseName, isRootContext, includedScript, failures)
          : new ScriptContext(
              importLoaders,
              databaseName,
              false,
              includedScript,
              failures.forImportedScript(databaseName));
    }

    ModuleLoader mainModuleLoader() {
      return moduleLoaders.getMainLoader();
    }

    boolean hasDifferentDatabase(ScriptContext other) {
      return !databaseName.equals(other.databaseName);
    }

    ObjectIdentifier toIdentifier(String name) {
      return ObjectIdentifier.of(FLINK_DEFAULT_CATALOG, this.databaseName, name);
    }

    ObjectIdentifier toIdentifier(NamePath namePath) {
      var size = namePath.size();
      if (size == 0 || size > 3) {
        return null;
      }

      var catalogName = size > 2 ? namePath.get(0).getDisplay() : FLINK_DEFAULT_CATALOG;
      var databaseName = size > 1 ? namePath.get(size - 2).getDisplay() : this.databaseName;
      var tableName = namePath.get(size - 1).getDisplay();

      return ObjectIdentifier.of(catalogName, databaseName, tableName);
    }

    ObjectIdentifier toIdentifier(Name name) {
      return toIdentifier(name.getDisplay());
    }
  }
}
