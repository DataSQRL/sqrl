package com.datasqrl.flinkwrapper.planner;


import static com.datasqrl.flinkwrapper.parser.ParsePosUtil.convertPosition;
import static com.datasqrl.flinkwrapper.parser.StatementParserException.checkFatal;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.flinkwrapper.Sqrl2FlinkSQLTranslator;
import com.datasqrl.flinkwrapper.hint.PlannerHints;
import com.datasqrl.flinkwrapper.parser.FlinkSQLStatement;
import com.datasqrl.flinkwrapper.parser.ParsedObject;
import com.datasqrl.flinkwrapper.parser.SQLStatement;
import com.datasqrl.flinkwrapper.parser.SqlScriptStatementSplitter;
import com.datasqrl.flinkwrapper.parser.SqrlCreateTableStatement;
import com.datasqrl.flinkwrapper.parser.SqrlDefinition;
import com.datasqrl.flinkwrapper.parser.SqrlExportStatement;
import com.datasqrl.flinkwrapper.parser.SqrlImportStatement;
import com.datasqrl.flinkwrapper.parser.SqrlRelationshipStatement;
import com.datasqrl.flinkwrapper.parser.SqrlStatement;
import com.datasqrl.flinkwrapper.parser.SqrlStatementParser;
import com.datasqrl.flinkwrapper.parser.SqrlTableFunctionStatement;
import com.datasqrl.flinkwrapper.parser.StackableStatement;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.loaders.FlinkTableNamespaceObject;
import com.datasqrl.loaders.FlinkTableNamespaceObject.FlinkTable;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ScriptSqrlModule.ScriptNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.table.RelDataTypeTableSchema;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.SqlNameUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.SqlAlterView;
import org.apache.flink.sql.parser.ddl.SqlAlterViewAs;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlDropView;
import org.apache.flink.table.functions.UserDefinedFunction;

@AllArgsConstructor(onConstructor_=@Inject)
public class SqlScriptPlanner {

  private ErrorCollector errorCollector;

  private ModuleLoader moduleLoader;
  private SqrlStatementParser sqrlParser;
  private final SqlNameUtil nameUtil;
  private final PackageJson packageJson;
  private final ExecutionPipeline pipeline;
  private final ExecutionGoal executionGoal;



  public void plan(MainScript mainScript, Sqrl2FlinkSQLTranslator sqrlEnv) {
    ErrorCollector scriptErrors = errorCollector.withScript(mainScript.getPath(), mainScript.getContent());
    List<ParsedObject<SQLStatement>> statements = sqrlParser.parseScript(mainScript.getContent(), scriptErrors);
    List<StackableStatement> statementStack = new ArrayList<>();
    for (ParsedObject<SQLStatement> statement : statements) {
      ErrorCollector lineErrors = scriptErrors.atFile(statement.getFileLocation());
      SQLStatement sqlStatement = statement.get();
      try {
        planStatement(sqlStatement, statementStack, sqrlEnv, lineErrors);
      } catch (CollectedException e) {
        throw e;
      } catch (Exception e) {
        //Map errors from the Flink parser/planner by adjusting the line numbers
        if (e instanceof org.apache.flink.table.api.SqlParserException) {
          e = (Exception)e.getCause();
        }
        FileLocation location = null;
        String message = null;
        if (e instanceof SqlParseException) {
          location = convertPosition(((SqlParseException) e).getPos());
          message = e.getMessage();
          message = message.replaceAll(" at line \\d*, column \\d*", ""); //remove line number from message
        }
        if (e instanceof CalciteContextException) {
          CalciteContextException calciteException = (CalciteContextException) e;
          location = new FileLocation(calciteException.getPosLine(), calciteException.getPosColumn());
          message = calciteException.getMessage();
          message = message.replaceAll("From line \\d*, column \\d* to line \\d*, column \\d*: ", ""); //remove line number from message
        }
        if (location != null) {
          location = sqlStatement.mapSqlLocation(location);
          e.printStackTrace();
          scriptErrors.atFile(statement.getFileLocation().add(location)).fatal(message);
        }

        //Print stack trace for unknown exceptions
        if (e.getMessage() == null || e instanceof IllegalStateException
            || e instanceof NullPointerException) {
          e.printStackTrace();
        }
        //Use registered error handlers
        throw lineErrors.handle(e);
      }
      if (sqlStatement instanceof StackableStatement) {
        StackableStatement stackableStatement = (StackableStatement) sqlStatement;
        if (stackableStatement.isRoot()) statementStack = new ArrayList<>();
        statementStack.add(stackableStatement);
      } else {
        statementStack = new ArrayList<>();
      }
    }
  }

  private void planStatement(SQLStatement stmt, List<StackableStatement> statementStack, Sqrl2FlinkSQLTranslator sqrlEnv, ErrorCollector errors) throws SqlParseException {
    //Process hints
    PlannerHints hints = PlannerHints.EMPTY;
    if (stmt instanceof SqrlStatement) {
      hints = PlannerHints.fromHints(((SqrlStatement)stmt).getComments());
    }
    if (stmt instanceof SqrlImportStatement) {
      addImport((SqrlImportStatement) stmt, sqrlEnv, errors);
    } else if (stmt instanceof SqrlExportStatement) {
      addExport((SqrlExportStatement) stmt, sqrlEnv);
    } else if (stmt instanceof SqrlCreateTableStatement) {
      SqlNode node = sqrlEnv.parseSQL(((SqrlCreateTableStatement) stmt).toSql(sqrlEnv));
      Preconditions.checkArgument(node instanceof SqlCreateTable);
      sqrlEnv.createTable((SqlCreateTable)node);
    } else if (stmt instanceof SqrlDefinition) {
      SqrlDefinition sqrlDef = (SqrlDefinition) stmt;
      //Ignore test tables (that are not queries) when we are not running tests
      if (executionGoal!=ExecutionGoal.TEST && hints.isTest() && !hints.isQuery()) return;
      String originalSql = sqrlDef.toSql(sqrlEnv, statementStack);
      SqlNode sqlNode = sqrlEnv.parseSQL(originalSql);
      //Relationships and Table functions require special handling
      RelRoot relRoot = sqrlEnv.toRelRoot(sqlNode);
      if (stmt instanceof SqrlTableFunctionStatement || hints.isQuerySink()) {
        if (stmt instanceof SqrlRelationshipStatement) {
          //Add WHERE clause for primary key on left-most join table and restrict top-level SELECT to only indexes from right-most table
        }
        RelNode processedFunction = relRoot.rel;
        List<FunctionParameter> parameters;
        if (stmt instanceof SqrlTableFunctionStatement) {
          //Convert arguments to resolved parameters and update RexDynamicParam to point to the right argument
          parameters = List.of();
        } else {
          parameters = List.of();
        }
        sqrlEnv.addTableFunctionInternal(processedFunction, sqrlDef.getPath(), parameters);
      } else {
        sqrlEnv.addView(sqlNode, originalSql, errors);
      }
    } else if (stmt instanceof FlinkSQLStatement) { //Some other Flink table statement we pass right through
      FlinkSQLStatement flinkStmt = (FlinkSQLStatement) stmt;
      SqlNode node = sqrlEnv.parseSQL(flinkStmt.getSql().get());
      if (node instanceof SqlCreateView || node instanceof SqlAlterViewAs) {
        //plan like other definitions from above
        sqrlEnv.addView(node, flinkStmt.getSql().get(), errors);
      } else if (node instanceof SqlCreateTable) {
        sqrlEnv.createTable((SqlCreateTable)node);
      } else if (node instanceof SqlAlterTable || node instanceof SqlAlterView) {
        errors.fatal("Renaming or altering tables is not supported. Rename them directly in the script or IMPORT AS.");
      } else if (node instanceof SqlDropTable || node instanceof SqlDropView) {
        errors.fatal("Removing tables is not supported. The DAG planner automatically removes unused tables.");
      } else {
        //just pass through
        sqrlEnv.executeSqlNode(node);
      }
    }
  }

  public static final Name STAR = Name.system("*");

  private void addImport(SqrlImportStatement importStmt, Sqrl2FlinkSQLTranslator sqrlEnv, ErrorCollector errors) {
    NamePath path = importStmt.getPackageIdentifier().get();
    boolean isStar = path.getLast().equals(STAR);

    //Alias
    Optional<Name> alias = Optional.empty();
    if (importStmt.getAlias().isPresent()) {
      NamePath aliasPath = importStmt.getAlias().get();
      checkFatal(aliasPath.size()==1, ErrorCode.INVALID_IMPORT, "Invalid table name - paths not supported");
      alias = Optional.of(aliasPath.getFirst());
    }

    SqrlModule module = moduleLoader.getModule(path.popLast()).orElse(null);
    checkFatal(module!=null, importStmt.getPackageIdentifier().getFileLocation(), ErrorLabel.GENERIC,
        "Could not find module [%s] at path: [%s]", path, String.join("/", path.toStringList()));

    if (isStar) {
      if (module.getNamespaceObjects().isEmpty()) {
        errors.warn("Module is empty: %s", path);
      }
      for (NamespaceObject namespaceObject : module.getNamespaceObjects()) {
        //For multiple imports, the alias serves as a prefix.
        addImport(namespaceObject, alias.map(x -> x.append(namespaceObject.getName()).getDisplay()), sqrlEnv);
      }
    } else {
      Optional<NamespaceObject> namespaceObject = module.getNamespaceObject(path.getLast());
      errors.checkFatal(namespaceObject.isPresent(), "Object [%s] not found in module: %s", path.getLast(), path);

      addImport(namespaceObject.get(),
          Optional.of(alias.orElse(path.getLast()).getDisplay()),
          sqrlEnv);
    }
  }

  private void addImport(NamespaceObject nsObject, Optional<String> alias, Sqrl2FlinkSQLTranslator sqrlEnv) {
    if (nsObject instanceof FlinkTableNamespaceObject) {
      //TODO: for a create table statement without options (connector), we manage it internally
      // add pass it to Log engine for augmentation after validating/adding event-id and event-time metadata columns & checking no watermark/partition/constraint is present
      ExternalFlinkTable flinkTable = ExternalFlinkTable.fromNamespaceObject((FlinkTableNamespaceObject) nsObject,
          alias, sqrlEnv, errorCollector);
      sqrlEnv.addImport(flinkTable.tableName.getDisplay(), flinkTable.sqlCreateTable , flinkTable.schema);
    } else if (nsObject instanceof FlinkUdfNsObject) {
      FlinkUdfNsObject fnsObject = (FlinkUdfNsObject) nsObject;
      Preconditions.checkArgument(fnsObject.getFunction() instanceof UserDefinedFunction, "Expected UDF: " + fnsObject.getFunction());
      Class<?> udfClass = fnsObject.getFunction().getClass();
      String name = alias.orElseGet(() -> FlinkUdfNsObject.getFunctionNameFromClass(udfClass).getDisplay());
      sqrlEnv.addFunction(name, udfClass.getName());
    } else if (nsObject instanceof ScriptNamespaceObject) {
      //TODO: 1) if this is a * import, then import inline (throw exception if trying to import only a specific table)
      // 2) if import ends in script, plan script into separate database (with name of script or alias) via CREATE/USE DATABASE
      throw new UnsupportedOperationException("Script imports not yet implemented");
    } else {
      throw new UnsupportedOperationException("Unexpected object imported: " + nsObject);
    }
  }

  private void addExport(SqrlExportStatement exportStmt, Sqrl2FlinkSQLTranslator sqrlEnv) {
    System.out.println("Adding export " + exportStmt.getTableIdentifier() + " to " + exportStmt.getTableIdentifier());
    NamePath sinkPath = exportStmt.getPackageIdentifier().get();
    NamePath tablePath = exportStmt.getTableIdentifier().get();


    Optional<SystemBuiltInConnectors> builtInSink = SystemBuiltInConnectors.forExport(sinkPath.getFirst())
        .filter(x -> sinkPath.size()==2);
    if (builtInSink.isPresent()) {
      SystemBuiltInConnectors connector = builtInSink.get();
      ExecutionStage exportStage;
      if (connector == SystemBuiltInConnectors.LOG_ENGINE) {
        Optional<ExecutionStage> logStage = pipeline.getStageByType(Type.LOG);
        errorCollector.checkFatal(logStage.isPresent(), "Cannot export to log since no log engine has been configured");
        exportStage = logStage.get();
      } else {
        String engineName = connector.getName().getCanonical();
        if (connector == SystemBuiltInConnectors.LOGGER) {
          engineName = packageJson.getCompilerConfig().getLogger();
          if (engineName.equalsIgnoreCase("none")) {
            return; //simply ignore
          }
        }
        Optional<ExecutionStage> optStage = pipeline.getStage(engineName);
        errorCollector.checkFatal(optStage.isPresent(), "The configured logger `%s` under 'compiler.logger' is not a configured engine.", engineName);
        exportStage = optStage.get();
      }
      sqrlEnv.addGeneratedExport(tablePath, exportStage, sinkPath.getLast());
    } else {
      SqrlModule module = moduleLoader.getModule(sinkPath.popLast()).orElse(null);
      checkFatal(module!=null, exportStmt.getPackageIdentifier().getFileLocation(), ErrorLabel.GENERIC,
          "Could not find module [%s] at path: [%s]", sinkPath, String.join("/", sinkPath.toStringList()));

      Optional<NamespaceObject> sinkObj = module.getNamespaceObject(sinkPath.getLast());
      checkFatal(sinkObj.isPresent(), exportStmt.getPackageIdentifier().getFileLocation(), ErrorLabel.GENERIC,
          "Could not find table [%s] in module [%s]", sinkPath.getLast(), module);
      checkFatal(sinkObj.get() instanceof FlinkTableNamespaceObject, exportStmt.getPackageIdentifier().getFileLocation(), ErrorLabel.GENERIC,
          "Not a valid sink table [%s] in module [%s]", sinkPath.getLast(), module);
      FlinkTableNamespaceObject sinkTable = (FlinkTableNamespaceObject) sinkObj.get();

      ExternalFlinkTable flinkTable = ExternalFlinkTable.fromNamespaceObject(sinkTable,
          Optional.of(sinkPath.popLast().getDisplay()), sqrlEnv, errorCollector);
      sqrlEnv.addExternalExport(tablePath, flinkTable.sqlCreateTable, flinkTable.schema);
    }



  }

  @Value
  public static class ExternalFlinkTable {

    Name tableName;
    SqlCreateTable sqlCreateTable;
    Optional<RelDataType> schema;

    public static ExternalFlinkTable fromNamespaceObject(FlinkTableNamespaceObject nsObject,
        Optional<String> alias,
        Sqrl2FlinkSQLTranslator sqrlEnv, ErrorCollector errorCollector) {
      FlinkTable flinkTable = nsObject.getTable();
      Name tableName = alias.map(Name::system).orElse(flinkTable.getName());

      //Parse SQL
      String tableSql = flinkTable.getFlinkSQL();
      ErrorCollector tableError = errorCollector.withScript(flinkTable.getFlinkSqlFile(), tableSql);
      tableSql = SqlScriptStatementSplitter.formatEndOfSqlFile(tableSql);
      SqlNode tableSqlNode;
      try {
        tableSqlNode = sqrlEnv.parseSQL(tableSql);
      } catch (Exception e) {
        throw tableError.handle(e);
      }
      tableError.checkFatal(tableSqlNode instanceof SqlCreateTable, "Expected CREATE TABLE statement");

      //Schema conversion
      Optional<RelDataType> schema = flinkTable.getSchema().map(tableSchema -> {
        if (tableSchema instanceof RelDataTypeTableSchema) {
          return ((RelDataTypeTableSchema) tableSchema).getRelDataType();
        } else {
          return SchemaToRelDataTypeFactory.load(tableSchema)
              .map(tableSchema, null, tableName, errorCollector);
        }
      });

      return new ExternalFlinkTable(tableName, (SqlCreateTable)tableSqlNode, schema);
    }

  }


}
