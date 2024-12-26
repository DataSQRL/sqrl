package com.datasqrl.flinkwrapper;


import static com.datasqrl.flinkwrapper.parser.StatementParserException.checkFatal;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.stream.flink.plan.FlinkSqlNodeFactory;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.flinkwrapper.parser.FlinkSQLStatement;
import com.datasqrl.flinkwrapper.parser.ParsedObject;
import com.datasqrl.flinkwrapper.parser.SQLStatement;
import com.datasqrl.flinkwrapper.parser.SqlScriptStatementSplitter;
import com.datasqrl.flinkwrapper.parser.SqrlCreateTableStatement;
import com.datasqrl.flinkwrapper.parser.SqrlDefinition;
import com.datasqrl.flinkwrapper.parser.SqrlExportStatement;
import com.datasqrl.flinkwrapper.parser.SqrlImportStatement;
import com.datasqrl.flinkwrapper.parser.SqrlStatement;
import com.datasqrl.flinkwrapper.parser.SqrlStatementParser;
import com.datasqrl.flinkwrapper.parser.StatementParserException;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.loaders.FlinkTableNamespaceObject;
import com.datasqrl.loaders.FlinkTableNamespaceObject.FlinkTable;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.MainScript;
import com.datasqrl.util.SqlNameUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.table.operations.Operation;

@AllArgsConstructor(onConstructor_=@Inject)
public class SqlScriptPlanner {

  private ErrorCollector errorCollector;

  private ModuleLoader moduleLoader;
  private SqrlStatementParser sqrlParser;
  private final SqlNameUtil nameUtil;



  public void plan(MainScript mainScript, SqrlEnvironment sqrlEnv) {
    ErrorCollector scriptErrors = errorCollector.withScript(mainScript.getPath().orElse(Path.of("undefined")),
        mainScript.getContent());

    List<ParsedObject<SQLStatement>> statements = sqrlParser.parseScript(mainScript.getContent(), scriptErrors);

    for (ParsedObject<SQLStatement> statement : statements) {
      ErrorCollector lineErrors = scriptErrors
          .atFile(statement.getFileLocation());
      try {
        planStatement(statement.get(), sqrlEnv.withErrors(lineErrors));
      } catch (CollectedException e) {
        throw e;
      } catch (Exception e) {
        //Print stack trace for unknown exceptions
        if (e.getMessage() == null || e instanceof IllegalStateException
            || e instanceof NullPointerException) {
          e.printStackTrace();
        }
        throw lineErrors.handle(e);
      }
    }
  }


  private void planStatement(SQLStatement stmt, SqrlEnvironment sqrlEnv) throws SqlParseException {
    if (stmt instanceof SqrlImportStatement) {
      addImport((SqrlImportStatement) stmt, sqrlEnv);
    } else if (stmt instanceof SqrlExportStatement) {
      addExport((SqrlExportStatement) stmt, sqrlEnv);
    } else if (stmt instanceof SqrlCreateTableStatement) {
      Operation createTable = sqrlEnv.parse(((SqrlCreateTableStatement) stmt).getCreateTable().get());
      SqlNode sqlNode = sqrlEnv.parseSQL(((SqrlCreateTableStatement) stmt).getCreateTable().get());
      System.out.println(createTable.asSummaryString());
    } else if (stmt instanceof SqrlDefinition) {
      ((SqrlDefinition)stmt).apply(sqrlEnv);
    } else if (stmt instanceof FlinkSQLStatement) {
      try {
        sqrlEnv.executeSQL(((FlinkSQLStatement)stmt).getSql().get());
      } catch (Exception e) {
        throw StatementParserException.from(e, FileLocation.START, 0);
      }
    }
  }

  public static final Name STAR = Name.system("*");

  private void addImport(SqrlImportStatement importStmt, SqrlEnvironment sqrlEnv) {
    NamePath path = importStmt.getPackageIdentifier().get();
    boolean isStar = path.getLast().equals(STAR);
    checkFatal(!isStar || importStmt.getAlias().isEmpty(), ErrorCode.IMPORT_CANNOT_BE_ALIASED, "Import cannot be aliased");

    if (path.getFirst().getDisplay().equals("log")) {
      //TODO: implement import for log
//      Log log = logManager.getLogs().get(path.getLast().getDisplay());
//      NamespaceObject namespaceObject = createTableResolver.create(log.getSource());
//      namespaceObject.apply(this, node.getAlias().map(SqlIdentifier::getSimple), framework, errorCollector);
    } else {
      SqrlModule module = moduleLoader.getModule(path.popLast()).orElse(null);

      sqrlEnv.getErrors().checkFatal(module!=null, "Could not find module [%s] at path: [%s]",
          path, String.join("/", path.toStringList()));

      if (isStar) {
        if (module.getNamespaceObjects().isEmpty()) {
          sqrlEnv.getErrors().warn("Module is empty: %s", path);
        }
//        module.getNamespaceObjects().forEach(obj -> obj.apply(this, Optional.empty(), framework, errorCollector));
        for (NamespaceObject namespaceObject : module.getNamespaceObjects()) {
          addImport(namespaceObject, Optional.empty(), sqrlEnv);
        }
      } else {
        Optional<NamespaceObject> namespaceObject = module.getNamespaceObject(path.getLast());
        sqrlEnv.getErrors().checkFatal(namespaceObject.isPresent(), "Object [%s] not found in module: %s", path.getLast(), path);
        Name tableName = path.getLast();
        if (importStmt.getAlias().isPresent()) {
          NamePath aliasPath = importStmt.getAlias().get();
          checkFatal(aliasPath.size()==1, ErrorCode.INVALID_IMPORT, "Invalid table name - paths not supported");
          tableName = aliasPath.getFirst();
        }
        addImport(namespaceObject.get(),
            Optional.of(tableName.getDisplay()),
            sqrlEnv);
      }
    }
  }

  private void addImport(NamespaceObject table, Optional<String> alias, SqrlEnvironment sqrlEnv) {
    Preconditions.checkArgument(table instanceof FlinkTableNamespaceObject);
    FlinkTable flinkTable = ((FlinkTableNamespaceObject) table).getTable();
    Name tableName = alias.map(Name::system).orElse(flinkTable.getName());
    String tableSql = flinkTable.getFlinkSQL();
    //TODO: replace name and use SqlIdentifier for proper escaping
    sqrlEnv.withErrors(errorCollector.withScript(flinkTable.getFlinkSqlFile(),
        tableSql));
    tableSql = SqlScriptStatementSplitter.formatEndOfSqlFile(tableSql);
    Preconditions.checkArgument(tableSql.endsWith(";\n"));
    if (flinkTable.getSchema().isPresent()) {
      String schemaTableName = tableName.getDisplay()+"__schema";
      TableSchema tableSchema = flinkTable.getSchema().get();
      RelDataType dataType = SchemaToRelDataTypeFactory.load(tableSchema)
          .map(tableSchema, null, tableName, sqrlEnv.getErrors());
      SqlCreateTable schemaTable = FlinkSqlNodeFactory.createTable(schemaTableName, dataType);
      sqrlEnv.executeSqlNode(schemaTable);
      //Append LIKE
      tableSql = tableSql.substring(0, tableSql.length()-2) + " LIKE " + schemaTableName + " ;\n";
    }
    sqrlEnv.executeSQL(tableSql);
  }

  private void addExport(SqrlExportStatement exportStmt, SqrlEnvironment sqrlEnv) {
    System.out.println("Adding export " + exportStmt.getTableIdentifier() + " to " + exportStmt.getTableIdentifier());
  }


}
