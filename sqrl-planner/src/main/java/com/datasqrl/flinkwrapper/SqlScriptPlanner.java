package com.datasqrl.flinkwrapper;


import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.log.Log;
import com.datasqrl.engine.stream.flink.plan.FlinkSqlNodeFactory;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.flinkwrapper.parser.SqrlStatement;
import com.datasqrl.flinkwrapper.parser.SqrlStatement.Type;
import com.datasqrl.flinkwrapper.parser.SqrlStatementParser;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.loaders.FlinkTableNamespaceObject;
import com.datasqrl.loaders.FlinkTableNamespaceObject.FlinkTable;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.TableSourceNamespaceObject;
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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;

@AllArgsConstructor(onConstructor_=@Inject)
public class SqlScriptPlanner {

  private ErrorCollector errorCollector;
  private SqlScriptStatementSplitter sqlSplitter;
  private ModuleLoader moduleLoader;
  private SqrlStatementParser sqrlParser;
  private final SqlNameUtil nameUtil;



  public void plan(MainScript mainScript, SqrlEnvironment sqrlEnv) {
    ErrorCollector scriptErrors = errorCollector.withScript(mainScript.getPath().orElse(Path.of("undefined")),
        mainScript.getContent());

    List<String> statements = sqlSplitter.splitStatements(mainScript.getContent());

    int lineNo = 1;

    for (String statement : statements) {
      ErrorCollector lineErrors = scriptErrors
          .atFile(new FileLocation(lineNo, 1));
      try {
        planStatement(statement, sqrlEnv.withErrors(lineErrors));
        if (lineErrors.hasErrors()) {
          throw new CollectedException(new RuntimeException("Script cannot validate"));
        }
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


  private void planStatement(String sqlStatement, SqrlEnvironment sqrlEnv) throws SqlParseException {
    //See if this is a special
    String sqlBefore = sqlStatement;
    Optional<SqrlStatement> sqrlStatement = sqrlParser.parse(sqlStatement);
    if (sqrlStatement.isPresent()) {
      SqrlStatement stmt = sqrlStatement.get();
      if (stmt.getType()== Type.IMPORT) {
        addImport(stmt.getPackageIdentifier().get(), stmt.getIdentifier(), sqrlEnv);
        return;
      } else if (stmt.getType()== Type.EXPORT) {
        addExport(stmt.getIdentifier().get(), stmt.getPackageIdentifier().get(), sqrlEnv);
        return;
      } else {
        Preconditions.checkArgument(stmt.isDefinition());
        sqlStatement = stmt.rewriteSQL();
      }
    }
    sqrlEnv.executeSQL(sqlStatement);
  }

  private void addImport(SqlIdentifier packageIdentifier, Optional<SqlIdentifier> name, SqrlEnvironment sqrlEnv) {
    System.out.println("Adding import " + packageIdentifier.toString() + " as " + name);
    sqrlEnv.getErrors().checkFatal(!packageIdentifier.isStar() || name.isEmpty(), ErrorCode.IMPORT_CANNOT_BE_ALIASED, "Import cannot be aliased");

    NamePath path = nameUtil.toNamePath(packageIdentifier.names);
    if (path.getFirst().getDisplay().equals("log")) {
      //TODO: implement import for log
//      Log log = logManager.getLogs().get(path.getLast().getDisplay());
//      NamespaceObject namespaceObject = createTableResolver.create(log.getSource());
//      namespaceObject.apply(this, node.getAlias().map(SqlIdentifier::getSimple), framework, errorCollector);
    } else {
      SqrlModule module = moduleLoader.getModule(path.popLast()).orElse(null);

      sqrlEnv.getErrors().checkFatal(module!=null, "Could not find module [%s] at path: [%s]",
          path, String.join("/", path.toStringList()));

      if (packageIdentifier.isStar()) {
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
        addImport(namespaceObject.get(),
            Optional.of(name.map(a -> a.names.get(0)).orElse(/*retain alias*/path.getLast().getDisplay())),
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

  private void addExport(SqlIdentifier tableName, SqlIdentifier packageIdentifier, SqrlEnvironment sqrlEnv) {
    System.out.println("Adding export " + tableName + " to " + packageIdentifier);
  }


}
