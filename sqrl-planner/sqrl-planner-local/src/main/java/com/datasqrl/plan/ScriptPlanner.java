package com.datasqrl.plan;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.util.SqlNameUtil;
import com.google.inject.Inject;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlStatement;

@AllArgsConstructor(onConstructor_=@Inject)
public class ScriptPlanner {

  private final SqrlFramework framework;
  private final ModuleLoader moduleLoader;
  private final ErrorCollector errorCollector;
  private final SqrlPlanningTableFactory planningTableFactory;

  public void plan(MainScript mainScript) {
    ErrorCollector errors = errorCollector;
    if (errorCollector.getLocation() == null
        || errorCollector.getLocation().getSourceMap() == null) {
      errors = errorCollector.withSchema("<schema>", mainScript.getContent());
    }

    ScriptNode scriptNode;
    try {
      scriptNode = (ScriptNode) framework.getQueryPlanner().parse(Dialect.SQRL,
          mainScript.getContent());
    } catch (Exception e) {
      throw errors.handle(e);
    }

    //wtf is this, fix it
    ErrorCollector scriptErrors = errorCollector.withScript("<script>", mainScript.getContent());
    plan(scriptNode, scriptErrors);
  }

  private void plan(ScriptNode node, ErrorCollector collector) {

    framework.resetPlanner();
    for (SqlNode statement : node.getStatements()) {
      try {
        ErrorCollector errors = collector
            .atFile(SqrlAstException.toLocation(statement.getParserPosition()));

        com.datasqrl.plan.validate.ScriptPlanner validator = new com.datasqrl.plan.validate.ScriptPlanner(framework, framework.getQueryPlanner(),
            moduleLoader, errors, new SqlNameUtil(framework.getNameCanonicalizer()), planningTableFactory);
        validator.validateStatement((SqrlStatement) statement);
        if (errors.hasErrors()) {
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

        ErrorCollector statementErrors = collector
            .atFile(SqrlAstException.toLocation(statement.getParserPosition()));

        throw statementErrors.handle(e);
      }
    }
  }
}
