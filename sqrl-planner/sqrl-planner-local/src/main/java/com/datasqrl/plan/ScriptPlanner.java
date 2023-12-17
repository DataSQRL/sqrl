package com.datasqrl.plan;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.plan.validate.ScriptValidator;
import com.datasqrl.util.SqlNameUtil;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlStatement;

public class ScriptPlanner {

  public static void plan(String script, SqrlFramework framework, ModuleLoader moduleLoader,
      NameCanonicalizer nameCanonicalizer, ErrorCollector errorCollector) {
    if (errorCollector.getLocation() == null || errorCollector.getLocation().getSourceMap() == null) {
      errorCollector = errorCollector.withSchema("<schema>", script);
    }

    ScriptNode scriptNode;
    try {
      scriptNode = (ScriptNode) framework.getQueryPlanner().parse(Dialect.SQRL, script);
    } catch (Exception e) {
      throw errorCollector.handle(e);
    }

    ErrorCollector collector = errorCollector.withScript("<script>", script);

    framework.resetPlanner();
    SqrlPlanningTableFactory sqrlPlanningTableFactory = new SqrlPlanningTableFactory(framework);
    for (SqlNode statement : scriptNode.getStatements()) {
      try {
        ErrorCollector errors = collector.atFile(
            SqrlAstException.toLocation(statement.getParserPosition()));

        ScriptValidator validator = new ScriptValidator(framework, framework.getQueryPlanner(),
            moduleLoader, errors, new SqlNameUtil(nameCanonicalizer));
        validator.validateStatement((SqrlStatement) statement);
        if (errors.hasErrors()) {
          throw new CollectedException(new RuntimeException("Script cannot validate"));
        }

        com.datasqrl.calcite.plan.ScriptPlanner planner = new com.datasqrl.calcite.plan.ScriptPlanner(
            framework.getQueryPlanner(), validator, sqrlPlanningTableFactory,
            framework, new SqlNameUtil(nameCanonicalizer), errors);

        planner.plan(statement);
      } catch (CollectedException e) {
        throw e;
      } catch (Exception e) {
        //Print stack trace for unknown exceptions
        if (e.getMessage() == null || e instanceof IllegalStateException
            || e instanceof NullPointerException) {
          e.printStackTrace();
        }

        ErrorCollector statementErrors = collector.atFile(
            SqrlAstException.toLocation(statement.getParserPosition()));

        throw statementErrors.handle(e);
      }
    }
  }
}
