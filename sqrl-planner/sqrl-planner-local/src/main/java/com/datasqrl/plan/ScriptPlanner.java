package com.datasqrl.plan;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderComposite;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.util.SqlNameUtil;
import java.util.List;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlStatement;

public class ScriptPlanner {
  public static void plan(String script, List<ModuleLoader> additionalModules, SqrlFramework framework, ModuleLoader moduleLoader,
      NameCanonicalizer nameCanonicalizer, ErrorCollector errorCollector) {
    if (errorCollector.getLocation() == null
        || errorCollector.getLocation().getSourceMap() == null) {
      errorCollector = errorCollector.withSchema("<schema>", script);
    }

    ScriptNode scriptNode;
    try {
      scriptNode = (ScriptNode) framework.getQueryPlanner().parse(Dialect.SQRL,
          script);
    } catch (Exception e) {
      throw errorCollector.handle(e);
    }

    ErrorCollector collector = errorCollector.withScript("<script>", script);
    plan(scriptNode, additionalModules, moduleLoader, framework, nameCanonicalizer,collector);
  }

  private static void plan(ScriptNode node, List<ModuleLoader> additionalModules,
      ModuleLoader moduleLoader, SqrlFramework framework, NameCanonicalizer nameCanonicalizer,
      ErrorCollector collector) {
    ModuleLoader updatedModuleLoader = moduleLoader;
    if (!additionalModules.isEmpty()) {
      updatedModuleLoader = ModuleLoaderComposite.builder()
          .moduleLoader(moduleLoader)
          .moduleLoaders(additionalModules)
          .build();
    }

    framework.resetPlanner();
    for (SqlNode statement : node.getStatements()) {
      try {
        ErrorCollector errors = collector
            .atFile(SqrlAstException.toLocation(statement.getParserPosition()));

        com.datasqrl.plan.validate.ScriptPlanner validator = new com.datasqrl.plan.validate.ScriptPlanner(framework, framework.getQueryPlanner(),
            updatedModuleLoader, errors, new SqlNameUtil(nameCanonicalizer), new SqrlPlanningTableFactory(framework));
        validator.validateStatement((SqrlStatement) statement);
        if (errors.hasErrors()) {
          System.out.println(ErrorPrinter.prettyPrint(errors));
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
