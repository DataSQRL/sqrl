package com.datasqrl.frontend;

import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.plan.SqrlPlanningTableFactory;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.plan.ScriptPlanner;
import com.datasqrl.error.CollectedException;
import com.datasqrl.plan.validate.ScriptValidator;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderComposite;
import com.datasqrl.plan.local.generate.*;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.util.SqlNameUtil;
import com.google.inject.Inject;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import org.apache.calcite.sql.SqrlStatement;

public class SqrlPlan extends SqrlBase {

  private final DebuggerConfig debuggerConfig;
  private final SqrlFramework framework;
  private final ExecutionPipeline pipeline;
  protected ModuleLoader moduleLoader;
  protected NameCanonicalizer nameCanonicalizer;
  protected CalciteTableFactory tableFactory;
  protected SqrlQueryPlanner planner;

  @Inject
  public SqrlPlan(ErrorCollector errors,
      ModuleLoader moduleLoader, NameCanonicalizer nameCanonicalizer,
      CalciteTableFactory tableFactory,
      SqrlQueryPlanner planner, DebuggerConfig debuggerConfig, SqrlFramework framework,
      ExecutionPipeline pipeline) {
    super(errors);
    this.moduleLoader = moduleLoader;
    this.nameCanonicalizer = nameCanonicalizer;
    this.tableFactory = tableFactory;
    this.planner = planner;
    this.debuggerConfig = debuggerConfig;
    this.framework = framework;
    this.pipeline = pipeline;
  }

  public Namespace plan(String script, List<ModuleLoader> additionalModules) {
    if (errors.getLocation() == null || errors.getLocation().getSourceMap() == null) {
      errors = errors.withSchema("<schema>", script);
    }

    ScriptNode scriptNode;
    try {
      scriptNode = (ScriptNode) framework.getQueryPlanner().parse(Dialect.SQRL,
          script);
    } catch (Exception e) {
      throw errors.handle(e);
    }

    ErrorCollector collector = errors.withScript("<script>", script);
    return plan(scriptNode, additionalModules, collector);
  }

  public Namespace plan(ScriptNode node, List<ModuleLoader> additionalModules,
      ErrorCollector collector) {
    ModuleLoader updatedModuleLoader = this.moduleLoader;
    if (!additionalModules.isEmpty()) {
      updatedModuleLoader = ModuleLoaderComposite.builder()
          .moduleLoader(this.moduleLoader)
          .moduleLoaders(additionalModules)
          .build();
    }

    framework.resetPlanner();
    for (SqlNode statement : node.getStatements()) {
      try {
        ErrorCollector errors = collector
            .atFile(SqrlAstException.toLocation(statement.getParserPosition()));

        ScriptValidator validator = new ScriptValidator(framework, framework.getQueryPlanner(),
            updatedModuleLoader, errors, new SqlNameUtil(nameCanonicalizer));
        validator.validateStatement((SqrlStatement) statement);
        if (errors.hasErrors()) {
          throw new CollectedException(new RuntimeException("Script cannot validate"));
        }

        ScriptPlanner planner = new ScriptPlanner(
            framework.getQueryPlanner(), validator,
            new SqrlPlanningTableFactory(framework), framework,
            new SqlNameUtil(nameCanonicalizer), errors);

        planner.plan(statement);
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

    return new Namespace(framework, pipeline);
  }

  public Debugger getDebugger() {
    return new Debugger(debuggerConfig, moduleLoader);
  }

}
