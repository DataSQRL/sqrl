package com.datasqrl;

import com.datasqrl.compile.Compiler;
import com.datasqrl.engine.PhysicalPlanExecutor;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.local.generate.ClasspathResourceResolver;

/**
 * Used for stand alone flink jars
 */
public class FlinkMain {

  public static void main(String[] args) {
    ErrorCollector errors = ErrorCollector.root();

    //Recompile
    Compiler compiler = new Compiler();

    Compiler.CompilerResult result = compiler.run(errors,
        new ClasspathResourceResolver(), false);

    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    PhysicalPlanExecutor.Result execute = executor.execute(result.getPlan());
  }
}
