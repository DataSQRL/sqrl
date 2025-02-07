package com.datasqrl.plan.global;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.v2.Sqrl2FlinkSQLTranslator;

public interface PhysicalPlanRewriter {

  boolean appliesTo(EnginePhysicalPlan plan);

  EnginePhysicalPlan rewrite(EnginePhysicalPlan plan, Sqrl2FlinkSQLTranslator sqrlEnv);

}
