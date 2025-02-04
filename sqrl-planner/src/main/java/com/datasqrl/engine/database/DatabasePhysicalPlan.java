package com.datasqrl.engine.database;

import com.datasqrl.engine.EnginePhysicalPlan;

public interface DatabasePhysicalPlan extends EnginePhysicalPlan {

  void generateIndexes();

  String getSchema();

  String getViews();


}
