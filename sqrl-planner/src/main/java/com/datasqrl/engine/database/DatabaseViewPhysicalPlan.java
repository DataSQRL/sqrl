package com.datasqrl.engine.database;

import java.util.List;
import lombok.Value;

public interface DatabaseViewPhysicalPlan extends DatabasePhysicalPlanOld {

  List<DatabaseView> getViews();


  interface DatabaseView {

    String getName();

    String getSql();

  }


  @Value
  class DatabaseViewImpl implements DatabaseView {

    String name;
    String sql;

  }

}
