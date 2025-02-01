package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.database.ExecutableQuery;
import lombok.Value;

@Value
public class ExecutableJdbcQuery implements ExecutableQuery {

  String sql;

}
