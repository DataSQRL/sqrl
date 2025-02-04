package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.ExecutableQuery;
import com.datasqrl.engine.pipeline.ExecutionStage;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.Value;

/**
 * Represents a read query that can be executed via JDBC against a database.
 * It has a SQL string for the query which contains parameter references to the
 * parameters of the associated {@link com.datasqrl.v2.tables.SqrlTableFunction}.
 *
 * Some JDBC engines don't support parameter references, i.e. the parameters are just a list
 * in the order that '?' occurs in the SQL string. In that case, we need to map those parameter occurences
 * to the function parameters which the optional parameterMap does.
 */
@Value
@Builder
public class ExecutableJdbcReadQuery implements ExecutableQuery {

  ExecutionStage stage;
  String sql;
  Optional<List<Integer>> parameterMap = Optional.empty(); //Not yet supported


}
