package com.datasqrl.v2.dag.plan;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.ExecutableQuery;
import com.datasqrl.engine.log.LogCreateTopic;
import com.datasqrl.engine.pipeline.ExecutionStage;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

@Value
public class MutationQuery implements ExecutableQuery {

  /**
   * The name of the mutation
   */
  Name name;
  /**
   * Stage against which the mutation is executed
   */
  ExecutionStage stage;
  /**
   * The topic that the mutation is written into
   */
  LogCreateTopic createTopic;
  /**
   * The data type of the input data for the mutation
   */
  RelDataType inputDataType;
  /**
   * The name of the uuid and timestamp column (if any) as defined in the CREATE TABLE statement.
   *
   * TODO: We want to generalize this to allow arbitrary upfront computations
   * on the server and not have this hardcoded to just these two columns.
   */
  Optional<String> uuidColumnName;
  Optional<String> timestampColumnName;

}
