package com.datasqrl.v2.dag.plan;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.ExecutableQuery;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.v2.tables.MutationComputedColumn;
import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

@Value
@Builder
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
  EngineCreateTable createTopic;
  /**
   * The data type of the input data for the mutation
   */
  RelDataType inputDataType;
  /**
   * The data type of the result data for the mutation
   */
  RelDataType outputDataType;
  /**
   * The columns that are computed and not provided
   * explicitly by the user
   */
  @Singular
  List<MutationComputedColumn> computedColumns;

}
