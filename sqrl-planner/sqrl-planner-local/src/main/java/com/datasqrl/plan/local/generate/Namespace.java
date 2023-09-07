package com.datasqrl.plan.local.generate;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.flink.table.functions.UserDefinedFunction;


@Deprecated
public class Namespace implements AbstractNamespace {

  private final SqrlFramework framework;
  private final ExecutionPipeline pipeline;

  @Getter
  private Map<String, UserDefinedFunction> udfs = new HashMap<>();

  @Getter
  private Set<URL> jars;

  private List<ResolvedExport> exports = new ArrayList<>();

  public Namespace(SqrlFramework framework, ExecutionPipeline pipeline) {
    this.framework = framework;
    this.pipeline = pipeline;
    this.jars = new HashSet<>();
  }

  @Override
  public SqlOperatorTable getOperatorTable() {
    return framework.getSqrlOperatorTable();
  }

  @Override
  public SqrlSchema getSchema() {
    return framework.getSchema();
  }

  @Override
  public List<ResolvedExport> getExports() {
    return exports;
  }

  public ExecutionPipeline getPipeline() {
    return pipeline;
  }
}
