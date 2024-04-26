/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.hints;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.rules.SqrlConverterConfig;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.lang3.tuple.Pair;

public interface OptimizerHint {

  @Value
  public static class Stage implements OptimizerHint.Pipeline {

    public static final String HINT_NAME = "exec";

    String stageName;

    @Override
    public void add2Config(ExecutionPipeline pipeline, SqrlConverterConfig.SqrlConverterConfigBuilder configBuilder, ErrorCollector errors) {
      configBuilder.stage(getStage(pipeline,errors));
    }

    public ExecutionStage getStage(ExecutionPipeline pipeline, ErrorCollector errors) {
      Optional<ExecutionStage> stage = pipeline.getStage(stageName)
          .or(()->pipeline.getStageByType(stageName));
      errors.checkFatal(stage.isPresent(),"Could not find execution stage [%s] specified in optimizer hint", stageName);
      return stage.get();
    }

  }

  interface Pipeline extends OptimizerHint {

    void add2Config(ExecutionPipeline pipeline,
        SqrlConverterConfig.SqrlConverterConfigBuilder configBuilder, ErrorCollector errors);

  }

  interface Generic extends OptimizerHint.Pipeline {

    void add2Config(SqrlConverterConfig.SqrlConverterConfigBuilder configBuilder, ErrorCollector errors);

    default void add2Config(ExecutionPipeline pipeline,
        SqrlConverterConfig.SqrlConverterConfigBuilder configBuilder, ErrorCollector errors) {
      add2Config(configBuilder, errors);
    }

  }

  static Pair<List<OptimizerHint>,List<SqlHint>> fromSqlHints(Optional<SqlNodeList> hints, ErrorCollector errors) {
    List<OptimizerHint> optHints = new ArrayList<>();
    List<SqlHint> otherHints = new ArrayList<>();
    if (hints.isPresent()) {
      for (SqlHint hint : Iterables.filter(hints.get().getList(), SqlHint.class)) {
        String hintname = hint.getName().toLowerCase();
        if (hintname.equalsIgnoreCase(Stage.HINT_NAME)) {
          List<String> options = hint.getOptionList();
          errors.checkFatal(options!=null && options.size()==1,
              "Expected a single option for [%s] hint but found: %s", Stage.HINT_NAME, options);
          optHints.add(new Stage(options.get(0).trim()));
        } else {
          otherHints.add(hint);
        }
      }
    }
    return Pair.of(optHints, otherHints);
  }





}
