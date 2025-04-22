package com.datasqrl.plan.hints;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.rules.SqrlConverterConfig.SqrlConverterConfigBuilder;

import lombok.Value;

@Value
public class FilteredDistinctOrderHint implements OptimizerHint.ConverterHint {

  public static final String HINT_NAME = "filtered_distinct_order";

  @Override
  public void add2Config(SqrlConverterConfigBuilder configBuilder, ErrorCollector errors) {
    configBuilder.filterDistinctOrder(true);
  }

}
