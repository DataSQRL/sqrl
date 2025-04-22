package com.datasqrl.plan.hints;

import java.util.List;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.rules.SqrlConverterConfig.SqrlConverterConfigBuilder;

import lombok.Value;

@Value
public class PrimaryKeyHint implements OptimizerHint.ConverterHint {

  public static final String HINT_NAME = "primary_key";

  List<String> columnNames;

  @Override
  public void add2Config(SqrlConverterConfigBuilder configBuilder, ErrorCollector errors) {
    errors.checkFatal(!columnNames.isEmpty(), "Primary key column hint cannot be empty.");
    configBuilder.primaryKeyNames(columnNames);
  }
}
