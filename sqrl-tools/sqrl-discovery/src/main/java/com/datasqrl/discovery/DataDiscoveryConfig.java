package com.datasqrl.discovery;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.ConnectorFactory;
import com.datasqrl.io.tables.FlinkConnectorFactory;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.NonNull;
import lombok.Value;

@Value
public class DataDiscoveryConfig {

  public static final String TABLE_PATTERN_KEY = "pattern";

  ErrorCollector errors;
  Optional<String> tablePattern;
  ConnectorFactory connectorFactory = FlinkConnectorFactory.INSTANCE;

  public static DataDiscoveryConfig of(SqrlConfig config, @NonNull ErrorCollector errors) {
    Optional<Pattern> pattern = Optional.empty();
    Optional<String> tablePattern = config.asString(TABLE_PATTERN_KEY)
        .validate(TablePattern::isValid, "Not a valid regular expression for the table pattern")
        .getOptional();
    return new DataDiscoveryConfig(errors, tablePattern);
  }

  public TablePattern getTablePattern(String defaultPattern) {
    return TablePattern.of(tablePattern, defaultPattern);
  }



}
