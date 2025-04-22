package com.datasqrl.config;

import java.util.Optional;
import java.util.regex.Pattern;

import com.datasqrl.error.ErrorCollector;

import lombok.NonNull;
import lombok.Value;

@Value
public class DataDiscoveryConfigImpl implements PackageJson.DataDiscoveryConfig {

  public static final String TABLE_PATTERN_KEY = "pattern";

  ErrorCollector errors;
  Optional<String> tablePattern;

  public static DataDiscoveryConfigImpl of(SqrlConfig config, @NonNull ErrorCollector errors) {
    Optional<Pattern> pattern = Optional.empty();
    var tablePattern = config.asString(TABLE_PATTERN_KEY)
        .validate(TablePattern::isValid, "Not a valid regular expression for the table pattern")
        .getOptional();
    return new DataDiscoveryConfigImpl(errors, tablePattern);
  }

  @Override
public TablePattern getTablePattern(String defaultPattern) {
    return TablePattern.of(tablePattern, defaultPattern);
  }

}
