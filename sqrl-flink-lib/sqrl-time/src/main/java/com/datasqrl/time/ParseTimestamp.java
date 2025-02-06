package com.datasqrl.time;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

import com.datasqrl.function.FlinkTypeUtil;
import com.datasqrl.function.FlinkTypeUtil.VariableArguments;

import lombok.extern.slf4j.Slf4j;

/**
 * Parses a timestamp from an ISO timestamp string.
 */
@Slf4j
public class ParseTimestamp extends ScalarFunction {

  public Instant eval(String s) {
    return Instant.parse(s);
  }

  public Instant eval(String s, String format) {
    var formatter = DateTimeFormatter.ofPattern(format, Locale.US);
    try {
      return LocalDateTime.parse(s, formatter).atZone(ZoneId.systemDefault()).toInstant();
    } catch (Exception e) {
      log.warn(e.getMessage());
      return null;
    }
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder().inputTypeStrategy(
            VariableArguments.builder().staticType(DataTypes.STRING()).variableType(DataTypes.STRING())
                .minVariableArguments(0).maxVariableArguments(1).build()).outputTypeStrategy(
            FlinkTypeUtil.nullPreservingOutputStrategy(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)))
        .build();
  }
}

