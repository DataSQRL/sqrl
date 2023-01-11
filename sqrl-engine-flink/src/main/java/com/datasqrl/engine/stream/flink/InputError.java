/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.error.ErrorCollection;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.error.ErrorMessage.Severity;
import com.datasqrl.error.ErrorPrinter;
import java.io.Serializable;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.event.Level;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class InputError<Input> implements Serializable {

  @NonNull private Input input;
  @NonNull private ErrorCollection errors;

  public static <Input> InputError<Input> of(@NonNull ErrorCollector errors, @NonNull Input input) {
    return new InputError(input, errors.getErrors());
  }

  public String prettyPrint() {
    return errors.stream().map(this::getInputErrorMessage).map(InputErrorMessage::toString)
        .collect(Collectors.joining());
  }

  @Override
  public String toString() {
    return prettyPrint();
  }

  public void printToLog(Logger log) {
    for (ErrorMessage error : errors) {
      log.atLevel(getLevel(error.getSeverity())).log(getInputErrorMessage(error).prettyPrint(false));
    }
  }

  public static Level getLevel(Severity severity) {
    switch (severity) {
      case FATAL: return Level.ERROR;
      case WARN: return Level.WARN;
      case NOTICE: return Level.INFO;
      default: throw new UnsupportedOperationException(severity.name());
    }
  }

  public static String getLocationString(ErrorLocation location) {
    return location.getPrefix().toLowerCase() + ":" + location.getPath();
  }

  public InputErrorMessage getInputErrorMessage(ErrorMessage error) {
    return new InputErrorMessage(error.getSeverity().name(), error.getMessage(),
        getLocationString(error.getLocation()),
        input.toString(), ErrorPrinter.getErrorDescription(error, true),
        error.getErrorLabel().getLabel());
  }

  public static class InputErrorMessage implements Serializable {

    public String severity;
    public String message;
    public String location;
    public String inputData;
    public String description;
    public String errorLabel;

    // default constructor for DataStream API
    public InputErrorMessage() {}

    // fully assigning constructor for Table API
    public InputErrorMessage(String severity, String message, String location,
        String inputData, String description, String errorLabel) {
      this.severity = severity;
      this.message = message;
      this.location = location;
      this.inputData = inputData;
      this.description = description;
      this.errorLabel = errorLabel;
    }

    public static Schema getTableSchema() {
      return Schema.newBuilder()
          .column("severity", "STRING")
          .column("message", "STRING")
          .column("location", "STRING")
          .column("inputData", "STRING")
          .column("description", "STRING")
          .column("errorLabel", "STRING")
          //.watermark("event_time", "SOURCE_WATERMARK()")
          .build();
    }

    @Override
    public String toString() {
      return prettyPrint(true);
    }

    public String prettyPrint(boolean includeSeverity) {
      StringBuilder b = new StringBuilder();
      if (includeSeverity) b.append("[").append(severity).append("]");
      b.append(message).append("\n");
      b.append("on data: ").append(inputData).append("\n");
      b.append("in ").append(location).append("\n");
      b.append(description);
      return b.toString();
    }

  }

  public static class Map2InputErrorMessage implements FlatMapFunction<InputError,InputErrorMessage> {

    @Override
    public void flatMap(InputError inputError, Collector<InputErrorMessage> collector)
        throws Exception {
      inputError.errors.forEach(error -> collector.collect(inputError.getInputErrorMessage(error)));
    }
  }

}
