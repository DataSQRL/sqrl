package com.datasqrl.io.impl.file;

import com.datasqrl.config.Constraints.Default;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableConfig;
import java.io.Serializable;
import java.util.List;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;

@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FileDataSystemConfig implements Serializable {

  public static final String DEFAULT_FILENAME_PATTERN = "^([^\\.]+?)(?:_part.*)?$";

  @Default
  String directoryURI = "";

  @Default
  List<String> fileURIs = List.of();

  @Default
  String filenamePattern = DEFAULT_FILENAME_PATTERN;

  @Default
  @JsonProperty("source.monitor-interval")
  String source$$monitor$interval = "10s";

  public static FileDataSystemConfig fromConfig(@NonNull TableConfig config) {
    return config.getConnectorConfig().allAs(FileDataSystemConfig.class).get();
  }

  public FilePathConfig getFilePath(@NonNull ErrorCollector errors) {
    return FilePathConfig.of(directoryURI, fileURIs, errors);
  }

  public Pattern getPattern() {
    String regex = filenamePattern;
    if (!regex.startsWith("^")) {
      regex = "^" + regex;
    }
    if (!regex.endsWith("$")) {
      regex += "$";
    }
    return Pattern.compile(regex);
  }
}
