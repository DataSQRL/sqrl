package com.datasqrl.io.impl.file;

import com.datasqrl.config.Constraints.Default;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableConfig;
import java.util.List;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FileDataSystemConfig {

  public static final String DEFAULT_FILENAME_PATTERN = "^([^\\.]+?)(?:_part.*)?$";

  @Default
  String directoryURI = "";

  @Default
  List<String> fileURIs = List.of();

  @Default
  String filenamePattern = DEFAULT_FILENAME_PATTERN;

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
    return Pattern.compile(regex);  }

}
