package com.datasqrl.discovery.file;

import static com.datasqrl.discovery.file.FileCompression.SUPPORTED_COMPRESSION_EXTENSIONS;

import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

@Value
public class FilenameAnalyzer {

  Pattern filePattern;

  public Optional<Components> analyze(Path file) {
    if (!Files.isRegularFile(file)) return Optional.empty();
    return analyze(file.getFileName().toString());
  }

  public Optional<Components> analyze(String file) {
    Matcher matcher = filePattern.matcher(file);
    if (matcher.matches()) {
      return Optional.of(new FilenameAnalyzer.Components(
          matcher.group(1),
          matcher.group(2).toLowerCase(),
          matcher.group(4).toLowerCase()));
    }
    return Optional.empty();
  }

  public static FilenameAnalyzer of(Set<String> fileExtensions) {
    Preconditions.checkArgument(fileExtensions.stream().allMatch(StringUtils::isAllLowerCase),
        "File extensions must be lowercase: %", fileExtensions);
    Pattern pattern = Pattern.compile("(.*)\\.(" +String.join("|", fileExtensions)+ ")"
        + "(\\.("+ String.join("|", SUPPORTED_COMPRESSION_EXTENSIONS) +"))?$", Pattern.CASE_INSENSITIVE);
    return new FilenameAnalyzer(pattern);
  }

  @Value
  public static class Components {
    String filename;
    String extension;
    String compression;
  }


}
