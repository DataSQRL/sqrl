/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.formats;

import com.datasqrl.config.Constraints.Default;
import com.datasqrl.config.Constraints.NotEmpty;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.NotYetImplementedException;
import com.datasqrl.io.impl.InputPreview;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.ToString;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

@AutoService(FormatFactory.class)
public class CSVFormat implements TextLineFormat {

  public static final String NAME = "csv";
  public static final List<String> EXTENSIONS = List.of(NAME);

  public static final String DEFAULT_DELIMITER = ",";
  public static final String DEFAULT_COMMENT = "#";

  @Override
  public Parser getParser(@NonNull SqrlConfig config) {
    return new CSVFormatParser(Configuration.of(config));
  }

  public static final String HEADER_KEY = "header";
  public static final String DELIMITER_KEY = "delimiter";

  public static class Configuration {

    @Default
    String delimiter = DEFAULT_DELIMITER;
    @Default
    String commentPrefix = DEFAULT_COMMENT;
    @NotEmpty
    List<String> header;

    static Configuration of(SqrlConfig config) {
      return config.allAs(Configuration.class).get();
    }

  }


  @NoArgsConstructor
  public static class CSVFormatParser implements TextLineFormat.Parser {

    private String[] header;
    private String delimiter;
    private String commentPrefix;
    private org.apache.commons.csv.CSVFormat format;

    public CSVFormatParser(Configuration config) {
      this.header = config.header.toArray(String[]::new);
      this.delimiter = config.delimiter;
      this.commentPrefix = config.commentPrefix;
      this.format = getDefaultFormat(delimiter);
    }


    @SneakyThrows
    @Override
    public Result parse(@NonNull String line) {
      if (isComment(line, commentPrefix)) {
        return Result.skip();
      }
      try (CSVParser parser = CSVParser.parse(line, format)) {
        for (CSVRecord parts : parser) {
          if (parts.size() > header.length) {
            return Result.error(
                String.format("Expected %d items per row but found %d", header.length,
                    parts.size()));
          }
          //Skip if line is equal to header
          boolean isHeader = true;
          for (int i = 0; i < parts.size(); i++) {
            if (!parts.get(i).equalsIgnoreCase(header[i])) {
              isHeader = false;
              break;
            }
          }
          if (isHeader) {
            return Result.skip();
          }
          LinkedHashMap<String, Object> map = new LinkedHashMap<>(parts.size());
          for (int i = 0; i < parts.size(); i++) {
            map.put(header[i], parts.get(i));
          }
          return Result.success(map);
        }
      }

      return Result.skip();
    }
  }

  private static org.apache.commons.csv.CSVFormat getDefaultFormat(String delimiter) {
    return org.apache.commons.csv.CSVFormat.DEFAULT
        .builder()
        .setDelimiter(delimiter.charAt(0))
        .setTrim(true)
        .build();
  }

  private static boolean isComment(String line, String commentPrefix) {
    return !Strings.isNullOrEmpty(commentPrefix) && line.startsWith(commentPrefix);

  }

  @Override
  public void inferConfig(@NonNull SqrlConfig config, @NonNull InputPreview inputPreview) {
    if (!config.containsKey(DELIMITER_KEY) || !config.containsKey(HEADER_KEY)) {
      //Try to infer
      FormatInference fci = new FormatInference();
      Inferer inferer = new Inferer(config.asString(DELIMITER_KEY).getOptional().orElse(null));
      fci.inferConfigFromText(inputPreview, inferer);
      if (inferer.foundHeader()) {
        if (!config.containsKey(DELIMITER_KEY)) config.setProperty(DELIMITER_KEY, inferer.delimiter);
        if (!config.containsKey(HEADER_KEY)) config.setProperty(HEADER_KEY, inferer.header);
      } else if (!config.containsKey(HEADER_KEY)) {
        config.getErrorCollector().fatal("Need to specify a csv header (could not be inferred)");
      }
    }
  }

  @Override
  public Writer getWriter(@NonNull SqrlConfig config) {
    throw new NotYetImplementedException("CSV writing not yet supported");
  }

  public static class Inferer implements FormatInference.Text {

    public static final String[] DELIMITER_CANDIDATES = new String[]{",", ";"};

    private String[] header;
    private String delimiter;

    private transient org.apache.commons.csv.CSVFormat format;

    public Inferer(String delimiter) {
      this.delimiter = delimiter;
    }

    @Override
    public double getConfidence() {
      return header == null ? 0 : 0.98;
    }

    boolean foundHeader() {
      return header != null && header.length > 0;
    }

    @Override
    public void nextSegment(@NonNull BufferedReader textInput) throws IOException {
      if (header != null) {
        return; //We already verified there is no header
      }
      String line = textInput.readLine();
      if (Strings.isNullOrEmpty(delimiter)) {
        //try to infer delimiter
        Pair<String, Integer> topScoringDelimiter = Arrays.stream(DELIMITER_CANDIDATES)
            .map(del -> Pair.of(del, StringUtils.countMatches(line, del)))
            .sorted((p1, p2) -> -Integer.compare(p1.getValue(), p2.getValue())).findFirst().get();
        if (topScoringDelimiter.getValue() > 0) {
          delimiter = topScoringDelimiter.getKey();
        } else {
          delimiter = DEFAULT_DELIMITER;
        }
      }

      if (format == null) {
        format = getDefaultFormat(delimiter);
      }

      try (CSVParser parser = CSVParser.parse(line, format)) {
        Optional<CSVRecord> first = parser.stream().findFirst();
        if (first.isPresent()) {
          //verify all header elements are proper strings
          boolean allProper = true;
          for (String s : first.get()) {
            if (s.isEmpty() || !Character.isLetter(s.charAt(0))) {
              allProper = false;
            }
          }
          if (allProper) {
            header = first.get().stream().toArray(size -> new String[size]);
          }
        }
      }
      if (header == null) {
        header = new String[0]; //Signal there is no header
      }
    }
  }

  @Override
  public List<String> getExtensions() {
    return EXTENSIONS;
  }

  @Override
  public String getName() {
    return NAME;
  }

}
