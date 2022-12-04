/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.formats;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.impl.InputPreview;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import lombok.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

public class CSVFormat implements TextLineFormat<CSVFormat.Configuration> {

  public static final FileFormat FORMAT = FileFormat.CSV;
  public static final String NAME = "csv";

  @Override
  public Parser getParser(Configuration config) {
    return new CSVFormatParser(config);
  }

  @Override
  public Configuration getDefaultConfiguration() {
    return new Configuration();
  }


  @NoArgsConstructor
  public static class CSVFormatParser implements TextLineFormat.Parser {

    private String[] header;
    private String delimiter;
    private String commentPrefix;

    private transient Splitter splitter;

    public CSVFormatParser(Configuration config) {
      Preconditions.checkArgument(config.header != null && config.header.length > 0);
      Preconditions.checkArgument(!Strings.isNullOrEmpty(config.delimiter));
      this.header = config.header;
      this.delimiter = config.delimiter;
      this.commentPrefix = config.commentPrefix;
    }


    @Override
    public Result parse(@NonNull String line) {
      if (isComment(line, commentPrefix)) {
        return Result.skip();
      }
      if (splitter == null) {
        splitter = getSplitter(delimiter);
      }
      List<String> parts = splitter.splitToList(line);
      if (parts.size() > header.length) {
        return Result.error(
            String.format("Expected %d items per row but found %d", header.length, parts.size()));
      }
      //Skip if line is equal to header
      boolean isHeader = true;
      for (int i = 0; i < parts.size(); i++) {
        if (!parts.get(i).equalsIgnoreCase(header[i])) {
          isHeader = false;
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

  private static Splitter getSplitter(String delimiter) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(delimiter));
    return Splitter.on(delimiter).trimResults();
  }

  private static boolean isComment(String line, String commentPrefix) {
    return !Strings.isNullOrEmpty(commentPrefix) && line.startsWith(commentPrefix);

  }

  @Override
  public Writer getWriter(Configuration configuration) {
    return new CSVWriter();
  }

  public static class CSVWriter implements TextLineFormat.Writer {

  }

  public static class Inferer implements TextLineFormat.ConfigurationInference<Configuration> {

    public static final String[] DELIMITER_CANDIDATES = new String[]{",", ";"};

    private String[] header;
    private String delimiter;

    private transient Splitter splitter;

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

      if (splitter == null) {
        splitter = getSplitter(delimiter);
      }

      List<String> h = splitter.splitToList(line);
      if (!h.isEmpty()) {
        //verify all header elements are proper strings
        boolean allProper = true;
        for (String s : h) {
          if (s.isEmpty() || !Character.isLetter(s.charAt(0))) {
            allProper = false;
          }
        }
        if (allProper) {
          header = h.toArray(new String[h.size()]);
        }
      }
      if (header == null) {
        header = new String[0]; //Signal there is no header
      }
    }
  }

  public static final String DEFAULT_DELIMITER = ",";

  @NoArgsConstructor
  @AllArgsConstructor
  @ToString
  @Builder
  @Getter
  public static class Configuration implements FormatConfiguration {

    private String delimiter;
    private String commentPrefix;
    private String[] header;

    public static Configuration getDefault() {
      return builder().build();
    }

    @Override
    public boolean initialize(InputPreview preview, @NonNull ErrorCollector errors) {
      if (header == null || header.length == 0 || Strings.isNullOrEmpty(delimiter)) {
        if (preview != null) {
          //Try to infer
          FormatConfigInferer fci = new FormatConfigInferer();
          Inferer inferer = new Inferer(delimiter);
          fci.inferConfig(preview, inferer);
          if (inferer.foundHeader()) {
            header = inferer.header;
            delimiter = inferer.delimiter;
          }
        }
      }
      if (Strings.isNullOrEmpty(delimiter)) {
        delimiter = DEFAULT_DELIMITER;
      }
      if (header == null || header.length == 0) {
        errors.fatal("Need to specify a header (could not be inferred)");
        return false;
      }
      return true;
    }

    @Override
    public FileFormat getFileFormat() {
      return FORMAT;
    }

    @Override
    public Format getImplementation() {
      return new CSVFormat();
    }

    @Override
    public String getName() {
      return NAME;
    }
  }

}
