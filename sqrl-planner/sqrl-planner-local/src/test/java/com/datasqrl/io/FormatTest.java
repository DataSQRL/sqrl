/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.formats.JsonLineFormat;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.util.FileStreamUtil;
import com.datasqrl.util.data.BookClub;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class FormatTest {

  @Test
  public void testJson() {
    List<SourceRecord.Raw> input = parseStream(FileStreamUtil.filesByline(BookClub.BOOK_FILES),
        new JsonLineFormat.JsonLineParser()).collect(Collectors.toList());
    testInput(input, 4, 2, 2);
  }

  public static void testInput(List<SourceRecord.Raw> input, int total, int minNumFields,
      int maxNumFields) {
    assertEquals(total, input.size());
    if (total > 0) {
      int min = input.stream().map(r -> r.getData().size()).min(Integer::compareTo).get();
      int max = input.stream().map(r -> r.getData().size()).max(Integer::compareTo).get();
      assertTrue(min >= minNumFields, "Min num fields: " + min);
      assertTrue(max <= maxNumFields, "Max num fields: " + max);
    }
  }

  public static Stream<SourceRecord.Raw> parseStream(Stream<String> textSource,
      TextLineFormat.Parser textparser) {
    return textSource.map(s -> {
          FormatFactory.Parser.Result result = textparser.parse(s);
          if (!result.isSuccess()) {
            throw new RuntimeException(
                String.format("Could not parse line [%s] due to error: %s", s,
                    result.getErrorMsg()));
          }
          return result;
        })
        .map(r -> new SourceRecord.Raw(r.getRecord(), r.getSourceTime()));
  }


}
