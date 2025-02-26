package com.datasqrl.text;

import com.datasqrl.function.FlinkTypeUtil;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

/** Returns false if the given text contains a banned word, else true */
public class BannedWordsFilter extends ScalarFunction {

  private static final String BANNED_WORDS_FILENAME = "banned_words_list.txt";

  private Set<String> bannedWords;

  @Override
  public void open(FunctionContext context) throws Exception {
    bannedWords = new HashSet<>();
    try (InputStream inputStream = getClass().getResourceAsStream(BANNED_WORDS_FILENAME);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String currentLine;
      while ((currentLine = reader.readLine()) != null) {
        bannedWords.add(currentLine);
      }
    } catch (IOException | NullPointerException e) {
      throw new RuntimeException(e);
    }
  }

  public Boolean eval(String text) {
    if (text == null) {
      return null;
    }
    StringTokenizer tokenizer = new StringTokenizer(text);
    while (tokenizer.hasMoreTokens()) {
      if (bannedWords.contains(tokenizer.nextToken().trim().toLowerCase())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return FlinkTypeUtil.basicNullInference(DataTypes.BOOLEAN(), DataTypes.STRING());
  }
}
