package com.datasqrl.text;

import com.datasqrl.function.FlinkTypeUtil;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.net.URL;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

public class BannedWordsFilter extends ScalarFunction {

  private static final String BANNED_WORDS_FILENAME = "banned_words_list.txt";

  private final Set<String> bannedWords;

  public BannedWordsFilter() {
    URL url = com.google.common.io.Resources.getResource(BANNED_WORDS_FILENAME);
    try {
      String allWords = com.google.common.io.Resources.toString(url, Charsets.UTF_8);
      bannedWords = ImmutableSet.copyOf(allWords.split("\\n"));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
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
