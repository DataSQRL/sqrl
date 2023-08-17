package com.datasqrl;

import com.datasqrl.SqrlFunctions.VariableArguments;
import com.datasqrl.function.SqrlFunction;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

public class TextFunctions {

  public static Format FORMAT = new Format();
  public static TextSearch TEXT_SEARCH = new TextSearch();
  public static BannedWordsFilter BANNED_WORDS_FILTER = new BannedWordsFilter();

  public static class Format extends ScalarFunction implements SqrlFunction {

    public String eval(String text, String... arguments) {
      if (text==null) return null;
      return String.format(text, (Object[]) arguments);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .inputTypeStrategy(VariableArguments.builder()
              .staticType(DataTypes.STRING())
              .variableType(DataTypes.STRING())
              .minVariableArguments(0)
              .maxVariableArguments(Integer.MAX_VALUE)
              .build())
          .outputTypeStrategy(
              SqrlFunctions.nullPreservingOutputStrategy(DataTypes.STRING()))
          .build();
    }

    @Override
    public String getDocumentation() {
      return "Replaces the placeholders in the first argument with the remaining arguments in order";
    }
  }

  public static class BannedWordsFilter extends ScalarFunction implements SqrlFunction {

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
      if (text==null) return null;
      StringTokenizer tokenizer = new StringTokenizer(text);
      while (tokenizer.hasMoreTokens()) {
        if (bannedWords.contains(tokenizer.nextToken().trim().toLowerCase())) return false;
      }
      return true;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return SqrlFunctions.basicNullInference(DataTypes.BOOLEAN(), DataTypes.STRING());
    }

    @Override
    public String getDocumentation() {
      return "Returns false if the given text contains a banned word, else true";
    }

  }

  public static class TextSearch extends ScalarFunction implements SqrlFunction {

    public Double eval(String query, String... texts) {
      if (query==null) return null;
      List<String> queryWords = new ArrayList<>();
      tokenizeTo(query, queryWords);
      if (queryWords.isEmpty()) return 1.0;

      Set<String> searchWords = new HashSet<>();
      Arrays.stream(texts).forEach(text -> tokenizeTo(text, searchWords));

      double score = 0;
      for (String queryWord : queryWords) {
        if (searchWords.contains(queryWord)) score += 1.0;
      }
      return score/ queryWords.size();
    }

    public static void tokenizeTo(String text, Collection<String> collection) {
      StringTokenizer tokenizer = new StringTokenizer(text);
      while (tokenizer.hasMoreTokens()) collection.add(tokenizer.nextToken().trim().toLowerCase());
    }


    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      return TypeInference.newBuilder()
          .inputTypeStrategy(VariableArguments.builder()
              .staticType(DataTypes.STRING())
              .variableType(DataTypes.STRING())
              .minVariableArguments(1)
              .maxVariableArguments(256)
              .build())
          .outputTypeStrategy(
              SqrlFunctions.nullPreservingOutputStrategy(DataTypes.DOUBLE()))
          .build();
    }

    @Override
    public String getDocumentation() {
      return "Replaces the placeholders in the first argument with the remaining arguments in order";
    }
  }


}
