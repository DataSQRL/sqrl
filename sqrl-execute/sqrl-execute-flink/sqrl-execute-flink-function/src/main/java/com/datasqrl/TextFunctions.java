package com.datasqrl;

import com.datasqrl.SqrlFunctions.VariableArguments;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SimpleCallTransform;
import com.datasqrl.calcite.convert.SimplePredicateTransform;
import com.datasqrl.calcite.function.RuleTransform;
import com.datasqrl.function.IndexType;
import com.datasqrl.function.IndexableFunction;
import com.datasqrl.function.SqrlFunction;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
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

  public static class TextSearch extends ScalarFunction implements SqrlFunction, IndexableFunction,
      RuleTransform {

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
    public List<RelRule> transform(Dialect dialect, SqlOperator operator) {
      if (dialect != Dialect.POSTGRES) {
        return List.of();
      }

      return List.of(
          new SimplePredicateTransform(operator, (rexBuilder, predicate) -> {
            Preconditions.checkArgument(predicate.isA(SqlKind.BINARY_COMPARISON) && predicate.getOperands().size()==2,
                "Expected %s in comparison predicate but got: %s",
                getFunctionName(), predicate);
            RexNode other;
            RexCall textSearch;
            if (predicate.getOperands().get(0) instanceof RexCall) {
              textSearch = (RexCall) predicate.getOperands().get(0);
              other = predicate.getOperands().get(1);
              Preconditions.checkArgument(predicate.isA(SqlKind.GREATER_THAN),
                  "Expected a greater-than (>) comparison in %s predicate but got %s",
                  getFunctionName(), predicate);
            } else if (predicate.getOperands().get(1) instanceof RexCall) {
              textSearch = (RexCall) predicate.getOperands().get(1);
              other = predicate.getOperands().get(0);
              Preconditions.checkArgument(predicate.isA(SqlKind.LESS_THAN),
                  "Expected a less-than (<) comparison in %s predicate but got %s",
                  getFunctionName(), predicate);
            } else {
              throw new IllegalArgumentException("Not a valid predicate");
            }
            Preconditions.checkArgument(FunctionUtil.getSqrlFunction(textSearch.getOperator())
                .filter(fct -> fct.equals(TEXT_SEARCH)).isPresent(),
                "Not a valid %s predicate", getFunctionName());
            //TODO generalize to other literals by adding ts_rank_cd to the filter condition
            Preconditions.checkArgument(other instanceof RexLiteral &&
                ((RexLiteral)other).getValueAs(Number.class).doubleValue()==0, "Expected comparison with 0 for %s", getFunctionName());
            //TODO: allow other languages
            RexLiteral language = rexBuilder.makeLiteral("english");
            List<RexNode> operands = textSearch.getOperands();
            Preconditions.checkArgument(operands.size()>1);

            //to_tsvector(col1  ' '  coalesce(col2,'')) @@ to_tsquery(:query) AND ts_rank_cd(col1..., :query) > 0.1
            return rexBuilder.makeCall(TsVectorOperatorTable.MATCH, makeTsVector(rexBuilder, language, operands.subList(1,
                    operands.size())), makeTsQuery(rexBuilder, language, operands.get(0)));
          }),
          new SimpleCallTransform(operator, (rexBuilder, call) -> {
            Preconditions.checkArgument(FunctionUtil.getSqrlFunction(call.getOperator())
                    .filter(fct -> fct.equals(TEXT_SEARCH)).isPresent(),
                "Not a valid %s predicate", getFunctionName());
            RexLiteral language = rexBuilder.makeLiteral("english");
            List<RexNode> operands = call.getOperands();
            Preconditions.checkArgument(operands.size()>1);
            return rexBuilder.makeCall(TsVectorOperatorTable.TS_RANK_CD, makeTsVector(rexBuilder, language, operands.subList(1,
                  operands.size())), makeTsQuery(rexBuilder, language, operands.get(0)));
          })
      );
    }

    private static RexNode makeTsQuery(RexBuilder rexBuilder, RexLiteral language, RexNode query) {
      return rexBuilder.makeCall(TsVectorOperatorTable.TO_WEBQUERY, language, query);
    }

    private static RexNode makeTsVector(RexBuilder rexBuilder, RexLiteral language, List<RexNode> columns) {
      RexLiteral space = rexBuilder.makeLiteral(" ");
      List<RexNode> args = columns.stream().map(col -> rexBuilder.makeCall(SqlStdOperatorTable.COALESCE,col, space))
              .collect(Collectors.toList());
      RexNode arg;
      if (args.size()>1) {
        arg = args.get(0);
        for (int i = 1; i < args.size(); i++) {
          arg = rexBuilder.makeCall(SqlStdOperatorTable.CONCAT, rexBuilder.makeCall(SqlStdOperatorTable.CONCAT, arg, space), args.get(i));
        }
      } else {
        arg = args.get(0);
      }
      return rexBuilder.makeCall(TsVectorOperatorTable.TO_TSVECTOR, language, arg);
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

    @Override
    public Predicate<Integer> getOperandSelector() {
      return idx -> idx>0;
    }

    @Override
    public double estimateSelectivity() {
      return 0.1;
    }

    @Override
    public EnumSet<IndexType> getSupportedIndexes() {
      return EnumSet.of(IndexType.TEXT);
    }
  }


}
