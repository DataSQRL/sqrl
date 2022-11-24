/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.datasqrl.parse;

import ai.datasqrl.parse.SqlBaseParser.BetweenContext;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import lombok.Value;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlStatement;

public class SqrlParser {
  private final LexerErrorHandler lexerErrorHandler = new LexerErrorHandler();
  private final ParsingErrorHandler parsingErrorHandler = ParsingErrorHandler.builder()
      .specialRule(SqlBaseParser.RULE_expression, "<expression>")
      .specialRule(SqlBaseParser.RULE_booleanExpression, "<expression>")
      .specialRule(SqlBaseParser.RULE_valueExpression, "<expression>")
      .specialRule(SqlBaseParser.RULE_primaryExpression, "<expression>")
      .specialRule(SqlBaseParser.RULE_identifier, "<identifier>")
      .specialRule(SqlBaseParser.RULE_string, "<string>")
      .specialRule(SqlBaseParser.RULE_query, "<query>")
      .specialRule(SqlBaseParser.RULE_type, "<type>")
      .specialToken(SqlBaseLexer.INTEGER_VALUE, "<integer>")
//      .ignoredRule(ai.datasqrl.sqml.parser.SqlBaseParser.RULE_nonReserved)
      .build();

  public static SqrlParser newParser() {
    return new SqrlParser();
  }

  public ScriptNode parse(String sql) {
    return (ScriptNode) invokeParser("script", sql, SqlBaseParser::script);
  }

  public SqrlStatement parseStatement(String sql) {
    return (SqrlStatement) invokeParser("statement", sql, SqlBaseParser::singleStatement);
  }

  private SqlNode invokeParser(String name, String sql,
      Function<SqlBaseParser, ParserRuleContext> parseFunction) {
    try {
      SqlBaseLexer lexer = new SqlBaseLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
      CommonTokenStream tokenStream = new CommonTokenStream(lexer);
      SqlBaseParser parser = new SqlBaseParser(tokenStream);

      // Override the default error strategy to not attempt inserting or deleting a token.
      // Otherwise, it messes up error reporting
      parser.setErrorHandler(new DefaultErrorStrategy() {
        @Override
        public Token recoverInline(Parser recognizer)
            throws RecognitionException {
          if (nextTokensContext == null) {
            throw new InputMismatchException(recognizer);
          } else {
            throw new InputMismatchException(recognizer, nextTokensState, nextTokensContext);
          }
        }
      });

      parser.addParseListener(new PostProcessor(Arrays.asList(parser.getRuleNames())));

      lexer.removeErrorListeners();
      lexer.addErrorListener(lexerErrorHandler);

      parser.removeErrorListeners();

      parser.addErrorListener(parsingErrorHandler);

      ParserRuleContext tree;
      parser.getInterpreter().setPredictionMode(PredictionMode.LL);
      tree = parseFunction.apply(parser);

      return new AstBuilder().visit(tree);
    } catch (StackOverflowError e) {
      e.printStackTrace();
      throw new ParsingException(name + " is too large (stack overflow while parsing)");
    }
  }

  @Value
  private class PostProcessor
      extends SqlBaseBaseListener {

    private final List<String> ruleNames;

    @Override
    public void exitBetween(BetweenContext ctx) {
      super.exitBetween(ctx);
    }
  }
}
