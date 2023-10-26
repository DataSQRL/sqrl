/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
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
package com.datasqrl.parse;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.parse.SqlBaseParser.BackQuotedIdentifierContext;
import com.datasqrl.parse.SqlBaseParser.QuotedIdentifierContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.Value;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlStatement;

public class SqrlParserImpl implements SqrlParser {

  private final LexerErrorHandler lexerErrorHandler = new LexerErrorHandler();
  private final ParsingErrorHandler parsingErrorHandler = ParsingErrorHandler.builder()
      .specialRule(SqlBaseParser.RULE_expression, "<expression>")
      .specialRule(SqlBaseParser.RULE_identifier, "<identifier>")
      .specialRule(SqlBaseParser.RULE_type, "<type>")
      .specialToken(SqlBaseLexer.INTEGER_VALUE, "<integer>")
      .build();

  @SneakyThrows
  @Override
  public ScriptNode parse(Path scriptPath, ErrorCollector errors) {
    String scriptContent = Files.readString(scriptPath);
    errors = errors.withScript(scriptPath, scriptContent);
    ScriptNode scriptNode = parse(scriptContent, errors);
    scriptNode.setScriptPath(Optional.of(scriptPath));
    return scriptNode;
  }

  public ScriptNode parse(String sql, ErrorCollector errors) {
    try {
      ScriptNode scriptNode = (ScriptNode) invokeParser("script", sql, SqlBaseParser::script);
      scriptNode.setOriginalScript(sql);
      scriptNode.setScriptPath(Optional.empty());
      return scriptNode;
    } catch (Exception e) {
      if (errors.getLocation() == null || errors.getLocation().getSourceMap() == null) {
        errors = errors.withSchema("<schema>", sql);
      }
      throw errors.handle(e);
    }
  }

  public SqrlStatement parseStatement(String sql, ErrorCollector errors) {
    try {
      return (SqrlStatement) invokeParser("statement", sql, SqlBaseParser::singleStatement);
    } catch (Exception e) {
      throw errors.handle(e);
    }
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
    Pattern pattern = Pattern.compile("[_A-Za-z][_0-9A-Za-z]");

    @Override
    public void exitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext context) {
      noSymbols(
          context.IDENTIFIER().getText(),
          context.IDENTIFIER().getSymbol());
    }

    @Override
    public void exitBackQuotedIdentifier(BackQuotedIdentifierContext context) {
      noSymbols(
          context.BACKQUOTED_IDENTIFIER().getText(),
          context.BACKQUOTED_IDENTIFIER().getSymbol());
    }

    @Override
    public void exitQuotedIdentifier(QuotedIdentifierContext context) {
      noSymbols(
          context.QUOTED_IDENTIFIER().getText(),
          context.QUOTED_IDENTIFIER().getSymbol());
    }

    public void noSymbols(String identifier, Token token) {
      if (identifier.indexOf("$") > 0) {
        throw new ParsingException(
            "identifiers '" + identifier + "' must not contain special token ($).", null,
            token.getLine(),
            token.getCharPositionInLine());
      }
    }

    @Override
    public void exitNonReserved(SqlBaseParser.NonReservedContext context) {
      // we can't modify the tree during rule enter/exit event handling unless we're dealing with a terminal.
      // Otherwise, ANTLR gets confused an fires spurious notifications.
      if (!(context.getChild(0) instanceof TerminalNode)) {
        int rule = ((ParserRuleContext) context.getChild(0)).getRuleIndex();
        throw new AssertionError(
            "nonReserved can only contain tokens. Found nested rule: " + ruleNames.get(rule));
      }

      // replace nonReserved words with IDENT tokens
      context.getParent().removeLastChild();

      Token token = (Token) context.getChild(0).getPayload();

      context.getParent().addChild(new CommonToken(
          new Pair<>(token.getTokenSource(), token.getInputStream()),
          SqlBaseLexer.IDENTIFIER,
          token.getChannel(),
          token.getStartIndex(),
          token.getStopIndex()));
    }
  }
}
