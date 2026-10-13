/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
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
package com.datasqrl.intellij;

import com.intellij.openapi.fileTypes.SyntaxHighlighterFactory;
import com.intellij.testFramework.PlatformTestUtil;
import com.intellij.testFramework.fixtures.BasePlatformTestCase;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.jetbrains.plugins.textmate.TextMateService;
import org.jetbrains.plugins.textmate.language.TextMateLanguageDescriptor;
import org.jetbrains.plugins.textmate.language.syntax.lexer.TextMateElementType;

/** Highlights scripts with the TextMate grammar that the plugin provides. */
public class SqrlHighlightingTest extends BasePlatformTestCase {

  private static final String SCRIPT =
      """
      IMPORT shop.sources AS src;
      /*+primary_key(id) */
      Orders := SELECT o.id, COUNT(*) AS n FROM src.Orders o WHERE o.status = 'open' -- latest
                GROUP BY o.id;
      Orders.items(lim INT) := SELECT * FROM Items i WHERE i.order_id = this.id LIMIT :lim;
      EXPORT Orders TO print.Orders;
      IMPORT connectors.police_{{variant}} AS ctl;
      """;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    TextMateService.getInstance().reloadEnabledBundles();
  }

  public void testScriptsAreHighlightedByTheGrammar() throws InterruptedException {
    assertEquals("source.sqrl", awaitGrammar().getRootScopeName().toString());

    var tokens = tokens(SCRIPT);

    assertTrue(tokens.toString(), tokens.contains("IMPORT: keyword.control.import.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("shop.sources: entity.name.namespace.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("src: entity.name.type.sqrl"));
    assertTrue(
        tokens.toString(), tokens.contains("primary_key: storage.type.annotation.hint.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("id: variable.parameter.hint.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("Orders: entity.name.type.sqrl"));
    assertTrue(tokens.toString(), tokens.contains(":=: keyword.operator.assignment.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("SELECT: keyword.other.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("COUNT: support.function.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("open': string.quoted.single.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("-- latest: comment.line.double-dash.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("Orders.items: entity.name.function.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("INT: storage.type.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("this: variable.language.this.sqrl"));
    assertTrue(tokens.toString(), tokens.contains(":lim: variable.parameter.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("EXPORT: keyword.control.export.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("print.Orders: entity.name.namespace.sqrl"));
    assertTrue(
        tokens.toString(), tokens.contains("connectors.police_: entity.name.namespace.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("{{variant}}: variable.other.template.sqrl"));
    assertTrue(tokens.toString(), tokens.contains("ctl: entity.name.type.sqrl"));
  }

  /** The grammar of SQRL, which the IDE may load in the background. */
  private static TextMateLanguageDescriptor awaitGrammar() throws InterruptedException {
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline) {
      var grammar = TextMateService.getInstance().getLanguageDescriptorByExtension("sqrl");
      if (grammar != null) {
        return grammar;
      }
      PlatformTestUtil.dispatchAllEventsInIdeEventQueue();
      Thread.sleep(50);
    }
    throw new AssertionError("The grammar of SQRL was not loaded");
  }

  /** The tokens of a script with the innermost scope of each. */
  private List<String> tokens(String text) {
    var file = myFixture.configureByText("main.sqrl", text);
    var lexer =
        SyntaxHighlighterFactory.getSyntaxHighlighter(
                file.getFileType(), getProject(), file.getVirtualFile())
            .getHighlightingLexer();
    var tokens = new ArrayList<String>();
    lexer.start(text);
    while (lexer.getTokenType() != null) {
      if (lexer.getTokenType() instanceof TextMateElementType element
          && element.getScope().getScopeName() != null) {
        var token = text.substring(lexer.getTokenStart(), lexer.getTokenEnd()).strip();
        if (!token.isEmpty()) {
          tokens.add(token + ": " + element.getScope().getScopeName());
        }
      }
      lexer.advance();
    }
    return tokens;
  }
}
