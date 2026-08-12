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
package com.datasqrl.server.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import graphql.parser.ParserOptions;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Safety limits graphql-java applies while parsing incoming GraphQL documents. Exceeding any of
 * them aborts parsing with a "To prevent Denial Of Service attacks, parsing has been cancelled"
 * error.
 *
 * <p>Defaults mirror graphql-java's own defaults, so behaviour is unchanged when the {@code
 * graphQLParserConfig} section is absent from {@code vertx-config.json}.
 */
@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Slf4j
public class GraphQLParserConfig {

  private int maxCharacters = ParserOptions.MAX_QUERY_CHARACTERS;
  private int maxTokens = ParserOptions.MAX_QUERY_TOKENS;
  private int maxWhitespaceTokens = ParserOptions.MAX_WHITESPACE_TOKENS;
  private int maxRuleDepth = ParserOptions.MAX_RULE_DEPTH;

  /**
   * Applies these limits to the JVM-wide graphql-java parser defaults. Both the generic and the
   * operation defaults are updated because query parsing resolves its options from {@link
   * ParserOptions#getDefaultOperationParserOptions()}. SDL parsing keeps its own, much higher
   * defaults and is deliberately left untouched.
   */
  public void applyParserConfig() {
    if (isDefault()) {
      return;
    }

    log.info(
        "Applying custom GraphQL parser limits: maxCharacters={}, maxTokens={}, maxWhitespaceTokens={}, maxRuleDepth={}",
        maxCharacters,
        maxTokens,
        maxWhitespaceTokens,
        maxRuleDepth);

    var updatedParserOptions = withCustomConfig(ParserOptions.getDefaultParserOptions());
    ParserOptions.setDefaultParserOptions(updatedParserOptions);

    var updatedOperationParserOptions =
        withCustomConfig(ParserOptions.getDefaultOperationParserOptions());
    ParserOptions.setDefaultOperationParserOptions(updatedOperationParserOptions);
  }

  private boolean isDefault() {
    return maxCharacters == ParserOptions.MAX_QUERY_CHARACTERS
        && maxTokens == ParserOptions.MAX_QUERY_TOKENS
        && maxWhitespaceTokens == ParserOptions.MAX_WHITESPACE_TOKENS
        && maxRuleDepth == ParserOptions.MAX_RULE_DEPTH;
  }

  private ParserOptions withCustomConfig(ParserOptions options) {
    return options.transform(
        builder ->
            builder
                .maxCharacters(maxCharacters)
                .maxTokens(maxTokens)
                .maxWhitespaceTokens(maxWhitespaceTokens)
                .maxRuleDepth(maxRuleDepth));
  }
}
