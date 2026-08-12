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
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Everything graphql-java exposes on {@link ParserOptions} for parsing incoming GraphQL documents.
 *
 * <p>The four limits guard against Denial Of Service attacks; exceeding any of them aborts parsing
 * with a "To prevent Denial Of Service attacks, parsing has been cancelled" error. Their defaults
 * mirror graphql-java's own constants.
 *
 * <p>The remaining flags are {@link Boolean} rather than {@code boolean} because graphql-java does
 * not use the same value for every kind of parsing — {@code captureLineComments}, for instance,
 * defaults to {@code true} for generic parsing but {@code false} for operations. Leaving one unset
 * (null) keeps whatever graphql-java itself defaults to for the parser being configured, so
 * behaviour is unchanged when the {@code graphQLParserConfig} section is absent from {@code
 * vertx-config.json}.
 *
 * <p>{@code parsingListener} is deliberately not exposed: it takes a callback implementation, which
 * cannot be expressed in a config file.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
@Slf4j
public class GraphQLParserConfig {

  private int maxCharacters = ParserOptions.MAX_QUERY_CHARACTERS;
  private int maxTokens = ParserOptions.MAX_QUERY_TOKENS;
  private int maxWhitespaceTokens = ParserOptions.MAX_WHITESPACE_TOKENS;
  private int maxRuleDepth = ParserOptions.MAX_RULE_DEPTH;

  private Boolean captureIgnoredChars;
  private Boolean captureSourceLocation;
  private Boolean captureLineComments;
  private Boolean readerTrackData;
  private Boolean redactTokenParserErrorMessages;

  /**
   * Applies this configuration to the JVM-wide graphql-java parser defaults. Both the generic and
   * the operation defaults are updated because query parsing resolves its options from {@link
   * ParserOptions#getDefaultOperationParserOptions()}. SDL parsing keeps its own, much higher
   * defaults and is deliberately left untouched.
   */
  public void applyParserConfig() {
    if (isDefault()) {
      return;
    }

    log.info("Applying custom GraphQL parser config: {}", this);

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
        && maxRuleDepth == ParserOptions.MAX_RULE_DEPTH
        && captureIgnoredChars == null
        && captureSourceLocation == null
        && captureLineComments == null
        && readerTrackData == null
        && redactTokenParserErrorMessages == null;
  }

  private ParserOptions withCustomConfig(ParserOptions options) {
    return options.transform(
        builder -> {
          builder
              .maxCharacters(maxCharacters)
              .maxTokens(maxTokens)
              .maxWhitespaceTokens(maxWhitespaceTokens)
              .maxRuleDepth(maxRuleDepth);

          if (captureIgnoredChars != null) {
            builder.captureIgnoredChars(captureIgnoredChars);
          }
          if (captureSourceLocation != null) {
            builder.captureSourceLocation(captureSourceLocation);
          }
          if (captureLineComments != null) {
            builder.captureLineComments(captureLineComments);
          }
          if (readerTrackData != null) {
            builder.readerTrackData(readerTrackData);
          }
          if (redactTokenParserErrorMessages != null) {
            builder.redactTokenParserErrorMessages(redactTokenParserErrorMessages);
          }
        });
  }
}
