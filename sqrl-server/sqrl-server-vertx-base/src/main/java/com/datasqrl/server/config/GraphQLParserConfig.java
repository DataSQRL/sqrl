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
import java.util.function.Consumer;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Overrides for everything graphql-java exposes on {@link ParserOptions} when parsing incoming
 * GraphQL documents. The four limits guard against Denial Of Service attacks; exceeding any of them
 * aborts parsing with a "To prevent Denial Of Service attacks, parsing has been cancelled" error.
 *
 * <p>Every setting is a boxed type rather than a primitive so that "not specified" is
 * representable. graphql-java does not use the same value for every kind of parsing — {@code
 * captureLineComments}, for instance, defaults to {@code true} for generic parsing but {@code
 * false} for operations — so this class holds only what the user actually set and leaves the rest
 * to graphql-java's own default for the parser being configured. Behaviour is therefore unchanged
 * when the {@code graphQLParserConfig} section is absent from {@code vertx-config.json}.
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

  private Integer maxCharacters;
  private Integer maxTokens;
  private Integer maxWhitespaceTokens;
  private Integer maxRuleDepth;

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
    if (!hasOverrides()) {
      return;
    }

    log.info("Applying custom GraphQL parser config: {}", this);

    var updatedParserOptions = withCustomConfig(ParserOptions.getDefaultParserOptions());
    ParserOptions.setDefaultParserOptions(updatedParserOptions);

    var updatedOperationParserOptions =
        withCustomConfig(ParserOptions.getDefaultOperationParserOptions());
    ParserOptions.setDefaultOperationParserOptions(updatedOperationParserOptions);
  }

  private boolean hasOverrides() {
    return maxCharacters != null
        || maxTokens != null
        || maxWhitespaceTokens != null
        || maxRuleDepth != null
        || captureIgnoredChars != null
        || captureSourceLocation != null
        || captureLineComments != null
        || readerTrackData != null
        || redactTokenParserErrorMessages != null;
  }

  private ParserOptions withCustomConfig(ParserOptions options) {
    return options.transform(
        builder -> {
          applyIfSet(maxCharacters, builder::maxCharacters);
          applyIfSet(maxTokens, builder::maxTokens);
          applyIfSet(maxWhitespaceTokens, builder::maxWhitespaceTokens);
          applyIfSet(maxRuleDepth, builder::maxRuleDepth);
          applyIfSet(captureIgnoredChars, builder::captureIgnoredChars);
          applyIfSet(captureSourceLocation, builder::captureSourceLocation);
          applyIfSet(captureLineComments, builder::captureLineComments);
          applyIfSet(readerTrackData, builder::readerTrackData);
          applyIfSet(redactTokenParserErrorMessages, builder::redactTokenParserErrorMessages);
        });
  }

  private static <T> void applyIfSet(T value, Consumer<T> setter) {
    if (value != null) {
      setter.accept(value);
    }
  }
}
