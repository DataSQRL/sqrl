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

import static org.assertj.core.api.Assertions.assertThat;

import graphql.parser.ParserOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GraphQLParserConfigTest {

  private ParserOptions originalParserOptions;
  private ParserOptions originalOperationParserOptions;

  @BeforeEach
  void captureParserDefaults() {
    originalParserOptions = ParserOptions.getDefaultParserOptions();
    originalOperationParserOptions = ParserOptions.getDefaultOperationParserOptions();
  }

  @AfterEach
  void restoreParserDefaults() {
    ParserOptions.setDefaultParserOptions(originalParserOptions);
    ParserOptions.setDefaultOperationParserOptions(originalOperationParserOptions);
  }

  @Test
  void given_newConfig_when_created_then_limitsMatchGraphQlJavaDefaults() {
    var config = new GraphQLParserConfig();

    assertThat(config.getMaxCharacters()).isEqualTo(ParserOptions.MAX_QUERY_CHARACTERS);
    assertThat(config.getMaxTokens()).isEqualTo(ParserOptions.MAX_QUERY_TOKENS);
    assertThat(config.getMaxWhitespaceTokens()).isEqualTo(ParserOptions.MAX_WHITESPACE_TOKENS);
    assertThat(config.getMaxRuleDepth()).isEqualTo(ParserOptions.MAX_RULE_DEPTH);
  }

  @Test
  void given_customLimits_when_appliedToParserDefaults_then_bothParserDefaultsAreUpdated() {
    var config = new GraphQLParserConfig();
    config.setMaxCharacters(2_000_000);
    config.setMaxTokens(45_000);
    config.setMaxWhitespaceTokens(400_000);
    config.setMaxRuleDepth(700);

    config.applyToParserDefaults();

    for (var options :
        new ParserOptions[] {
          ParserOptions.getDefaultParserOptions(), ParserOptions.getDefaultOperationParserOptions()
        }) {
      assertThat(options.getMaxCharacters()).isEqualTo(2_000_000);
      assertThat(options.getMaxTokens()).isEqualTo(45_000);
      assertThat(options.getMaxWhitespaceTokens()).isEqualTo(400_000);
      assertThat(options.getMaxRuleDepth()).isEqualTo(700);
    }
  }

  @Test
  void given_customLimits_when_appliedToParserDefaults_then_sdlParserDefaultsAreUntouched() {
    var sdlOptions = ParserOptions.getDefaultSdlParserOptions();
    var config = new GraphQLParserConfig();
    config.setMaxTokens(45_000);

    config.applyToParserDefaults();

    assertThat(ParserOptions.getDefaultSdlParserOptions().getMaxTokens())
        .isEqualTo(sdlOptions.getMaxTokens());
  }

  @Test
  void given_customLimits_when_appliedToParserDefaults_then_otherParserSettingsArePreserved() {
    var operationDefaults = ParserOptions.getDefaultOperationParserOptions();
    var config = new GraphQLParserConfig();
    config.setMaxTokens(45_000);

    config.applyToParserDefaults();

    var applied = ParserOptions.getDefaultOperationParserOptions();
    assertThat(applied.isCaptureIgnoredChars())
        .isEqualTo(operationDefaults.isCaptureIgnoredChars());
    assertThat(applied.isCaptureLineComments())
        .isEqualTo(operationDefaults.isCaptureLineComments());
    assertThat(applied.isCaptureSourceLocation())
        .isEqualTo(operationDefaults.isCaptureSourceLocation());
    assertThat(applied.isRedactTokenParserErrorMessages())
        .isEqualTo(operationDefaults.isRedactTokenParserErrorMessages());
  }

  @Test
  void given_defaultLimits_when_appliedToParserDefaults_then_parserDefaultsAreUnchanged() {
    var operationDefaults = ParserOptions.getDefaultOperationParserOptions();

    new GraphQLParserConfig().applyToParserDefaults();

    assertThat(ParserOptions.getDefaultOperationParserOptions()).isSameAs(operationDefaults);
  }
}
