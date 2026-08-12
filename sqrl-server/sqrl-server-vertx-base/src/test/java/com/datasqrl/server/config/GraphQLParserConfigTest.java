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
  void given_newConfig_when_created_then_nothingIsOverridden() {
    var config = new GraphQLParserConfig();

    assertThat(config.getMaxCharacters()).isNull();
    assertThat(config.getMaxTokens()).isNull();
    assertThat(config.getMaxWhitespaceTokens()).isNull();
    assertThat(config.getMaxRuleDepth()).isNull();
    assertThat(config.getCaptureIgnoredChars()).isNull();
    assertThat(config.getCaptureSourceLocation()).isNull();
    assertThat(config.getCaptureLineComments()).isNull();
    assertThat(config.getReaderTrackData()).isNull();
    assertThat(config.getRedactTokenParserErrorMessages()).isNull();
  }

  @Test
  void given_customFlags_when_appliedToParserDefaults_then_bothParserDefaultsAreUpdated() {
    var config = new GraphQLParserConfig();
    config.setCaptureIgnoredChars(true);
    config.setCaptureSourceLocation(false);
    config.setCaptureLineComments(true);
    config.setReaderTrackData(false);
    config.setRedactTokenParserErrorMessages(true);

    config.applyParserConfig();

    for (var options :
        new ParserOptions[] {
          ParserOptions.getDefaultParserOptions(), ParserOptions.getDefaultOperationParserOptions()
        }) {
      assertThat(options.isCaptureIgnoredChars()).isTrue();
      assertThat(options.isCaptureSourceLocation()).isFalse();
      assertThat(options.isCaptureLineComments()).isTrue();
      assertThat(options.isReaderTrackData()).isFalse();
      assertThat(options.isRedactTokenParserErrorMessages()).isTrue();
    }
  }

  /**
   * graphql-java defaults {@code captureLineComments} to true for generic parsing but false for
   * operations, so an unset flag must not flatten that difference.
   */
  @Test
  void given_unsetFlags_when_limitsAreApplied_then_graphQlJavaFlagDefaultsAreKeptPerParser() {
    var config = new GraphQLParserConfig();
    config.setMaxTokens(45_000);

    config.applyParserConfig();

    assertThat(ParserOptions.getDefaultParserOptions().isCaptureLineComments()).isTrue();
    assertThat(ParserOptions.getDefaultOperationParserOptions().isCaptureLineComments()).isFalse();
  }

  @Test
  void given_unsetLimits_when_flagIsApplied_then_graphQlJavaLimitDefaultsAreKept() {
    var config = new GraphQLParserConfig();
    config.setCaptureIgnoredChars(true);

    config.applyParserConfig();

    var applied = ParserOptions.getDefaultOperationParserOptions();
    assertThat(applied.getMaxCharacters()).isEqualTo(ParserOptions.MAX_QUERY_CHARACTERS);
    assertThat(applied.getMaxTokens()).isEqualTo(ParserOptions.MAX_QUERY_TOKENS);
    assertThat(applied.getMaxWhitespaceTokens()).isEqualTo(ParserOptions.MAX_WHITESPACE_TOKENS);
    assertThat(applied.getMaxRuleDepth()).isEqualTo(ParserOptions.MAX_RULE_DEPTH);
  }

  @Test
  void given_customLimits_when_appliedToParserDefaults_then_bothParserDefaultsAreUpdated() {
    var config = new GraphQLParserConfig();
    config.setMaxCharacters(2_000_000);
    config.setMaxTokens(45_000);
    config.setMaxWhitespaceTokens(400_000);
    config.setMaxRuleDepth(700);

    config.applyParserConfig();

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

    config.applyParserConfig();

    assertThat(ParserOptions.getDefaultSdlParserOptions().getMaxTokens())
        .isEqualTo(sdlOptions.getMaxTokens());
  }

  @Test
  void given_customLimits_when_appliedToParserDefaults_then_otherParserSettingsArePreserved() {
    var operationDefaults = ParserOptions.getDefaultOperationParserOptions();
    var config = new GraphQLParserConfig();
    config.setMaxTokens(45_000);

    config.applyParserConfig();

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
  void given_noOverrides_when_appliedToParserDefaults_then_parserDefaultsAreUnchanged() {
    var operationDefaults = ParserOptions.getDefaultOperationParserOptions();

    new GraphQLParserConfig().applyParserConfig();

    assertThat(ParserOptions.getDefaultOperationParserOptions()).isSameAs(operationDefaults);
  }
}
