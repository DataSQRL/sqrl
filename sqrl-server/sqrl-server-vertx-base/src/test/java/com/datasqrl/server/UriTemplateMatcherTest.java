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
package com.datasqrl.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class UriTemplateMatcherTest {

  @ParameterizedTest
  @CsvSource({
    "'queries/HighTempAlert{?offset,limit}', /queries/HighTempAlert",
    "/queries/{sensorid}/maxTemp, /queries/:sensorid/maxTemp",
    "users/{userId}/posts{?limit}, /users/:userId/posts",
    "mutations/SensorReading, /mutations/SensorReading"
  })
  void givenUriTemplate_whenToRoutePath_thenReturnsVertxRoute(String template, String route) {
    assertThat(UriTemplateMatcher.toRoutePath(template)).isEqualTo(route);
  }

  @Test
  void givenPathTemplate_whenUriMatches_thenReturnsDecodedPathParameters() {
    var matcher = new UriTemplateMatcher("sqrl://v1/queries/{name}/orders/{id}");

    var match = matcher.match("sqrl://v1/queries/Ada%20L+x/orders/42").orElseThrow();

    assertThat(match.pathParameters()).containsExactly(entry("name", "Ada L+x"), entry("id", "42"));
    assertThat(match.queryParameters()).isEmpty();
  }

  @Test
  void givenQueryTemplate_whenUriHasQuery_thenReturnsDecodedQueryParameters() {
    var matcher = new UriTemplateMatcher("sqrl://v1/queries/People{?limit,name}");

    var match =
        matcher.match("sqrl://v1/queries/People?limit=5&name=Ada+L%26x&limit=7").orElseThrow();

    assertThat(match.pathParameters()).isEmpty();
    assertThat(match.queryParameters())
        .containsExactly(entry("limit", "5"), entry("name", "Ada L&x"));
  }

  @Test
  void givenQueryTemplate_whenUriHasNoQuery_thenMatchesWithoutParameters() {
    var matcher = new UriTemplateMatcher("sqrl://v1/queries/People{?limit}");

    var match = matcher.match("sqrl://v1/queries/People").orElseThrow();

    assertThat(match.queryParameters()).isEmpty();
  }

  @Test
  void givenTemplateWithRegexCharacters_whenMatching_thenTreatsThemLiterally() {
    var matcher = new UriTemplateMatcher("sqrl://v1/a.b/{id}");

    assertThat(matcher.match("sqrl://v1/a.b/1")).isPresent();
    assertThat(matcher.match("sqrl://v1/aXb/1")).isEmpty();
  }

  @Test
  void givenPathTemplate_whenUriDiffers_thenDoesNotMatch() {
    var matcher = new UriTemplateMatcher("sqrl://v1/queries/personByAge/{age}");

    assertThat(matcher.match("sqrl://v1/queries/personByAge")).isEmpty();
    assertThat(matcher.match("sqrl://v1/queries/personByAge/30/extra")).isEmpty();
    assertThat(matcher.match("sqrl://v2/queries/personByAge/30")).isEmpty();
    assertThat(matcher.match("queries/personByAge/30")).isEmpty();
  }

  @Test
  void givenInvalidPercentEncoding_whenMatching_thenThrows() {
    var matcher = new UriTemplateMatcher("sqrl://v1/queries/{name}");

    assertThatThrownBy(() -> matcher.match("sqrl://v1/queries/%zz"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
