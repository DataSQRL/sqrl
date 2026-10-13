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
package com.datasqrl.compile.check;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.datasqrl.compile.check.ScriptText.NamePart;
import org.junit.jupiter.api.Test;

class ScriptTextTest {

  @Test
  void givenNameInCommentsAndStrings_whenFind_thenFindsItInTheQuery() {
    var text = "/* Orders */ -- Orders\nX := SELECT 'Orders' FROM `sources`.`Orders`;";
    var script = new ScriptText(text);

    var name = script.find("Orders", 0);

    assertThat(name)
        .hasValueSatisfying(
            parts ->
                assertThat(parts)
                    .containsExactly(
                        new NamePart("Orders", text.indexOf("`Orders`"), text.length() - 1)));
  }

  @Test
  void givenNameInNextStatement_whenFind_thenStopsAtTheEndOfTheStatement() {
    var script = new ScriptText("A := SELECT 1;\nOrders := SELECT 2;");

    assertThat(script.find("Orders", 0)).isEmpty();
  }

  @Test
  void givenQualifiedName_whenFind_thenMatchesAllParts() {
    var script = new ScriptText("Customers.orders := SELECT * FROM Orders;");

    assertThat(script.find("Customers.orders", 0))
        .hasValueSatisfying(
            parts ->
                assertThat(parts)
                    .extracting(NamePart::name, NamePart::start)
                    .containsExactly(tuple("Customers", 0), tuple("orders", 10)));
  }

  @Test
  void givenImportPathWithHyphens_whenPathStartingAt_thenItsPartsKeepThem() {
    var text = "IMPORT cluster-events.Metrics-api.Metrics AS m;";
    var script = new ScriptText(text);

    assertThat(script.pathStartingAt(text.indexOf("cluster")))
        .extracting(NamePart::name)
        .containsExactly("cluster-events", "Metrics-api", "Metrics");
    // In a query, `-` is the minus of SQL
    assertThat(script.nameStartingAt(text.indexOf("cluster")))
        .extracting(NamePart::name)
        .containsExactly("cluster");
  }

  @Test
  void givenOffsetInQualifiedName_whenNameAt_thenReturnsThePartsUpToTheOffset() {
    var text = "SELECT * FROM sources.Orders o";
    var script = new ScriptText(text);

    assertThat(script.nameAt(text.indexOf("Orders") + 2))
        .hasValueSatisfying(
            parts ->
                assertThat(parts).extracting(NamePart::name).containsExactly("sources", "Orders"));
    assertThat(script.nameAt(text.indexOf("sources") + 1))
        .hasValueSatisfying(
            parts -> assertThat(parts).extracting(NamePart::name).containsExactly("sources"));
    assertThat(script.nameAt(text.indexOf("*"))).isEmpty();
  }

  @Test
  void givenMultipleLines_whenConvertingPositions_thenRoundTrips() {
    var script = new ScriptText("ab\r\ncd\nef");

    assertThat(script.line(0)).isEqualTo("ab");
    assertThat(script.offset(1, 1)).isEqualTo(5);
    assertThat(script.lineOf(5)).isEqualTo(1);
    assertThat(script.characterOf(5)).isEqualTo(1);
    assertThat(script.offset(9, 0)).isEqualTo(9);
  }
}
