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
package com.datasqrl.planner;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.engine.stream.flink.FlinkCalciteParser;
import com.datasqrl.planner.StatementObjects.Catalog;
import com.datasqrl.planner.StatementObjects.Named;
import com.datasqrl.planner.parser.SqrlStatementParser;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.junit.jupiter.api.Test;

class StatementObjectsTest {

  private static final TableEnvironmentImpl TABLE_ENVIRONMENT =
      TableEnvironmentImpl.create(new Configuration());

  private final SqrlStatementParser parser = new SqrlStatementParser();

  @Test
  void givenFlinkStatements_whenRead_thenTheirObjectsComeFromTheFlinkParser() {
    assertThat(
            read("CREATE TEMPORARY FUNCTION IF NOT EXISTS myudf AS 'com.example.Udf';").defined())
        .containsExactly(new Named(List.of("myudf")));
    assertThat(read("CREATE DATABASE IF NOT EXISTS mydb;").defined())
        .containsExactly(new Catalog("mydb"));
    assertThat(read("CREATE CATALOG iceberg WITH ('type' = 'iceberg');").defined())
        .containsExactly(new Catalog("iceberg"));
    assertThat(read("INSERT INTO mydb.Sink SELECT id FROM Orders;").targets())
        .containsExactly(List.of("mydb", "Sink"));
  }

  @Test
  void givenUseStatements_whenRead_thenTheySetTheContext() {
    assertThat(read("USE CATALOG iceberg;").isUse()).isTrue();
    assertThat(read("/* the database */ USE mydb;").isUse()).isTrue();
    assertThat(read("USE MODULES core;").isUse()).isTrue();
    assertThat(read("CREATE DATABASE mydb;").isUse()).isFalse();
  }

  @Test
  void givenFlinkStatementThatFailsToParse_whenRead_thenItsObjectIsReadFromItsText() {
    assertThat(read("CREATE TABLE Broken (id INT WITH ('connector' = 'x');").defined())
        .containsExactly(new Named(List.of("Broken")));
  }

  @Test
  void givenStatementsThatFailedToParse_whenRead_thenOnlyColumnsAddColumns() {
    assertThat(StatementObjects.ofUnparsed("Orders.total := ;").addsColumn()).isTrue();
    assertThat(StatementObjects.ofUnparsed("Broken := DISTINCT Orders ON id junk;").addsColumn())
        .isFalse();
    assertThat(StatementObjects.ofUnparsed("IMPORT ecommerce.Product extra;").addsColumn())
        .isFalse();
    assertThat(StatementObjects.ofUnparsed("CREATE TABLE junk").addsColumn()).isFalse();
  }

  private StatementObjects read(String statement) {
    return StatementObjects.of(
        parser.parseStatement(statement),
        sql -> FlinkCalciteParser.parseSql(sql, TABLE_ENVIRONMENT));
  }
}
