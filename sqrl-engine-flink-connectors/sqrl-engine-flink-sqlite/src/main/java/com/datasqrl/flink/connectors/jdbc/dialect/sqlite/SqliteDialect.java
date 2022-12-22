/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datasqrl.flink.connectors.jdbc.dialect.sqlite;

import java.util.EnumSet;
import java.util.Set;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqliteDialect extends AbstractDialect {
  private static final long serialVersionUID = 1L;
  private static final int MAX_TIMESTAMP_PRECISION = 6;
  private static final int MIN_TIMESTAMP_PRECISION = 1;
  private static final int MAX_DECIMAL_PRECISION = 1000;
  private static final int MIN_DECIMAL_PRECISION = 1;

  @Override
  public String dialectName() {
    return "sqlite";
  }

  public Optional<AbstractDialect.Range> decimalPrecisionRange() {
    return Optional.of(Range.of(MIN_DECIMAL_PRECISION, MAX_DECIMAL_PRECISION));
  }

  public Optional<AbstractDialect.Range> timestampPrecisionRange() {
    return Optional.of(Range.of(MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
  }
  @Override
  public Optional<String> defaultDriverName() {
    return Optional.of("org.sqlite.JDBC");
  }

  @Override
  public JdbcRowConverter getRowConverter(RowType rowType) {
    return new SqliteJdbcRowConverter(rowType);
  }

  @Override
  public String getLimitClause(long limit) {
    return "LIMIT " + limit;
  }


  @Override
  public String quoteIdentifier(String identifier) {
    return "`" + identifier + "`";
  }

  @Override
  public Optional<String> getUpsertStatement(String tableName, String[] fieldNames,
      String[] uniqueKeyFields) {
    String updateClause = Arrays.stream(fieldNames)
        .map(
            fieldName -> quoteIdentifier(fieldName) + "=excluded." + quoteIdentifier(fieldName) + "")
        .collect(Collectors.joining(", "));

    String conflictFields = Arrays.stream(uniqueKeyFields)
        .map(this::quoteIdentifier)
        .collect(Collectors.joining(","));

    String upsertSQL =
        getInsertIntoStatement(tableName, fieldNames) + " ON CONFLICT(" + conflictFields
            + ") DO UPDATE SET " + updateClause;
    return Optional.of(upsertSQL);
  }

  @Override
  public Set<LogicalTypeRoot> supportedTypes() {
    return EnumSet.of(
        LogicalTypeRoot.CHAR,
        LogicalTypeRoot.VARCHAR,
        LogicalTypeRoot.BOOLEAN,
        LogicalTypeRoot.VARBINARY,
        LogicalTypeRoot.DECIMAL,
        LogicalTypeRoot.TINYINT,
        LogicalTypeRoot.SMALLINT,
        LogicalTypeRoot.INTEGER,
        LogicalTypeRoot.BIGINT,
        LogicalTypeRoot.FLOAT,
        LogicalTypeRoot.DOUBLE,
        LogicalTypeRoot.DATE,
        LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
        LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
        LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        LogicalTypeRoot.ARRAY);
  }
}
