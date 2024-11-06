/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datasqrl.jdbc;

import com.datasqrl.type.JdbcTypeSerializer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.databases.postgres.dialect.PostgresDialect;
import org.apache.flink.connector.jdbc.databases.postgres.dialect.PostgresRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect.Range;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

/** JDBC dialect for PostgreSQL.
 *
 * SQRL: Add quoting to identifiers
 */
public class SqrlPostgresDialect extends PostgresDialect {

    private static final long serialVersionUID = 1L;

    @Override
    public PostgresRowConverter getRowConverter(RowType rowType) {
        return new SqrlPostgresRowConverter(rowType);
    }

    @Override
    public void validate(RowType rowType) throws ValidationException {
        List<LogicalType> unsupportedTypes = rowType.getFields().stream()
            .map(RowField::getType)
            .filter(type -> LogicalTypeRoot.RAW.equals(type.getTypeRoot()))
            .filter(type -> !isSupportedType(type))
            .collect(Collectors.toList());

        if (!unsupportedTypes.isEmpty()) {
            throw new ValidationException(String.format("The %s dialect doesn't support type: %s.", this.dialectName(),
                unsupportedTypes));
        }

        super.validate(rowType);
    }

    private boolean isSupportedType(LogicalType type) {
        for (JdbcTypeSerializer serializer : SqrlPostgresRowConverter.sqrlSerializers) {
            if (serializer.supportsType(type)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        // The data types used in PostgreSQL are list at:
        // https://www.postgresql.org/docs/12/datatype.html

        Set<LogicalTypeRoot> logicalTypeRoots = new HashSet<>(super.supportedTypes());
        logicalTypeRoots.add(LogicalTypeRoot.RAW); //see validate
        logicalTypeRoots.add(LogicalTypeRoot.MAP);
        logicalTypeRoots.add(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

        return logicalTypeRoots;
    }
}
