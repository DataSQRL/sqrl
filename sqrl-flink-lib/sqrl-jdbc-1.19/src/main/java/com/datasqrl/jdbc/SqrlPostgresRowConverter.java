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
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.connector.jdbc.databases.postgres.dialect.PostgresRowConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * PostgreSQL.
 *
 * SQRL: Adds additional serializers
 */
public class SqrlPostgresRowConverter extends PostgresRowConverter {

    private static final long serialVersionUID = 1L;

    public static final List<JdbcTypeSerializer> sqrlSerializers = discoverSerializers();

    private static List<JdbcTypeSerializer> discoverSerializers() {
        return ServiceLoader.load(JdbcTypeSerializer.class)
            .stream()
            .map(f->f.get())
            .filter(f->f.getDialectId().equalsIgnoreCase("postgres"))
            .collect(Collectors.toList());
    }

    public SqrlPostgresRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        if (sqrlSupportsType(type)) {
            return getSqrlType(type)
                .getDeserializerConverter().create();
        }

        return super.createInternalConverter(type);
    }

    protected AbstractJdbcRowConverter.JdbcSerializationConverter createNullableExternalConverter(LogicalType type) {
        //SQRL: Remove array type check
        return this.wrapIntoNullableExternalConverter(this.createExternalConverter(type), type);
    }

    private JdbcTypeSerializer<JdbcDeserializationConverter, JdbcSerializationConverter> getSqrlType(LogicalType type) {
        for (JdbcTypeSerializer serializer : sqrlSerializers) {
            if (serializer.supportsType(type)) {
                return serializer;
            }
        }
        throw new RuntimeException("Could not find sqrl postgres type serializer");
    }

    private boolean sqrlSupportsType(LogicalType type) {
      return sqrlSerializers.stream().anyMatch(serializer -> serializer.supportsType(type));
    }

    @Override
    protected JdbcSerializationConverter wrapIntoNullableExternalConverter(
        JdbcSerializationConverter jdbcSerializationConverter, LogicalType type) {
        // SQRL serializers must account for null handling
        if (sqrlSupportsType(type)) {
            return jdbcSerializationConverter;
        }

        return super.wrapIntoNullableExternalConverter(jdbcSerializationConverter, type);
    }

    @Override
    protected JdbcSerializationConverter createExternalConverter(LogicalType type) {
        if (sqrlSupportsType(type)) {
            return getSqrlType(type)
                .getSerializerConverter(type)
                .create();
        }
        return super.createExternalConverter(type);
    }
}
