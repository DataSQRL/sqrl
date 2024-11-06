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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectLoader;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSource;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;

import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.COMPATIBLE_MODE;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.DRIVER;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.MAX_RETRY_TIMEOUT;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.URL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.USERNAME;

/**
 * Factory for creating configured instances of {@link JdbcDynamicTableSource} and {@link
 * JdbcDynamicTableSink}.
 */
@Internal
public class SqrlJdbcDynamicTableFactory extends JdbcDynamicTableFactory {

    public static final String IDENTIFIER = "jdbc-sqrl";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
            FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();
        invokeMethod(
            "validateConfigOptions", config, context.getClassLoader());

        validateDataTypeWithJdbcDialect(
            context.getPhysicalRowDataType(),
            config.get(URL),
            config.get(COMPATIBLE_MODE),
            context.getClassLoader());

        InternalJdbcConnectionOptions jdbcOptions =
            getJdbcOptions(config, context.getClassLoader());

        JdbcExecutionOptions jdbcExecutionOptions = (JdbcExecutionOptions) invokeMethod(
            "getJdbcExecutionOptions",
            config);
        JdbcDmlOptions jdbcDmlOptions = (JdbcDmlOptions) invokeMethod(
            "getJdbcDmlOptions",
            jdbcOptions,
            context.getPhysicalRowDataType(),
            context.getPrimaryKeyIndexes());

        return new JdbcDynamicTableSink(
            jdbcOptions,
            jdbcExecutionOptions,
            jdbcDmlOptions,
            context.getPhysicalRowDataType());
    }

    private void validateDataTypeWithJdbcDialect(
        DataType dataType, String url, String compatibleMode, ClassLoader classLoader) {
        final JdbcDialect dialect = loadDialect(url, compatibleMode, classLoader);
        dialect.validate((RowType) dataType.getLogicalType());
    }

    private InternalJdbcConnectionOptions getJdbcOptions(
        ReadableConfig readableConfig, ClassLoader classLoader) {
        final String url = readableConfig.get(URL);
        final InternalJdbcConnectionOptions.Builder builder =
            InternalJdbcConnectionOptions.builder()
                .setClassLoader(classLoader)
                .setDBUrl(url)
                .setTableName(readableConfig.get(TABLE_NAME))
                .setDialect(
                    loadDialect(
                        url, readableConfig.get(COMPATIBLE_MODE), classLoader))
                .setParallelism(readableConfig.getOptional(SINK_PARALLELISM).orElse(null))
                .setConnectionCheckTimeoutSeconds(
                    (int) readableConfig.get(MAX_RETRY_TIMEOUT).getSeconds());

        readableConfig.getOptional(DRIVER).ifPresent(builder::setDriverName);
        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        readableConfig.getOptional(COMPATIBLE_MODE).ifPresent(builder::setCompatibleMode);
        return builder.build();
    }

    private JdbcDialect loadDialect(String url, String compatibleMode, ClassLoader classLoader) {
        JdbcDialect dialect = JdbcDialectLoader.load(url, compatibleMode, classLoader);
        //sqrl: standard postgres dialect with extended dialect
        if (dialect.dialectName().equalsIgnoreCase("PostgreSQL")) {
            return new SqrlPostgresDialect();
        }
        return dialect;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    private Object invokeMethod(String methodName, Object... args) {
        try {
            Method method = Arrays.stream(getClass().getSuperclass().getDeclaredMethods())
                .filter(m -> m.getName().equals(methodName))
                .findFirst()
                .orElseThrow(() -> new NoSuchMethodException("Method " + methodName + " not found"));

            method.setAccessible(true);
            return method.invoke(this, args);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Error invoking method " + methodName, e);
        }
    }
}
