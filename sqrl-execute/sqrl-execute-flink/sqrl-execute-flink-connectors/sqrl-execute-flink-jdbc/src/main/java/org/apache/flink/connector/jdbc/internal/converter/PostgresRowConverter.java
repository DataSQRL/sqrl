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

package org.apache.flink.connector.jdbc.internal.converter;

import java.sql.Array;
import java.sql.PreparedStatement;
import lombok.SneakyThrows;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatementImpl;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.postgresql.jdbc.PgArray;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * PostgreSQL.
 *
 * SQRL:Add array support
 */
public class PostgresRowConverter extends BaseRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "PostgreSQL";
    }

    public PostgresRowConverter(RowType rowType) {
        super(rowType);
    }

    @SneakyThrows
    public void createRowSerializer(LogicalType type, RowData val, int index,
        FieldNamedPreparedStatement statement) {
        FieldNamedPreparedStatementImpl flinkPreparedStatement = (FieldNamedPreparedStatementImpl) statement;
        for (int idx : flinkPreparedStatement.getIndexMapping()[index]) {
            statement.setObject(idx, val.getBinary(index));
        }
    }

    @SneakyThrows
    public void createArraySerializer(LogicalType type, RowData val, int index, FieldNamedPreparedStatement statement) {
        FieldNamedPreparedStatementImpl flinkPreparedStatement = (FieldNamedPreparedStatementImpl) statement;
        for (int idx : flinkPreparedStatement.getIndexMapping()[index]) {
            ArrayData arrayData = val.getArray(index);
            if (arrayData instanceof GenericArrayData) {
                createSqlArrayObject(type, (GenericArrayData)arrayData, idx, flinkPreparedStatement.getStatement());
            } else if (arrayData instanceof BinaryArrayData) {
                Array array = flinkPreparedStatement.getStatement().getConnection()
                    .createArrayOf("bytea", ArrayUtils.toObject( arrayData.toByteArray()));
                flinkPreparedStatement.getStatement().setArray(idx, array);
            } else {
                throw new RuntimeException("Unsupported ArrayData type: " + arrayData.getClass());
            }
        }
    }

    @SneakyThrows
    private void createSqlArrayObject(LogicalType type, GenericArrayData data, int idx,
        PreparedStatement statement) {
        //Scalar arrays of any dimension are one array call
        if (isScalarArray(type)) {
            Object[] boxed = data.toObjectArray();
            Array array = statement.getConnection()
                .createArrayOf(getArrayScalarName(type), boxed);
            statement.setArray(idx, array);
        } else {
            Array array = statement.getConnection()
                .createArrayOf("bytea", ArrayUtils.toObject(data.toByteArray()));
            statement.setArray(idx, array);
        }
    }

    @Override
    public JdbcDeserializationConverter createArrayConverter(ArrayType arrayType) {
        // Since PGJDBC 42.2.15 (https://github.com/pgjdbc/pgjdbc/pull/1194) bytea[] is wrapped in
        // primitive byte arrays
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        final JdbcDeserializationConverter elementConverter =
                createNullableInternalConverter(arrayType.getElementType());
        return val -> {
            PgArray pgArray = (PgArray) val;
            Object[] in = (Object[]) pgArray.getArray();
            final Object[] array = (Object[]) java.lang.reflect.Array.newInstance(elementClass, in.length);
            for (int i = 0; i < in.length; i++) {
                array[i] = elementConverter.deserialize(in[i]);
            }
            return new GenericArrayData(array);
        };
    }


    private String getArrayScalarName(LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return "text";
            case BOOLEAN:
                return "boolean";
            case BINARY:
            case VARBINARY:
                return "bytea";
            case DECIMAL:
                return "decimal";
            case TINYINT:
                return "smallint";
            case SMALLINT:
                return "smallint";
            case INTEGER:
                return "integer";
            case BIGINT:
                return "bigint";
            case FLOAT:
                return "real"; // PostgreSQL uses REAL for float
            case DOUBLE:
                return "double";
            case DATE:
                return "date";
            case TIME_WITHOUT_TIME_ZONE:
                return "time without time zone";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return "timestamp without time zone";
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "timestamp with time zone";
            case INTERVAL_YEAR_MONTH:
                return "interval year to month";
            case INTERVAL_DAY_TIME:
                return "interval day to second";
            case NULL:
                return "void";
            case ARRAY:
                return getArrayScalarName(((ArrayType) type).getElementType());
            case MULTISET:
            case MAP:
            case ROW:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case RAW:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new RuntimeException("Unknown type");
        }
    }
}
