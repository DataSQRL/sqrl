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

import static com.datasqrl.type.FlinkArrayTypeUtil.isScalarArray;
import static com.datasqrl.type.PostgresArrayTypeConverter.getArrayScalarName;

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
public class PostgresRowConverter extends BaseJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "PostgreSQL";
    }

    public PostgresRowConverter(RowType rowType) {
        super(rowType);
    }

    @SneakyThrows
    public void setRow(LogicalType type, RowData val, int index,
        FieldNamedPreparedStatement statement) {
        FieldNamedPreparedStatementImpl flinkPreparedStatement = (FieldNamedPreparedStatementImpl) statement;
        for (int idx : flinkPreparedStatement.getIndexMapping()[index]) {
//            RowData row = val.getRow(index, ((RowType) type).getFieldCount());
//            java.sql.Array sqlArray = flinkPreparedStatement.getStatement()
//                .getConnection().createArrayOf("bytea", );
            flinkPreparedStatement.getStatement().setBytes(idx, new byte[0]);
        }
    }

    @Override
    protected String getArrayType() {
        return "bytea";
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
            //sqrl: check if scalar array

            Object[] in;
            if (val instanceof PgArray) {
                PgArray pgArray = (PgArray) val;
                in = (Object[]) pgArray.getArray();
            } else {
                in = (Object[])val;
            }
            final Object[] array = (Object[]) java.lang.reflect.Array.newInstance(elementClass, in.length);
            for (int i = 0; i < in.length; i++) {
                array[i] = elementConverter.deserialize(in[i]);
            }
            return new GenericArrayData(array);
        };
    }
}
