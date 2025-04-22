package com.datasqrl.io.schema.flexible.converters;

import org.apache.calcite.rel.type.RelDataType;

import com.datasqrl.io.schema.flexible.type.ArrayType;
import com.datasqrl.io.schema.flexible.type.Type;
import com.datasqrl.io.schema.flexible.type.basic.BigIntType;
import com.datasqrl.io.schema.flexible.type.basic.BooleanType;
import com.datasqrl.io.schema.flexible.type.basic.DoubleType;
import com.datasqrl.io.schema.flexible.type.basic.StringType;
import com.datasqrl.io.schema.flexible.type.basic.TimestampType;

public class RelDataTypePrimitiveToFlexibleType {

  public static Type toType(RelDataType type) {
    return switch (type.getSqlTypeName()) {
	case BOOLEAN -> new BooleanType();
	case TINYINT, SMALLINT, BIGINT, INTEGER, DATE, TIMESTAMP -> new BigIntType();
	case CHAR, VARCHAR -> new StringType();
	case DECIMAL, FLOAT, DOUBLE -> new DoubleType();
	case TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIME -> new TimestampType();
	case MULTISET -> new ArrayType(toType(type.getComponentType()));
	case BINARY, VARBINARY, INTERVAL_YEAR_MONTH, INTERVAL_DAY, NULL, SYMBOL, ARRAY, MAP, ROW -> throw new UnsupportedOperationException("Unsupported type:" + type);
	default -> throw new UnsupportedOperationException("Unsupported type:" + type);
	};
  }
}
