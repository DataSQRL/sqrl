package org.apache.calcite.rex;

import java.math.BigDecimal;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.NlsString;

/** Implementation copied/modified from calcite */
public class SqrlRexBuilder extends RexBuilder {

  public SqrlRexBuilder(RelDataTypeFactory typeFactory) {
    super(typeFactory);
  }

  @Override
  boolean canRemoveCastFromLiteral(RelDataType toType, Comparable value, SqlTypeName fromTypeName) {
    if (value == null) {
      return true;
    }
    final SqlTypeName sqlType = toType.getSqlTypeName();
    if (!RexLiteral.valueMatchesType(value, sqlType, false)) {
      return false;
    }
    if (toType.getSqlTypeName() != fromTypeName
        && SqlTypeFamily.DATETIME.getTypeNames().contains(fromTypeName)) {
      return false;
    }
    if (value instanceof NlsString) {
      // do not remove cast e.g. char -> String
      return false;
      //      final int length = ((NlsString) value).getValue().length();
      //      switch (toType.getSqlTypeName()) {
      //        case CHAR:
      //          return SqlTypeUtil.comparePrecision(toType.getPrecision(), length) == 0;
      //        case VARCHAR:
      //          return SqlTypeUtil.comparePrecision(toType.getPrecision(), length) >= 0;
      //        default:
      //          throw new AssertionError(toType);
      //      }
    }
    if (value instanceof ByteString) {
      final int length = ((ByteString) value).length();
      switch (toType.getSqlTypeName()) {
        case BINARY:
          return SqlTypeUtil.comparePrecision(toType.getPrecision(), length) == 0;
        case VARBINARY:
          return SqlTypeUtil.comparePrecision(toType.getPrecision(), length) >= 0;
        default:
          throw new AssertionError(toType);
      }
    }

    if (toType.getSqlTypeName() == SqlTypeName.DECIMAL) {
      final BigDecimal decimalValue = (BigDecimal) value;
      return SqlTypeUtil.isValidDecimalValue(decimalValue, toType);
    }

    if (SqlTypeName.INT_TYPES.contains(sqlType)) {
      final BigDecimal decimalValue = (BigDecimal) value;
      final int s = decimalValue.scale();
      if (s != 0) {
        return false;
      }
      long l = decimalValue.longValue();
      switch (sqlType) {
        case TINYINT:
          return l >= Byte.MIN_VALUE && l <= Byte.MAX_VALUE;
        case SMALLINT:
          return l >= Short.MIN_VALUE && l <= Short.MAX_VALUE;
        case INTEGER:
          return l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE;
        case BIGINT:
        default:
          return true;
      }
    }

    return true;
  }
}
