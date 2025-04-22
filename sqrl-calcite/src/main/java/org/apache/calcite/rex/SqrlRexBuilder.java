package org.apache.calcite.rex;

import java.math.BigDecimal;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.NlsString;

/**
 * Implementation copied/modified from calcite
 */
public class SqrlRexBuilder extends RexBuilder {

  public SqrlRexBuilder(RelDataTypeFactory typeFactory) {
    super(typeFactory);
  }

  @Override
  boolean canRemoveCastFromLiteral(RelDataType toType, Comparable value,
      SqlTypeName fromTypeName) {
    if (value == null) {
      return true;
    }
    final var sqlType = toType.getSqlTypeName();
    if (!RexLiteral.valueMatchesType(value, sqlType, false) || (toType.getSqlTypeName() != fromTypeName
        && SqlTypeFamily.DATETIME.getTypeNames().contains(fromTypeName)) || (value instanceof NlsString)) {
      //do not remove cast e.g. char -> String
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
      final var length = ((ByteString) value).length();
      return switch (toType.getSqlTypeName()) {
    case BINARY -> SqlTypeUtil.comparePrecision(toType.getPrecision(), length) == 0;
    case VARBINARY -> SqlTypeUtil.comparePrecision(toType.getPrecision(), length) >= 0;
    default -> throw new AssertionError(toType);
    };
    }

    if (toType.getSqlTypeName() == SqlTypeName.DECIMAL) {
      final var decimalValue = (BigDecimal) value;
      return SqlTypeUtil.isValidDecimalValue(decimalValue, toType);
    }

    if (SqlTypeName.INT_TYPES.contains(sqlType)) {
      final var decimalValue = (BigDecimal) value;
      final var s = decimalValue.scale();
      if (s != 0) {
        return false;
      }
      var l = decimalValue.longValue();
      return switch (sqlType) {
    case TINYINT -> l >= Byte.MIN_VALUE && l <= Byte.MAX_VALUE;
    case SMALLINT -> l >= Short.MIN_VALUE && l <= Short.MAX_VALUE;
    case INTEGER -> l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE;
    case BIGINT -> true;
    default -> true;
    };
    }

    return true;
  }
}
