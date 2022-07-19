package ai.datasqrl.plan.calcite;

import ai.datasqrl.schema.input.TableBuilderFlexibleTableConverterVisitor;
import ai.datasqrl.schema.type.ArrayType;
import ai.datasqrl.schema.type.Type;
import ai.datasqrl.schema.type.basic.*;
import lombok.Value;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

@Value
public class SqrlType2Calcite implements TableBuilderFlexibleTableConverterVisitor.SqrlTypeConverter<RelDataType> {

    RelDataTypeFactory typeFactory;

    @Override
    public RelDataType visitType(Type type, Void context) {
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public <J> RelDataType visitBasicType(AbstractBasicType<J> type, Void context) {
        throw new IllegalArgumentException("Basic type is not supported: " + type.getName());
    }

    @Override
    public RelDataType visitBooleanType(BooleanType type, Void context) {
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    }

    @Override
    public RelDataType visitDateTimeType(DateTimeType type, Void context) {
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3);
    }

    @Override
    public RelDataType visitFloatType(FloatType type, Void context) {
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
    }

    @Override
    public RelDataType visitIntegerType(IntegerType type, Void context) {
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
    }

    @Override
    public RelDataType visitStringType(StringType type, Void context) {
        return typeFactory.createSqlType(SqlTypeName.VARCHAR,Short.MAX_VALUE);
    }

    @Override
    public RelDataType visitUuidType(UuidType type, Void context) {
        return typeFactory.createSqlType(SqlTypeName.CHAR, 36);
    }

    @Override
    public RelDataType visitIntervalType(IntervalType type, Void context) {
        return typeFactory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.SECOND, 9, null, 3, SqlParserPos.ZERO));
    }

    @Override
    public RelDataType visitArrayType(ArrayType type, Void context) {
        return typeFactory.createArrayType(type.getSubType().accept(this, null), -1L);
    }
}
