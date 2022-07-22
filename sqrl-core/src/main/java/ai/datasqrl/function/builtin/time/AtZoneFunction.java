package ai.datasqrl.function.builtin.time;

import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.parse.tree.name.Name;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import java.time.Instant;
import java.util.List;

public class AtZoneFunction extends SqlUserDefinedFunction implements SqrlAwareFunction {

    static final ScalarFunction fnc = ScalarFunctionImpl.create(Types.lookupMethod(
            AtZone.class, "atZone", Instant.class, String.class));

    public AtZoneFunction() {
        super(
                new SqlIdentifier("AT_ZONE", SqlParserPos.ZERO),
                SqlKind.OTHER,
                ReturnTypes.TIMESTAMP,
                //ReturnTypes.explicit(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
                // want to return TIMESTAMP_WITH_TIME_ZONE but that is not an option in calcite?
                InferTypes.ANY_NULLABLE,
                OperandTypes.operandMetadata(
                        List.of(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER),
                        typeFactory -> List.of(
                                typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
                                typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                        i -> "arg" + i,
                        i -> false),
                fnc,
                SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
    }

    public SqlSyntax getSyntax() {return SqlSyntax.FUNCTION;}

    public boolean isDynamicFunction() {return true;}

    @Override
    public Name getSqrlName() {return Name.system(getName());}

    @Override
    public boolean isAggregate() {return false;}

    @Override
    public boolean isTimestampPreserving() {return true;}

    @Override
    public SqlOperator getOp() {return this;}
}
