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

public class GetDayOfMonthFunction extends SqlUserDefinedFunction implements SqrlAwareFunction {

    static final ScalarFunction fnc = ScalarFunctionImpl.create(Types.lookupMethod(
            GetDayOfMonth.class, "getDayOfMonth", Instant.class));

    public GetDayOfMonthFunction() {
        super(
                new SqlIdentifier("GET_DAY_OF_MONTH", SqlParserPos.ZERO),
                SqlKind.OTHER,
                ReturnTypes.INTEGER,
                InferTypes.ANY_NULLABLE,
                OperandTypes.operandMetadata(
                        List.of(SqlTypeFamily.TIMESTAMP),
                        typeFactory -> List.of(
                                typeFactory.createSqlType(SqlTypeName.TIMESTAMP)),
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
    public boolean isTimestampPreserving() {return false;}

    @Override
    public SqlOperator getOp() {return this;}
}
