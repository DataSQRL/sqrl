package com.datasqrl.calcite.dialect.snowflake;

import static com.datasqrl.calcite.dialect.snowflake.SqlCatalogParams.Param.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

public class SqlCatalogParams {
    public static SqlNodeList createGlueCatalogParams(SqlParserPos pos,
                                                      String awsRoleArn,
                                                      String catalogId,
                                                      String awsRegion, // This can be null if not specified
                                                      String catalogNamespace) {
        List<SqlNode> params = new ArrayList<>();

        // Add the GLUE_AWS_ROLE_ARN parameter
        params.add(createAssignment(GLUE_AWS_ROLE_ARN, SqlLiteral.createCharString(awsRoleArn, SqlParserPos.ZERO), SqlParserPos.ZERO));

        // Add the GLUE_CATALOG_ID parameter
        params.add(createAssignment(GLUE_CATALOG_ID, SqlLiteral.createCharString(catalogId, SqlParserPos.ZERO), pos));

        // Add the GLUE_REGION parameter if it is specified
        if (awsRegion != null && !awsRegion.isEmpty()) {
            params.add(createAssignment(GLUE_REGION, SqlLiteral.createCharString(awsRegion, SqlParserPos.ZERO), pos));
        }

        // Add the CATALOG_NAMESPACE parameter
        params.add(createAssignment(CATALOG_NAMESPACE, SqlLiteral.createCharString(catalogNamespace, SqlParserPos.ZERO), pos));

        return new SqlNodeList(params, SqlParserPos.ZERO);
    }

    private static SqlNode createAssignment(Param key, SqlNode value, SqlParserPos pos) {
        return new SqlAssignment(SqlLiteral.createSymbol(key, pos), value, pos);
    }
    
    public enum Param {
        GLUE_AWS_ROLE_ARN,
        GLUE_CATALOG_ID,
        GLUE_REGION,
        CATALOG_NAMESPACE
    }
}