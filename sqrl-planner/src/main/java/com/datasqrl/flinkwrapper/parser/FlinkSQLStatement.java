package com.datasqrl.flinkwrapper.parser;

import lombok.Value;

/**
 * A generic FlinkSQL statement that is not SQRL specific
 */
@Value
public class FlinkSQLStatement implements SQLStatement {

  ParsedObject<String> sql;

}
