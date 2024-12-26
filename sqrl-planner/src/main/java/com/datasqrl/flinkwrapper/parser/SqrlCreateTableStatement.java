package com.datasqrl.flinkwrapper.parser;

import lombok.Value;

@Value
public class SqrlCreateTableStatement implements SqrlDdlStatement {

  ParsedObject<String> createTable;

}
