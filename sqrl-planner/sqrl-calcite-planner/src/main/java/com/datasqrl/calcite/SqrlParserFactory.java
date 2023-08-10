package com.datasqrl.calcite;

import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.flink.sql.parser.impl.SqrlSqlParserImpl;

import java.io.Reader;

public class SqrlParserFactory implements SqlParserImplFactory {
  @Override
  public SqlAbstractParserImpl getParser(Reader reader) {
    return new SqrlSqlParserImpl(reader);
  }
}