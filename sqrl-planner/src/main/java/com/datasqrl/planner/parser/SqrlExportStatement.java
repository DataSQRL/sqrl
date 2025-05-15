package com.datasqrl.planner.parser;

import com.datasqrl.canonicalizer.NamePath;

import lombok.Value;

/**
 * Represents an EXPORT statement
 */
@Value
public class SqrlExportStatement implements SqrlDdlStatement {

  ParsedObject<NamePath> tableIdentifier;
  ParsedObject<NamePath> packageIdentifier;
  SqrlComments comments;

}
