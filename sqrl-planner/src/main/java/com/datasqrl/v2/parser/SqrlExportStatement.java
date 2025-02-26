package com.datasqrl.v2.parser;

import com.datasqrl.canonicalizer.NamePath;
import lombok.Value;

/**
 * Represents an EXPORT statement
 */
@Value
public class SqrlExportStatement implements SqrlDdlStatement {

  ParsedObject<NamePath> tableIdentifier;
  ParsedObject<NamePath> packageIdentifier;

}
