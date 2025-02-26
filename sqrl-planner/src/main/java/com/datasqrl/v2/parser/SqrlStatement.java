package com.datasqrl.v2.parser;


/**
 * A SQRL specific statement
 */
public interface SqrlStatement extends SQLStatement {

  default SqrlComments getComments() {
    return SqrlComments.EMPTY;
  }


}
