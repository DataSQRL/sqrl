package com.datasqrl.v2.parser;


public interface SqrlStatement extends SQLStatement {

  default SqrlComments getComments() {
    return SqrlComments.EMPTY;
  }


}
