package com.datasqrl.flinkwrapper.parser;


public interface SqrlStatement extends SQLStatement {

  default SqrlComments getComments() {
    return SqrlComments.EMPTY;
  }


}
