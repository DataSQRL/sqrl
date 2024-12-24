package com.datasqrl.flinkwrapper.parser;


import com.datasqrl.flinkwrapper.SqrlEnvironment;

public interface SqrlStatement {

  void apply(SqrlEnvironment sqrlEnv);

}
