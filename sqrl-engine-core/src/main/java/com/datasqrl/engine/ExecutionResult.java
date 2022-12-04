package com.datasqrl.engine;

import lombok.Value;

public interface ExecutionResult {


  @Value
  class Message implements ExecutionResult {

    String message;

  }

}
