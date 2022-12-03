package com.datasqrl.physical;

import lombok.Value;

public interface ExecutionResult {


    @Value
    class Message implements ExecutionResult {

        String message;

    }

}
