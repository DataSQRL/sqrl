package com.datasqrl.time;

import java.time.Instant;

public interface TimeTumbleWindowFunctionEval {

  Instant eval(Instant instant, Long multiple, Long offset);

  default Instant eval(Instant instant, Long multiple) {
    return eval(instant, multiple, 0L);
  }

  default Instant eval(Instant instant) {
    return eval(instant, 1L);
  }
}
