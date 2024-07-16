package com.datasqrl.engine.log;

import java.util.Map;

public interface LogManager extends LogFactory {

  /**
   *
   * @return Whether this {@link LogManager} has a configured LogEngine. If not, creating logs
   * will throw exceptions.
   */
  boolean hasLogEngine();

  /**
   *
   * @return All the logs created by this {@link LogManager} with their globally unique ids.
   */
  Map<String, Log> getLogs();

}
