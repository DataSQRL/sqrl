/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.server.util;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

/** Captures what the logger of one class writes, from {@link #of} until {@link #close}. */
public final class LogCapture extends AbstractAppender implements AutoCloseable {

  private final List<LogEvent> events = new CopyOnWriteArrayList<>();
  private final Logger logger;

  private LogCapture(Logger logger) {
    super("log-capture-" + logger.getName(), null, null, false, Property.EMPTY_ARRAY);
    this.logger = logger;
  }

  public static LogCapture of(Class<?> loggerClass) {
    var capture = new LogCapture((Logger) LogManager.getLogger(loggerClass));
    capture.start();
    capture.logger.addAppender(capture);
    return capture;
  }

  @Override
  public void append(LogEvent event) {
    events.add(event.toImmutable());
  }

  public List<LogEvent> events(Level level) {
    return events.stream().filter(event -> event.getLevel() == level).toList();
  }

  public List<String> messages(Level level) {
    return events(level).stream().map(event -> event.getMessage().getFormattedMessage()).toList();
  }

  @Override
  public void close() {
    logger.removeAppender(this);
    stop();
  }
}
