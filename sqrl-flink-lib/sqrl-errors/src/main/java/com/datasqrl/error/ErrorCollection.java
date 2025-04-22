/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.NonNull;

public class ErrorCollection implements Iterable<ErrorMessage>, Serializable {

  private final List<ErrorMessage> errors;

  public ErrorCollection() {
    this(new ArrayList<>(5));
  }

  private ErrorCollection(@NonNull List<ErrorMessage> errors) {
    this.errors = errors;
  }

  protected void addInternal(@NonNull ErrorMessage error) {
    //Preconditions.checkArgument(error.getLocation().hasPrefix(), "Error is not grounded: %s", error);
    errors.add(error);
  }

  protected void add(@NonNull ErrorMessage err, ErrorLocation baseLocation) {
    var errLoc = err.getLocation();
    if (!errLoc.hasPrefix()) {
      //Adjust relative location
      var newloc = baseLocation.append(errLoc);
      err = new ErrorMessage.Implementation(err.getErrorLabel(), err.getMessage(), newloc,
          err.getSeverity());
    }
    addInternal(err);
  }

  public void addAll(@NonNull ErrorCollection other, ErrorLocation baseLocation) {
    if (other == null || !other.hasErrorsWarningsOrNotices()) {
		return;
	}
    other.stream().forEach(err -> add(err, baseLocation));
  }

  public boolean hasErrorsWarningsOrNotices() {
    return !errors.isEmpty();
  }

  public boolean hasErrors() {
    return errors.stream().anyMatch(ErrorMessage::isFatal);
  }

  public String combineMessages(ErrorMessage.Severity minSeverity, String prefix,
      String delimiter) {
    var suffix = "";
    if (errors != null) {
      suffix = errors.stream().filter(m -> m.getSeverity().compareTo(minSeverity) >= 0)
          .map(ErrorMessage::toString)
          .collect(Collectors.joining(delimiter));
    }
    return prefix + suffix;
  }

  @Override
  public String toString() {
    return combineMessages(ErrorMessage.Severity.NOTICE, "", "\n");
  }

  @Override
  public Iterator<ErrorMessage> iterator() {
    return errors.iterator();
  }

  public Stream<ErrorMessage> stream() {
    return errors.stream();
  }

  public List<ErrorMessage> getAll() {
    return new ArrayList<>(errors);
  }

  public ErrorCollector asCollector(ErrorLocation location) {
    return new ErrorCollector(location, this);
  }

//  public void log() {
//    for (ErrorMessage message : errors) {
//      if (message.isNotice()) {
//        log.info(message.toStringNoSeverity());
//      } else if (message.isWarning()) {
//        log.warn(message.toStringNoSeverity());
//      } else if (message.isFatal()) {
//        log.error(message.toStringNoSeverity());
//      } else {
//        throw new UnsupportedOperationException("Unexpected severity: " + message.getSeverity());
//      }
//    }
//  }

}
