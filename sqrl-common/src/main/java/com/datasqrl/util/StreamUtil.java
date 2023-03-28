/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

public class StreamUtil {

  public static <T, C extends T> Stream<C> filterByClass(Stream<T> stream, Class<C> clazz) {
    return stream.filter(clazz::isInstance).map(clazz::cast);
  }

  public static <T, C extends T> Stream<C> filterByClass(Collection<T> col, Class<C> clazz) {
    return col.stream().filter(clazz::isInstance).map(clazz::cast);
  }

  public static <T, C extends T> Function<T,Stream<C>> classFilter(Class<C> clazz) {
    return (t) -> clazz.isInstance(t)?Stream.of(clazz.cast(t)):Stream.empty();
  }

  public static <T> Stream<T> getPresent(Stream<Optional<T>> stream) {
    return stream.filter(Optional::isPresent).map(Optional::get);
  }

  public static <T> Optional<T> getOnlyElement(Stream<T> stream) {
    AtomicReference<T> elements = new AtomicReference<>(null);
    long count = stream.map(e -> { elements.set(e); return e;}).count();
    if (count==0) return Optional.empty();
    else if (count==1) return Optional.of(elements.get());
    else throw new IllegalArgumentException("Stream contains ["+count+"] elements");
  }


}
