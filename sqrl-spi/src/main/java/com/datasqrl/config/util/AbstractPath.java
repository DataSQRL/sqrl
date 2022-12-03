package com.datasqrl.config.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public abstract class AbstractPath<E extends Comparable,P extends AbstractPath<E,P>> implements Iterable<E>, Serializable, Comparable<P> {

  protected final E[] elements;

  protected AbstractPath(@NonNull E... elements) {
    this.elements = elements;
  }

  protected abstract Constructor<E,P> constructor();

  public P concat(@NonNull E element) {
    E[] newelements = Arrays.copyOf(elements, elements.length + 1);
    newelements[elements.length] = element;
    return constructor().create(newelements);
  }

  public P concat(@NonNull P sub) {
    E[] newelements = Arrays.copyOf(elements, elements.length + sub.elements.length);
    System.arraycopy(sub.elements, 0, newelements, elements.length, sub.elements.length);
    return constructor().create(newelements);
  }

  public P prefix(int depth) {
    if (depth == 0) {
      return constructor().root();
    }
    E[] newelements = Arrays.copyOf(elements, depth);
    return constructor().create(newelements);
  }

  public int size() {
    return elements.length;
  }

  public E get(int index) {
    Preconditions.checkArgument(index >= 0 && index < size());
    return elements[index];
  }

  public Optional<E> getOptional(int index) {
    if (index >= 0 && index < size()) {
      return Optional.of(elements[index]);
    } else {
      return Optional.empty();
    }
  }

  public E getLast() {
    Preconditions.checkArgument(elements.length > 0);
    return elements[elements.length - 1];
  }

  @Override
  public String toString() {
    return toString('.');
  }

  public String toString(char separator) {
    if (elements.length == 0) {
      return "/";
    }
    return StringUtils.join(elements, separator);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(elements);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null) {
      return false;
    } else if (!(other instanceof AbstractPath)) {
      return false;
    }
    AbstractPath o = (AbstractPath) other;
    return Arrays.equals(elements, o.elements);
  }


  @Override
  public Iterator<E> iterator() {
    return Iterators.forArray(elements);
  }

  @Override
  public int compareTo(P o) {
    return Arrays.compare(elements, o.elements);
  }

  public Optional<P> getPrefix() {
    if (elements.length <= 1) {
      return Optional.empty();
    }

    E[] newNames = Arrays.copyOfRange(elements, 0, elements.length - 1);
    return Optional.of(constructor().create(newNames));
  }

  public P popFirst() {
    Preconditions.checkArgument(size()>0);
    E[] newNames = Arrays.copyOfRange(elements, 1, elements.length);
    return constructor().create(newNames);
  }

  public P popLast() {
    Preconditions.checkArgument(size()>0);
    E[] newNames = Arrays.copyOfRange(elements, 0, elements.length - 1);
    return constructor().create(newNames);
  }

  public P parent() {
    return popLast();
  }

  public E getFirst() {
    return elements[0];
  }

  public boolean isEmpty() {
    return elements.length == 0;
  }

  public P subList(int from, int to) {
    if (from < 0 || to < from || to > elements.length) {
      throw new IllegalArgumentException("Invalid offsets");
    } else if (from == to) {
      return constructor().root();
    }
    E[] newNames = Arrays.copyOfRange(elements, from, to);
    return constructor().create(newNames);
  }

  public Stream<E> stream() {
    return Arrays.stream(elements);
  }

  protected static abstract class Constructor<E extends Comparable,P extends AbstractPath<E,P>> {

    protected abstract P create(@NonNull E... elements);

    protected abstract E[] createArray(int length);

    protected abstract P root();

    public P of(@NonNull List<E> elements) {
      return create(elements.toArray(createArray(elements.size())));
    }

    public P parse(String path, Function<String,E> parser) {
      String[] arr = path.split("\\.");
      E[] elements = createArray(arr.length);
      int i = 0;
      for (String e : arr) {
        elements[i++] = parser.apply(e);
      }
      return create(elements);
    }

    public<E2> P of(Function<E2,E> converter, @NonNull E2[] elements) {
      E[] arr = Arrays.stream(elements)
              .map(converter)
              .toArray(this::createArray);
      return create(arr);
    }

  }

}
