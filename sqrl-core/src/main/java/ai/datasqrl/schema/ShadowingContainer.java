package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * A container that keeps any type of object that implements the {@link Element}
 * interface which returns the name of that object. Objects can be retrieved by their name. If an
 * object with the same name is inserted, it shadows the previous element. That means, the previous
 * element is no longer accessible by name but only when iterating through the entire list of
 * elements.
 *
 * @param <E>
 */
public class ShadowingContainer<E extends ShadowingContainer.Element> implements Iterable<E> {

  private int indexCounter = 0;
  private final List<E> elements = new ArrayList<>();
  private final Map<Name, E> byName = new LinkedHashMap<>();

  /**
   * Adds an element to this container
   *
   * @param e
   * @return true if the added element shadows a previously added element; else false
   */
  public boolean add(E e) {
    Preconditions.checkArgument(e.getVersion()>=0 &&
            getMaxVersion(e.getName()).map(v -> v<e.getVersion()).orElse(true),"Invalid version: " + e.getVersion());
    if (e instanceof IndexElement) {
      IndexElement ie = (IndexElement)e;
      Preconditions.checkArgument(ie.getIndex() == indexCounter, "Invalid index: " + ie.getIndex());
      indexCounter++;
      elements.add(ie.getIndex(),e);
    } else {
      elements.add(e);
    }
    if (e.isVisible()) {
      return (byName.put(e.getName(), e) != null);
    } else {
      return false;
    }
  }

  public Optional<Integer> getMaxVersion(Name name) {
    return stream().filter(e -> e.getName().equals(name)).map(Element::getVersion).max(Integer::compareTo);
  }

  public int getIndexLength() {
    return indexCounter;
  }

  public E atIndex(int index) {
    Preconditions.checkArgument(index< indexCounter);
    return elements.get(index);
  }

  /**
   * Returns the element currently visible by the given name
   *
   * @param name
   * @return
   */
  public Optional<E> getVisibleByName(Name name) {
    return Optional.ofNullable(byName.get(name));
  }

  public int size() {
    return this.elements.size();
  }

  public boolean isEmpty() {
    return this.elements.isEmpty();
  }

  public boolean contains(Object o) {
    return this.elements.contains(o);
  }

  @Override
  public Iterator<E> iterator() {
    return elements.iterator();
  }

  public Stream<E> stream() {
    return elements.stream();
  }

  @Override
  public String toString() {
    return "ShadowingContainer{" +
        "elements=" + elements +
        '}';
  }

  public interface Element {

    Name getName();

    default boolean isVisible() {
      return true;
    }

    int getVersion();

  }

  public interface IndexElement extends Element {

    int getIndex();

  }

}
