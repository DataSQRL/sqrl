package ai.dataeng.sqml.parser.operator;

import ai.dataeng.sqml.tree.name.Name;
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;

/**
 * A container that keeps any type of object that implements the {@link ShadowingContainer.Nameable} interface which
 * returns the name of that object. Objects can be retrieved by their name. If an object with the same name is inserted,
 * it shadows the previous element. That means, the previous element is no longer accessible by name but only when iterating
 * through the entire list of elements.
 *
 *
 * @param <E>
 */
public class ShadowingContainer<E extends ShadowingContainer.Nameable> implements Collection<E> {

    private List<E> elements = new ArrayList<>();
    private Map<Name,E> byName = new LinkedHashMap<>();

    /**
     * Adds an element to this container
     * @param e
     * @return true if the added element shadows a previously added element; else false
     */
    public boolean add(E e) {
        elements.add(e);
        if (e.isVisible()) return (byName.put(e.getName(),e)!=null);
        else return false;
    }

    @Override
    public boolean remove(Object o) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        for (E ele : c) {
            add(ele);
        }
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void clear() {
        throw new RuntimeException("Not implemented");
    }

    /**
     * Returns the element currently visible by the given name
     * @param name
     * @return
     */
    public E getByName(Name name) {
        return byName.get(name);
    }

    /**
     * Returns true if there is an element in this container with the given name
     * @param name
     * @return
     */
    public boolean hasWithName(Name name) {
        return byName.containsKey(name);
    }

    /**
     * @return An iterator which pairs each element with a boolean flag indicating whether the element is shadowed or not
     */
    public Iterator<Pair<E,Boolean>> shadowIterator() {
        return Iterators.transform(elements.iterator(), e -> Pair.of(e,!e.equals(byName.get(e.getName()))));
    }

    @Override
    public int size() {
        return this.elements.size();
    }

    @Override
    public boolean isEmpty() {
        return this.elements.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return this.elements.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return elements.iterator();
    }

    @Override
    public Object[] toArray() {
        return this.elements.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return this.elements.toArray(a);
    }

    /**
     * @return Returns an iterator with only the visible (i.e. not shadowed) elements.
     */
    public Iterator<E> visibleIterator() {
        return Iterators.filter(elements.iterator(), e -> e.equals(byName.get(e.getName())));
    }

    public Stream<E> stream() {
        return elements.stream();
    }

    /**
     * @return A stream which pairs each element with a boolean flag indicating whether the element is shadowed or not
     */
    public Stream<Pair<E,Boolean>> shadowStream() {
        return elements.stream().map(e -> Pair.of(e,!e.equals(byName.get(e.getName()))));
    }

    /**
     * @return Returns a stream with only visible elements
     */
    public Stream<E> visibleStream() {
        return elements.stream().filter(e -> e.equals(byName.get(e.getName())));
    }
    public List<E> visibleList() {
        return elements.stream().filter(e -> e.equals(byName.get(e.getName()))).collect(Collectors.toList());
    }

    public List<E> getElements() {
        return elements;
    }

    public int indexOf(Name name) {
        int lastIndex = -1;
        for (int i = 0; i < elements.size(); i++) {
            E element = elements.get(i);
            if (element.getName().equals(name)) {
                lastIndex = i;
            }
        }
        return lastIndex;
    }

    public E get(int i) {
        return this.elements.get(i);
    }

    public E getByName(Name name, int version) {
        for (E f : this.elements) {
            if (f.getName().equals(name) && f.getVersion() == version) {
                return f;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "ShadowingContainer{" +
            "elements=" + elements +
            '}';
    }

    public interface Nameable {

        Name getName();

        default boolean isVisible() {
            return true;
        }

        int getVersion();

    }

}
