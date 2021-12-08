package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.tree.name.Name;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Stream;

/**
 * A container that keeps any type of object that implements the {@link ShadowingContainer.Nameable} interface which
 * returns the name of that object. Objects can be retrieved by their name. If an object with the same name is inserted,
 * it shadows the previous element. That means, the previous element is no longer accessible by name but only when iterating
 * through the entire list of elements.
 *
 *
 * @param <E>
 */
public class ShadowingContainer<E extends ShadowingContainer.Nameable> implements Iterable<E> {

    private List<E> elements = new ArrayList<>();
    private Map<Name,E> byName = new HashMap<>();

    /**
     * Adds an element to this container
     * @param e
     * @return true if the added element shadows a previously added element; else false
     */
    public boolean add(E e) {
        elements.add(e);
        if (e.isSystem()) return false;
        else return (byName.put(e.getName(),e)!=null);
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
    public Iterator<E> iterator() {
        return elements.iterator();
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

    public List<E> getElements() {
        return elements;
    }

    public interface Nameable {

        Name getName();

        default boolean isSystem() {
            return false;
        }

    }

}
