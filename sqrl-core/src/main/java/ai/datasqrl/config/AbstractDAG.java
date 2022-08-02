package ai.datasqrl.config;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import graphql.com.google.common.collect.Streams;

import java.util.*;
import java.util.stream.Collectors;


public abstract class AbstractDAG<E extends AbstractDAG.Node, D extends AbstractDAG<E, D>> implements Iterable<E> {

    Multimap<E, E> inputs;
    Multimap<E, E> outputs;
    Set<E> sources;

    protected AbstractDAG(Multimap<E, E> inputs) {
        this.inputs = inputs;
        this.outputs = HashMultimap.create();
        this.sources = new HashSet<>();
        inputs.entries().forEach(e -> {
            E in = e.getValue(), out = e.getKey();
            outputs.put(in,out);
            if (inputs.get(in).isEmpty()) sources.add(in);
        });
    }

    public D addNodes(Multimap<E, E> inputs) {
        HashMultimap<E,E> newInputs = HashMultimap.create(this.inputs);
        inputs.entries().forEach(e -> newInputs.put(e.getKey(),e.getValue()));
        return create(newInputs);
    }

    protected abstract D create(Multimap<E, E> inputs);

    public Set<E> getSinks() {
        return Streams.concat(inputs.keySet().stream(),sources.stream()).filter(Node::isSink).collect(Collectors.toSet());
    }

    /**
     * Remove all nodes in the DAG that do not reach a sink
     * @return
     */
    public D trimToSinks() {
        Set<E> reached = (Set<E>) getAllInputsFromSource(getSinks());
        return create(Multimaps.filterKeys(inputs,e -> reached.contains(e)));
    }

    public Collection<E> getAllInputsFromSource(E element) {
        return getAllInputsFromSource(List.of(element));
    }

    public Collection<E> getAllInputsFromSource(Collection<E> elements) {
        Set<E> reached = new HashSet<>();
        Deque<E> next = new ArrayDeque<>(elements);
        while (!next.isEmpty()) {
            E n = next.removeFirst();
            if (!reached.contains(n)) {
                reached.add(n);
                next.addAll(inputs.get(n));
            }
        }
        return reached;
    }

    public Collection<E> getSources() {
        return sources;
    }

    public Collection<E> getInputs(E element) {
        return inputs.get(element);
    }

    public Collection<E> getOutputs(E element) {
        return outputs.get(element);
    }

    @Override
    public Iterator<E> iterator() {
        return new Source2SinkIterator();
    }

    private class Source2SinkIterator implements Iterator<E> {

        private final Deque<E> toVisit = new ArrayDeque<>(sources);
        private final Set<E> visited = new HashSet<>();
        private E next = toVisit.removeFirst();

        @Override
        public boolean hasNext() {
            return next!=null;
        }

        @Override
        public E next() {
            E toReturn = next;
            toVisit.addAll(outputs.get(toReturn));
            visited.add(toReturn);
            next = null;
            while (next==null && !toVisit.isEmpty()) {
                next = toVisit.removeFirst();
                if (visited.contains(next)) next = null;
            }
            return toReturn;
        }
    }

    public interface Node {

        default boolean isSink() {
            return false;
        }

    }


}
