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
package com.datasqrl.util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Streams;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractDAG<E extends AbstractDAG.Node, D extends AbstractDAG<E, D>>
    implements Iterable<E> {

  Multimap<E, E> inputs;
  Multimap<E, E> outputs;
  Set<E> sources;
  Set<E> sinks;
  Set<E> allNodes;

  protected AbstractDAG(Multimap<E, E> inputs) {
    this.inputs = inputs;
    this.outputs = HashMultimap.create();
    this.sources = new HashSet<>();
    this.allNodes = new HashSet<>();
    inputs.forEach(
        (out, in) -> {
          allNodes.add(in);
          allNodes.add(out);
          outputs.put(in, out);
          if (inputs.get(in).isEmpty()) {
            sources.add(in);
          }
        });
    this.sinks =
        Streams.concat(inputs.keySet().stream(), sources.stream())
            .filter(Node::isSink)
            .collect(Collectors.toSet());
  }

  public <T extends E> Stream<T> allNodesByClass(Class<T> clazz) {
    return StreamUtil.filterByClass(allNodes.stream(), clazz);
  }

  //  public D addNodes(Multimap<E, E> inputs) {
  //      if (inputs.isEmpty()) {
  //          return (D) this;
  //      }
  //    HashMultimap<E, E> newInputs = HashMultimap.create(this.inputs);
  //    inputs.entries().forEach(e -> newInputs.put(e.getKey(), e.getValue()));
  //    return create(newInputs);
  //  }

  protected abstract D create(Multimap<E, E> inputs);

  public Set<E> getSinks() {
    return sinks;
  }

  /**
   * Remove all nodes in the DAG that do not reach a sink
   *
   * @return
   */
  public D trimToSinks() {
    var reached = getAllInputsFromSource(getSinks(), true);
    return create(Multimaps.filterKeys(inputs, e -> reached.contains(e)));
  }

  //
  //  public Set<E> getAllInputsFromSource(E element, boolean includeElement) {
  //    return getAllInputsFromSource(List.of(element), includeElement);
  //  }

  public Set<E> getAllInputsFromSource(Collection<E> elements, boolean includeElements) {
    Set<E> reached = new HashSet<>();
    Deque<E> next = new ArrayDeque<>(elements);
    while (!next.isEmpty()) {
      var n = next.removeFirst();
      if (!reached.contains(n)) {
        reached.add(n);
        next.addAll(inputs.get(n));
      }
    }
    if (!includeElements) {
      reached.removeAll(elements);
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
    return new OrderedIterator(true);
  }

  private class OrderedIterator implements Iterator<E> {

    private final Deque<E> toVisit;
    private final Set<E> visited;
    private E next;
    private final boolean source2sink; // direction of iteration

    public OrderedIterator(boolean source2sink) {
      toVisit = new ArrayDeque<>(source2sink ? sources : sinks);
      visited = new HashSet<>();
      next = toVisit.isEmpty() ? null : toVisit.removeFirst();
      this.source2sink = source2sink;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public E next() {
      var toReturn = next;
      var lookup = source2sink ? outputs : inputs;
      toVisit.addAll(lookup.get(toReturn));
      visited.add(toReturn);
      next = null;
      while (next == null && !toVisit.isEmpty()) {
        next = toVisit.removeFirst();
        if (visited.contains(next)) {
          next = null;
        }
      }
      return toReturn;
    }
  }

  /**
   * @param processNode
   * @param maxIterations
   * @return whether message passing converged within the given number of maxIterations
   */
  protected boolean messagePassing(Function<E, Boolean> processNode, int maxIterations) {
    var iteration = 0;
    var nodeChanged = true;
    while (nodeChanged && iteration < maxIterations) {
      nodeChanged = false;
      var iter = new OrderedIterator(iteration % 2 == 0); // reverse order of traversal
      while (iter.hasNext()) {
        var node = iter.next();
        nodeChanged |= processNode.apply(node);
      }
    }
    return !nodeChanged;
  }

  @Override
  public String toString() {
    var s = new StringBuilder();
    for (E node : this) { // list from source to sink
      s.append("- Node [").append(node.getName()).append("]:\n");
      s.append(node.toString()).append("\n");
      s.append("inputs: [");
      s.append(inputs.get(node).stream().map(Node::getName).collect(Collectors.joining(", ")));
      s.append("]\n");
    }
    return s.toString();
  }

  public interface Node {

    default boolean isSink() {
      return false;
    }

    String getName();
  }
}
