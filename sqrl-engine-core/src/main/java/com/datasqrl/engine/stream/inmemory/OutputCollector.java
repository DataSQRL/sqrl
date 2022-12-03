package com.datasqrl.engine.stream.inmemory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;

public class OutputCollector<K,V> {

    private Multimap<K, V> elements = ArrayListMultimap.create();

    public Collection<V> get(K key) {
        return elements.get(key);
    }

    public Collection<V> getAll() {
        return elements.values();
    }

    public Consumer<V> getCollector(K key) {
        return e -> elements.put(Objects.requireNonNull(key),e);
    }

}
