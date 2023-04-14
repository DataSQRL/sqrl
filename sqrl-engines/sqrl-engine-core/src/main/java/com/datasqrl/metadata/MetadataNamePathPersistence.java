/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.metadata;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor
public class MetadataNamePathPersistence implements Closeable {

  private final MetadataStore store;

  public <T> void put(@NonNull T value, @NonNull NamePath path, String... suffix) {
    Pair<String, String[]> key = getKey(path, suffix);
    store.put(value, key.getKey(), key.getValue());
  }

  public <T> T get(@NonNull Class<T> clazz, @NonNull NamePath path, String... suffix) {
    Pair<String, String[]> key = getKey(path, suffix);
    return store.get(clazz, key.getKey(), key.getValue());
  }

  public boolean remove(@NonNull NamePath path, String... suffix) {
    Pair<String, String[]> key = getKey(path, suffix);
    return store.remove(key.getKey(), key.getValue());
  }

  public List<NamePath> getSubPaths(@NonNull NamePath path) {
    return store.getSubKeys(getKey(path)).stream()
        .map(s -> path.concat(Name.system(s))).collect(Collectors.toList());
  }

  private List<String> getKeyInternal(@NonNull NamePath path, String... suffix) {
    List<String> components = new ArrayList<>();
    path.stream().map(Name::getCanonical).forEach(components::add);
    if (suffix != null && suffix.length > 0) {
      for (String s : suffix) {
        components.add(s);
      }
    }
    assert components.size() >= 1;
    return components;
  }

  private Pair<String, String[]> getKey(@NonNull NamePath path, String... suffix) {
    List<String> components = getKeyInternal(path, suffix);
    return Pair.of(components.get(0),
        components.subList(1, components.size()).toArray(new String[components.size() - 1]));
  }

  private String[] getKey(@NonNull NamePath path) {
    List<String> components = getKeyInternal(path);
    return components.toArray(new String[components.size()]);
  }


  @Override
  public void close() throws IOException {
    store.close();
  }

}
