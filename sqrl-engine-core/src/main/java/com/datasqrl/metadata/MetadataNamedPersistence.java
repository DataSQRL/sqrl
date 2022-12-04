package com.datasqrl.metadata;

import com.datasqrl.io.stats.TableStatisticsStoreProvider;
import com.datasqrl.io.stats.SourceTableStatistics;
import com.datasqrl.io.stats.TableStatisticsStore;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
public class MetadataNamedPersistence implements TableStatisticsStore {

  public static final String STORE_TABLE_STATS_KEY = "stats";

  private final MetadataStore store;

  private <T> void put(@NonNull T value, @NonNull NamePath path, String... suffix) {
    Pair<String, String[]> key = getKey(path, suffix);
    store.put(value, key.getKey(), key.getValue());
  }

  private <T> T get(@NonNull Class<T> clazz, @NonNull NamePath path, String... suffix) {
    Pair<String, String[]> key = getKey(path, suffix);
    return store.get(clazz, key.getKey(), key.getValue());
  }

  private boolean remove(@NonNull NamePath path, String... suffix) {
    Pair<String, String[]> key = getKey(path, suffix);
    return store.remove(key.getKey(), key.getValue());
  }

  private List<NamePath> getSubPaths(@NonNull NamePath path) {
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

  @Override
  public void putTableStatistics(NamePath path, SourceTableStatistics stats) {
    put(stats, path, STORE_TABLE_STATS_KEY);
  }

  @Override
  public SourceTableStatistics getTableStatistics(NamePath path) {
    return get(SourceTableStatistics.class, path, STORE_TABLE_STATS_KEY);
  }

  @Override
  public Map<Name, SourceTableStatistics> getTablesStatistics(NamePath basePath) {
    return getSubPaths(basePath).stream()
        .map(path -> Pair.of(path.getLast(), getTableStatistics(path))).
        filter(pair -> pair.getValue() != null)
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }


  public static class TableStatsProvider implements TableStatisticsStoreProvider {

    @Override
    public TableStatisticsStore openStore(MetadataStore metaStore) {
      return new MetadataNamedPersistence(metaStore);
    }
  }


}
