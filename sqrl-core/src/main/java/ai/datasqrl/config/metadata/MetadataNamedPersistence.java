package ai.datasqrl.config.metadata;

import ai.datasqrl.config.provider.TableStatisticsStoreProvider;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.io.sources.stats.TableStatisticsStore;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class MetadataNamedPersistence implements TableStatisticsStore {

  public static final String STORE_TABLE_STATS_KEY = "stats";

  private final MetadataStore store;

  private <T> void put(@NonNull T value, @NonNull NamePath path, String... suffix) {
    Pair<String,String[]> key = getKey(path,suffix);
    store.put(value,key.getKey(),key.getValue());
  }

  private <T> T get(@NonNull Class<T> clazz, @NonNull NamePath path, String... suffix) {
    Pair<String,String[]> key = getKey(path,suffix);
    return store.get(clazz,key.getKey(),key.getValue());
  }

  private boolean remove(@NonNull NamePath path, String... suffix) {
    Pair<String,String[]> key = getKey(path,suffix);
    return store.remove(key.getKey(),key.getValue());
  }

  private Pair<String,String[]> getKey(@NonNull NamePath path, String... suffix) {
    List<String> components = new ArrayList<>();
    path.stream().map(Name::getCanonical).forEach(components::add);
    if (suffix!=null && suffix.length>0) {
      for (String s : suffix) {
        components.add(s);
      }
    }
    assert components.size()>=1;
    return Pair.of(components.get(0),components.subList(1,components.size()).toArray(new String[components.size()-1]));
  }


  @Override
  public void close() throws IOException {
    store.close();
  }

  @Override
  public void putTableStatistics(NamePath path, SourceTableStatistics stats) {
    put(stats,path,STORE_TABLE_STATS_KEY);
  }

  @Override
  public SourceTableStatistics getTableStatistics(NamePath path) {
    return get(SourceTableStatistics.class,path,STORE_TABLE_STATS_KEY);
  }


  public static class TableStatsProvider implements TableStatisticsStoreProvider {

    @Override
    public TableStatisticsStore openStore(MetadataStore metaStore) {
      return new MetadataNamedPersistence(metaStore);
    }
  }


}
