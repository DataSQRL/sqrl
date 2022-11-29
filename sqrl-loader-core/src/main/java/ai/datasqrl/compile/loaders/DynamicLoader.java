package ai.datasqrl.compile.loaders;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DynamicLoader extends CompositeLoader {

  public DynamicLoader(List<Loader> loaders) {
    super(loadDynamicLoaders(loaders));
  }

  public DynamicLoader(Loader... loaders) {
    this(List.of(loaders));
  }

  private static List<Loader> loadDynamicLoaders(List<Loader> loaders) {
    ServiceLoader<Loader> serviceLoader
        = ServiceLoader.load(Loader.class);
    List<Loader> loaderList = new ArrayList<>(loaders);
    for (Loader l : serviceLoader) {
      log.info("Loading dynamic loader {}", l.getClass().getCanonicalName());
      loaderList.add(l);
    }

    return loaderList;
  }
}
