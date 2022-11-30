package ai.datasqrl.compile.loaders;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

@Slf4j
public class DynamicExporter extends CompositeExporter {

  public DynamicExporter(List<Exporter> exporters) {
    super(loadDynamicLoaders(exporters));
  }

  public DynamicExporter(Exporter... exporters) {
    super(loadDynamicLoaders(List.of(exporters)));
  }

  private static List<Exporter> loadDynamicLoaders(List<Exporter> exporters) {
    ServiceLoader<Exporter> serviceLoader
        = ServiceLoader.load(Exporter.class);
    List<Exporter> exporterList = new ArrayList<>(exporters);
    for (Exporter exp : serviceLoader) {
      log.info("Loading dynamic exporter {}", exp.getClass().getCanonicalName());
      exporterList.add(exp);
    }
    return exporterList;
  }
}
