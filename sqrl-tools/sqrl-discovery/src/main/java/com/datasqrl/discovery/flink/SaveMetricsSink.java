package com.datasqrl.discovery.flink;

import com.datasqrl.metadata.MetricStore;
import com.datasqrl.io.util.Metric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class SaveMetricsSink<M extends Metric<M>> extends RichSinkFunction<M> {

  private final MetricStore.Provider<M> storeProvider;

  private transient MetricStore<M> store;

  public SaveMetricsSink(MetricStore.Provider<M> storeProvider) {
    this.storeProvider = storeProvider;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    store = storeProvider.open();
  }

  @Override
  public void close() throws Exception {
    store.close();
    store = null;
  }

  @Override
  public void invoke(M metric, Context context) throws Exception {
    store.put(metric);
  }


}
