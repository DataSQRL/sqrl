//package com.datasqrl.test;
//
//import com.google.auto.service.AutoService;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.stream.Collectors;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.configuration.ConfigOption;
//import org.apache.flink.configuration.ReadableConfig;
//import org.apache.flink.connector.file.table.FileSystemTableFactory;
//import org.apache.flink.connector.file.table.FileSystemTableSource;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource;
//import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory;
//import org.apache.flink.table.connector.ChangelogMode;
//import org.apache.flink.table.connector.ProviderContext;
//import org.apache.flink.table.connector.source.DataStreamScanProvider;
//import org.apache.flink.table.connector.source.DynamicTableSource;
//import org.apache.flink.table.connector.source.ScanTableSource;
//import org.apache.flink.table.connector.source.SourceFunctionProvider;
//import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
//import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
//import org.apache.flink.table.data.RowData;
//import org.apache.flink.table.factories.DynamicTableSourceFactory;
//import org.apache.flink.table.factories.Factory;
//import org.apache.flink.table.factories.FactoryUtil;
//import org.apache.flink.table.types.DataType;
//
///**
// * Factory for creating configured instances of a Hybrid File and Kafka DynamicTableSource.
// */
//@AutoService(Factory.class)
//public class HybridFileKafkaSourceFactory implements DynamicTableSourceFactory {
//
//    @Override
//    public DynamicTableSource createDynamicTableSource(Context context) {
//        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
//        // Creating file source
//        FileSystemTableFactory fileFactory = new FileSystemTableFactory() {
//            @Override
//            public Set<ConfigOption<?>> optionalOptions() {
//                Set<ConfigOption<?>> forward = new HashSet<>();
//                forward.addAll(new FileSystemTableFactory().optionalOptions());
//                forward.addAll(new KafkaDynamicTableFactory().requiredOptions());
//                forward.addAll(new KafkaDynamicTableFactory().optionalOptions());
//                forward.addAll(new KafkaDynamicTableFactory().forwardOptions());
//                return forward;
//            }
//
//        };
//        FileSystemTableSource fileSource =(FileSystemTableSource) fileFactory.createDynamicTableSource(context);
////            helper.copy(filePath, format));
//
//        // Creating Kafka source
//        KafkaDynamicTableFactory kafkaFactory = new KafkaDynamicTableFactory() {
//            @Override
//            public Set<ConfigOption<?>> optionalOptions() {
//                Set<ConfigOption<?>> forward = new HashSet<>();
//                forward.addAll(new KafkaDynamicTableFactory().optionalOptions());
//                forward.addAll(new FileSystemTableFactory().requiredOptions());
//                forward.addAll(new FileSystemTableFactory().optionalOptions());
//                forward.addAll(new FileSystemTableFactory().forwardOptions());
//                return forward;
//            }
//        };
//        KafkaDynamicSource kafkaSource = (KafkaDynamicSource)kafkaFactory.createDynamicTableSource(context);
////            helper.copy(kafkaTopic, kafkaBootstrapServers, format));
//
//        // Combine both sources
//        return new HybridDynamicTableSource(fileSource, kafkaSource);
//    }
//
//    @Override
//    public String factoryIdentifier() {
//        return "hybrid-file-kafka";
//    }
//
//    @Override
//    public Set<ConfigOption<?>> requiredOptions() {
//        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
//        return requiredOptions;
//    }
//
//    @Override
//    public Set<ConfigOption<?>> optionalOptions() {
//        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
//        optionalOptions.addAll(new KafkaDynamicTableFactory().optionalOptions());
//        optionalOptions.addAll(new FileSystemTableFactory().optionalOptions());
//        return optionalOptions;
//    }
//}
//
///**
// * Implementation of DynamicTableSource that combines file and Kafka sources.
// */
//class HybridDynamicTableSource implements ScanTableSource, SupportsReadingMetadata,
//    SupportsWatermarkPushDown {
//    private final FileSystemTableSource fileSource;
//    private final KafkaDynamicSource kafkaSource;
//    private List<String> metadataKeys;
//    private DataType producedDataType;
//    private WatermarkStrategy<RowData> watermarkStrategy;
//
//    public HybridDynamicTableSource(FileSystemTableSource fileSource, KafkaDynamicSource kafkaSource) {
//        this.fileSource = fileSource;
//        this.kafkaSource = kafkaSource;
//    }
//
//    @Override
//    public DynamicTableSource copy() {
//        return new HybridDynamicTableSource((FileSystemTableSource)fileSource.copy(), (KafkaDynamicSource)kafkaSource.copy());
//    }
//
//    @Override
//    public String asSummaryString() {
//        return "Hybrid File-Kafka Source";
//    }
//
//    @Override
//    public ChangelogMode getChangelogMode() {
//        return kafkaSource.getChangelogMode();
//    }
//
//    @Override
//    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
//        // Obtain DataStreamScanProviders from both file and Kafka sources
//        DataStreamScanProvider fileStreamProvider = (DataStreamScanProvider) fileSource.getScanRuntimeProvider(runtimeProviderContext);
//        DataStreamScanProvider kafkaStreamProvider = (DataStreamScanProvider) kafkaSource.getScanRuntimeProvider(runtimeProviderContext);
//
//        return new DataStreamScanProvider() {
//            @Override
//            public DataStream<RowData> produceDataStream(ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
//                // Produce the DataStream for the file source
//                DataStream<RowData> fileStream = fileStreamProvider.produceDataStream(providerContext, execEnv);
//                // Set up the transformation to switch to Kafka stream after the file stream
//                DataStream<RowData> kafkaStream = kafkaStreamProvider.produceDataStream(providerContext, execEnv);
//
//                // Use a custom ProcessFunction to transfer control from file stream to Kafka stream
//                return fileStream
//                    .transform("SwitchStream",
//                        TypeInformation.of(RowData.class),
//                        new StreamSwitchProcessFunction(kafkaStream.broadcast()))
//                    .setParallelism(1); // Ensure that the switch is coordinated
//            }
//
//            @Override
//            public boolean isBounded() {
//                // Return true if both sources are bounded; adjust based on actual source properties
//                return fileStreamProvider.isBounded() && kafkaStreamProvider.isBounded();
//            }
//        };
//    }
//
//    @Override
//    public Map<String, DataType> listReadableMetadata() {
//        Map<String, DataType> fileMetadata = fileSource.listReadableMetadata();
//        Map<String, DataType> kafkaMetadata = kafkaSource.listReadableMetadata();
//        Map<String, DataType> combinedMetadata = new HashMap<>(fileMetadata);
//        kafkaMetadata.forEach(combinedMetadata::putIfAbsent);
//        return combinedMetadata;
//    }
//
//    @Override
//    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
//        this.metadataKeys = metadataKeys;
//        this.producedDataType = producedDataType;
//        fileSource.applyReadableMetadata(metadataKeys, producedDataType);
//        kafkaSource.applyReadableMetadata(metadataKeys.stream().filter(kafkaSource.listReadableMetadata()::containsKey).collect(
//            Collectors.toList()), producedDataType);
//    }
//
//    @Override
//    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
//        this.watermarkStrategy = watermarkStrategy;
////        fileSource.applyWatermark(watermarkStrategy);
//        kafkaSource.applyWatermark(watermarkStrategy);
//    }
//    class HybridSourceFunction implements SourceFunction<RowData> {
//        private final SourceFunction<RowData> firstSource;
//        private final SourceFunction<RowData> secondSource;
//        private volatile boolean isRunning = true;
//
//        public HybridSourceFunction(SourceFunction<RowData> firstSource, SourceFunction<RowData> secondSource) {
//            this.firstSource = firstSource;
//            this.secondSource = secondSource;
//        }
//
//        @Override
//        public void run(SourceContext<RowData> ctx) throws Exception {
//            // Run the first source (file)
//            firstSource.run(ctx);
//            // Upon completion, switch to the second source (Kafka)
//            if (isRunning) {
//                secondSource.run(ctx);
//            }
//        }
//
//        @Override
//        public void cancel() {
//            isRunning = false;
//            firstSource.cancel();
//            secondSource.cancel();
//        }
//    }
//}
