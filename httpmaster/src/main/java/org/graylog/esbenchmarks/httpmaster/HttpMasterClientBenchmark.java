package org.graylog.esbenchmarks.httpmaster;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.joschi.jadconfig.util.Duration;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.gson.Gson;
import io.searchbox.client.JestClient;
import org.graylog2.audit.NullAuditEventSender;
import org.graylog2.bindings.providers.JestClientProvider;
import org.graylog2.indexer.IndexMappingFactory;
import org.graylog2.indexer.IndexSet;
import org.graylog2.indexer.cluster.Node;
import org.graylog2.indexer.indexset.IndexSetConfig;
import org.graylog2.indexer.indices.Indices;
import org.graylog2.indexer.messages.Messages;
import org.graylog2.inputs.random.generators.FakeHttpRawMessageGenerator;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.system.NodeId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpMasterClientBenchmark {
    private static final Logger log = LoggerFactory.getLogger(HttpMasterClientBenchmark.class);
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final int messageCount = Integer.parseInt(System.getProperty("messages"));

    private static final JestClientProvider jestClientProvider = new JestClientProvider(
            ImmutableList.of(URI.create(System.getProperty("es.uri"))),
            Duration.milliseconds(60),
            Duration.seconds(30),
            Duration.seconds(60),
            200,
            20,
            false,
            null,
            Duration.minutes(10),
            true,
            new Gson()
    );

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private final Timer timer = metricRegistry.timer(name(HttpMasterClientBenchmark.class, "bulkIndex"));
        private final Meter throughput = metricRegistry.meter(name(HttpMasterClientBenchmark.class, "throughput"));
        private final FakeHttpRawMessageGenerator fakeHttpRawMessageGenerator = new FakeHttpRawMessageGenerator("benchmark");
        private final String indexName = "httpmaster";

        private List<Map.Entry<IndexSet, Message>> messageList;
        private Messages messages;
        private Indices indices;
        private IndexSet indexSet;

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            this.messageList = new ArrayList<>(messageCount);

            final IndexSetConfig indexSetConfig = mock(IndexSetConfig.class);
            when(indexSetConfig.shards()).thenReturn(4);
            when(indexSetConfig.replicas()).thenReturn(0);
            when(indexSetConfig.indexAnalyzer()).thenReturn("standard");
            when(indexSetConfig.indexTemplateName()).thenReturn("template");

            this.indexSet = mock(IndexSet.class);
            when(indexSet.getConfig()).thenReturn(indexSetConfig);
            when(indexSet.getWriteIndexAlias()).thenReturn(indexName);
            when(indexSet.getIndexWildcard()).thenReturn(indexName);

            final FakeHttpRawMessageGenerator.GeneratorState generatorState = fakeHttpRawMessageGenerator.generateState();

            for (int i = 0; i < messageCount; i++) {
                messageList.add(Maps.immutableEntry(indexSet, FakeHttpRawMessageGenerator.generateMessage(generatorState)));
            }

            final File tempNodeFile = File.createTempFile("temp-node-file", ".tmp");
            final NodeId nodeId = new NodeId(tempNodeFile.getAbsolutePath());

            final JestClient jestClient = jestClientProvider.get();
            final Node node = new Node(jestClient);
            this.messages = new Messages(metricRegistry, jestClient);
            this.indices = new Indices(jestClient, new Gson(), new IndexMappingFactory(node), this.messages, nodeId, new NullAuditEventSender(), new EventBus());
        }

        @Setup(Level.Invocation)
        public void cycleIndex() throws Exception {
            try {
                this.indices.delete(indexName);
            } catch (Exception ignored) {}
            this.indices.create(indexName, indexSet);
        }

        @TearDown
        public void tearDown() throws Exception {
            final ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                    .filter((name, metric) -> name.startsWith("org.graylog.esbenchmarks"))
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();

            reporter.report();
        }
    }

    @Benchmark @BenchmarkMode(Mode.Throughput) @OutputTimeUnit(TimeUnit.SECONDS)
    public void benchmarkBulkIndex(BenchmarkState state) throws Exception {
        log.debug("Starting bulk indexing of {} messages.", messageCount);
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try (Timer.Context ignored = state.timer.time()) {
            state.messages.bulkIndex(state.messageList);
            state.throughput.mark(messageCount);
        }
        log.debug("Bulk indexing {} messages took {} ms.", messageCount, stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
    }
}
