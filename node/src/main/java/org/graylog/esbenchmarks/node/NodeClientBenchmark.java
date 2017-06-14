package org.graylog.esbenchmarks.node;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.joschi.jadconfig.util.Duration;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.node.Node;
import org.graylog2.audit.NullAuditEventSender;
import org.graylog2.bindings.providers.EsClientProvider;
import org.graylog2.bindings.providers.EsNodeProvider;
import org.graylog2.configuration.ElasticsearchConfiguration;
import org.graylog2.indexer.IndexMapping;
import org.graylog2.indexer.IndexSet;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeClientBenchmark {
    private static final Logger log = LoggerFactory.getLogger(NodeClientBenchmark.class);
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final int messageCount = Integer.parseInt(System.getProperty("messages"));
    private static final String indexName = "node";

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private final FakeHttpRawMessageGenerator fakeHttpRawMessageGenerator = new FakeHttpRawMessageGenerator("benchmark");
        private final Timer timer = metricRegistry.timer(name(NodeClientBenchmark.class, "bulkIndex"));
        private final Meter throughput = metricRegistry.meter(name(NodeClientBenchmark.class, "throughput"));
        private final String indexName = "node";

        private List<Map.Entry<IndexSet, Message>> messageList;

        private Messages messages;
        private Indices indices;
        private IndexSet indexSet;

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            final ElasticsearchConfiguration esConfig = new ElasticsearchConfiguration() {
                @Override
                public List<String> getUnicastHosts() {
                    return ImmutableList.of(System.getProperty("es.host"));
                }

                @Override
                public String getClusterName() {
                    return System.getProperty("es.cluster");
                }
            };

            final File tempNodeFile = File.createTempFile("temp-node-file", ".tmp");
            final NodeId nodeId = new NodeId(tempNodeFile.getAbsolutePath());
            final EsNodeProvider esNodeProvider = new EsNodeProvider(esConfig, nodeId);
            final Node node = esNodeProvider.get().start();

            waitForNode(node);

            final EsClientProvider esClientProvider = new EsClientProvider(node, metricRegistry, Duration.parse("60s"));
            final Client esClient = esClientProvider.get();
            this.messages = new Messages(esClient, metricRegistry);
            this.indices = new Indices(esClient, new IndexMapping(), messages, nodeId, new NullAuditEventSender());

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

            this.messageList = new ArrayList<>(messageCount);

            for (int i = 0; i < messageCount; i++) {
                messageList.add(Maps.immutableEntry(indexSet, FakeHttpRawMessageGenerator.generateMessage(generatorState)));
            }
        }

        @Setup(Level.Invocation)
        public void cycleIndex() throws Exception {
            try {
                this.indices.delete(indexName);
            } catch (Exception ignored) {}
            this.indices.create(indexName, indexSet);
        }

        private void waitForNode(Node node) {
            final Client client = node.client();
            final ClusterHealthRequest atLeastRed = client.admin().cluster().prepareHealth()
                    .setWaitForStatus(ClusterHealthStatus.RED)
                    .request();
            final ClusterHealthResponse health = client.admin().cluster().health(atLeastRed)
                    .actionGet(30000, MILLISECONDS);

            // we don't get here if we couldn't join the cluster. just check for red cluster state
            if (ClusterHealthStatus.RED.equals(health.getStatus())) {
                log.warn("The Elasticsearch cluster state is RED which means shards are unassigned.");
                log.info("This usually indicates a crashed and corrupt cluster and needs to be investigated. Graylog will write into the local disk journal.");
            }
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
    public void benchmarkNode(BenchmarkState state) throws Exception {
        log.debug("Starting bulk indexing of {} messages.", messageCount);
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try (Timer.Context ignored = state.timer.time()) {
            state.messages.bulkIndex(state.messageList);
            state.throughput.mark(messageCount);
        }
        log.debug("Bulk indexing {} messages took {} ms.", messageCount, stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
    }
}
