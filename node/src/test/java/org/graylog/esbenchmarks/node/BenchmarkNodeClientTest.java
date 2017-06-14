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
import org.graylog.esbenchmarks.common.BenchmarkBase;
import org.graylog2.audit.NullAuditEventSender;
import org.graylog2.bindings.providers.EsClientProvider;
import org.graylog2.bindings.providers.EsNodeProvider;
import org.graylog2.configuration.ElasticsearchConfiguration;
import org.graylog2.indexer.IndexMapping;
import org.graylog2.indexer.IndexNotFoundException;
import org.graylog2.indexer.IndexSet;
import org.graylog2.indexer.indexset.IndexSetConfig;
import org.graylog2.indexer.indices.Indices;
import org.graylog2.indexer.messages.Messages;
import org.graylog2.inputs.random.generators.FakeHttpRawMessageGenerator;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.system.NodeId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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

public class BenchmarkNodeClientTest extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(BenchmarkNodeClientTest.class);
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final String indexName = "node";

    private final FakeHttpRawMessageGenerator fakeHttpRawMessageGenerator = new FakeHttpRawMessageGenerator("benchmark");
    private final Timer timer = metricRegistry.timer(name(BenchmarkNodeClientTest.class, "bulkIndex"));
    private final Meter throughput = metricRegistry.meter(name(BenchmarkNodeClientTest.class, "throughput"));

    private List<Map.Entry<IndexSet, Message>> messageList;

    private Messages messages;

    @Before
    public void setUp() throws Exception {
        final ElasticsearchConfiguration esConfig = new ElasticsearchConfiguration() {
            @Override
            public List<String> getUnicastHosts() {
                return ImmutableList.of(System.getProperty("es.host"));
            }

            @Override
            public String getClusterName() {
                return "graylog2-dennis";
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
        final Indices indices = new Indices(esClient, new IndexMapping(), messages, nodeId, new NullAuditEventSender());

        final IndexSetConfig indexSetConfig = mock(IndexSetConfig.class);
        when(indexSetConfig.shards()).thenReturn(4);
        when(indexSetConfig.replicas()).thenReturn(0);
        when(indexSetConfig.indexAnalyzer()).thenReturn("standard");
        when(indexSetConfig.indexTemplateName()).thenReturn("template");

        final IndexSet indexSet = mock(IndexSet.class);
        when(indexSet.getConfig()).thenReturn(indexSetConfig);
        when(indexSet.getWriteIndexAlias()).thenReturn(indexName);
        when(indexSet.getIndexWildcard()).thenReturn(indexName);

        try {
            indices.delete(indexName);
            indices.delete("http");
            indices.delete("httpmaster");
        } catch (Exception ignored) {}

        indices.create(indexName, indexSet);

        final FakeHttpRawMessageGenerator.GeneratorState generatorState = fakeHttpRawMessageGenerator.generateState();

        this.messageList = new ArrayList<>(messageCount);

        for (int i = 0; i < messageCount; i++) {
            messageList.add(Maps.immutableEntry(indexSet, FakeHttpRawMessageGenerator.generateMessage(generatorState)));
        }
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

    @Test
    public void benchmarkNode() throws Exception {
        log.error("Starting bulk indexing of {} messages for {} times.", messageCount, iterationCount);
        final Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < iterationCount; i++) {
            try (Timer.Context ignored = timer.time()) {
                messages.bulkIndex(messageList);
                throughput.mark(messageCount);
            }
        }
        log.error("Bulk indexing {} messages for {} times took {} ms.", messageCount, iterationCount, stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
    }

    @After
    public void tearDown() throws Exception {
        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                .filter((name, metric) -> name.startsWith("org.graylog.esbenchmarks"))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.report();
    }
}
