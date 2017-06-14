package org.graylog.esbenchmarks.http;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.graylog.esbenchmarks.common.BenchmarkBase;
import org.graylog2.bindings.providers.JestClientProvider;
import org.graylog2.indexer.IndexSet;
import org.graylog2.indexer.messages.Messages;
import org.graylog2.inputs.random.generators.FakeHttpRawMessageGenerator;
import org.graylog2.plugin.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BenchmarkHttpClientTest extends BenchmarkBase {
    private static final Logger log = LoggerFactory.getLogger(BenchmarkHttpClientTest.class);
    private static final MetricRegistry metricRegistry = new MetricRegistry();

    private static final JestClientProvider jestClientProvider = new JestClientProvider(
            ImmutableList.of(URI.create(System.getProperty("es.uri"))),
            Duration.of(60, SECONDS),
            Duration.of(30, SECONDS),
            Duration.of(60, SECONDS),
            200,
            20,
            false,
            null,
            Duration.of(10, MINUTES),
            new Gson()
    );

    private final Timer timer = metricRegistry.timer(name(BenchmarkHttpClientTest.class, "bulkIndex"));
    private final Meter throughput = metricRegistry.meter(name(BenchmarkHttpClientTest.class, "throughput"));
    private final FakeHttpRawMessageGenerator fakeHttpRawMessageGenerator = new FakeHttpRawMessageGenerator("benchmark");

    private List<Map.Entry<IndexSet, Message>> messageList;
    private Messages messages;

    @Before
    public void setUp() throws Exception {
        this.messageList = new ArrayList<>(messageCount);
        final IndexSet indexSet = mock(IndexSet.class);
        when(indexSet.getWriteIndexAlias()).thenReturn("http");

        final FakeHttpRawMessageGenerator.GeneratorState generatorState = fakeHttpRawMessageGenerator.generateState();

        for (int i = 0; i < messageCount; i++) {
            messageList.add(Maps.immutableEntry(indexSet, FakeHttpRawMessageGenerator.generateMessage(generatorState)));
        }

        this.messages = new Messages(metricRegistry, jestClientProvider.get());
    }

    @Test
    public void benchmarkBulkIndex() throws Exception {
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
