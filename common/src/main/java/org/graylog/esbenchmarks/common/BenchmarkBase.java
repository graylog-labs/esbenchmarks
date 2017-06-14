package org.graylog.esbenchmarks.common;

public class BenchmarkBase {
    protected static final int messageCount = Integer.parseInt(System.getProperty("messages"));
    protected static final int iterationCount = Integer.parseInt(System.getProperty("iterations"));
}
