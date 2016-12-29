/**
 * 
 */
package com.weibo.dip.flume.extension.test;

import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.Context;
import org.apache.flume.event.EventBuilder;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weibo.dip.flume.extension.sink.scribe.EventToLogEntrySerializer;
import com.weibo.dip.flume.extension.sink.scribe.FlumeEventSerializer;
import com.weibo.dip.flume.extension.sink.scribe.LogEntry;
import com.weibo.dip.flume.extension.sink.scribe.Scribe;
import com.weibo.dip.flume.extension.sink.scribe.ScribeSinkConfigurationConstants;

/**
 * @author yurun
 * 
 *         java -Xmx2048m -cp
 *         target/test-classes/:target/flume-extension-0.0.3.jar:target/flume-
 *         extension-0.0.3-lib/*
 *         com.weibo.dip.flume.extension.test.ScribeClientMn -host 10.13.4.44
 *         -port 1467 -category app_dipsinacomkafka12345_hadooplog -threads 15
 *         -lines 100000000
 *
 */
public class OpenAPIOPScribeClientMain {

	private static final Logger LOGGER = LoggerFactory.getLogger(OpenAPIOPScribeClientMain.class);

	private static final String PREFIX;

	static {
		StringBuilder buffer = new StringBuilder();

		for (int index = 0; index < 500; index++) {
			buffer.append("word");
		}

		PREFIX = buffer.toString();
	}

	private static final String UNDERLINE = "_";

	private static final int BATCH = 1000;

	private static final AtomicLong COUNTING = new AtomicLong(0);

	private static class ScribeLogger implements Runnable {

		private String host;

		private int port = 1467;

		private String category = "app_dipsinacomkafka12345_wwwanalyzetest";

		private TTransport transport = null;

		private Scribe.Client client = null;

		public ScribeLogger(String host) {
			this.host = host;
		}

		private void flush(List<LogEntry> buffer) throws TException {
			if (CollectionUtils.isNotEmpty(buffer)) {
				client.Log(buffer);

				COUNTING.addAndGet(buffer.size());

				buffer.clear();
			}
		}

		@Override
		public void run() {
			try {
				transport = new TFramedTransport(new TSocket(new Socket(host, port)));

				client = new Scribe.Client(new TBinaryProtocol(transport, false, false));

				FlumeEventSerializer serializer = new EventToLogEntrySerializer();

				Map<String, String> parameters = new HashMap<>();

				parameters.put(ScribeSinkConfigurationConstants.CONFIG_SCRIBE_CATEGORY_HEADER, "category");

				serializer.configure(new Context(parameters));

				List<LogEntry> buffer = new ArrayList<>();

				while (true) {
					String line = PREFIX + UNDERLINE + System.currentTimeMillis();

					Map<String, String> headers = new HashMap<>();

					headers.put("category", category);

					buffer.add(serializer.serialize(EventBuilder.withBody(line.getBytes(CharEncoding.UTF_8), headers)));

					if (buffer.size() >= BATCH) {
						flush(buffer);
					}
				}
			} catch (Exception e) {
				LOGGER.error("ScribeLogger " + host + " log error: " + ExceptionUtils.getFullStackTrace(e));
			} finally {
				client = null;

				if (transport != null) {
					transport.close();
				}
			}
		}

	}

	private static class Monitor extends Thread {

		private int interval = 5;

		private long lastCount = 0;

		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(interval * 1000);
				} catch (InterruptedException e) {
				}

				long count = COUNTING.get();

				double speed = (count - lastCount) / 1.0 / interval;

				lastCount = count;

				LOGGER.info("ScribeClient log " + count + " lines, speed: " + speed + " lines/s");
			}
		}

	}

	public static void main(String[] args) {
		Monitor monitor = new Monitor();

		monitor.setDaemon(true);

		monitor.start();

		String[] hosts = { "10.13.1.121", "10.13.1.122", "10.13.2.133", "10.13.2.134", "172.16.108.108",
				"172.16.108.68", "172.16.140.67", "172.16.140.84", "172.16.142.56", "172.16.142.57", "172.16.142.58",
				"172.16.234.85", "172.16.234.92", "172.16.234.99", "172.16.235.235", "172.16.235.27", "172.16.235.28",
				"172.16.78.21", "172.16.78.83" };

		ExecutorService senders = Executors.newFixedThreadPool(hosts.length);

		for (int index = 0; index < hosts.length; index++) {
			senders.submit(new ScribeLogger(hosts[index]));
		}

		senders.shutdown();

		while (!senders.isTerminated()) {
			try {
				senders.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
			}
		}

		LOGGER.info("ScribeClient total log " + COUNTING.get() + " lines");
	}

}
