/**
 * 
 */
package com.weibo.dip.flume.extension.sink;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author yurun
 *
 */
public class DIPMultithreadingHDFSEventSink extends AbstractSink implements Configurable {

	private static final Logger LOGGER = LoggerFactory.getLogger(DIPMultithreadingHDFSEventSink.class);

	private int threads;

	private ExecutorService sinkers;

	private boolean sinkStoped;

	private int batchSize;

	private BufferedWriter[] writers;

	private class Sinker implements Runnable {

		private BufferedWriter writer;

		public Sinker(BufferedWriter writer) {
			this.writer = writer;
		}

		@Override
		public void run() {
			while (!sinkStoped) {
				Channel channel = getChannel();

				Transaction transaction = channel.getTransaction();

				transaction.begin();

				try {
					int txnEventCount = 0;

					List<Event> events = new ArrayList<>();

					for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
						Event event = channel.take();
						if (event == null) {
							break;
						}

						events.add(event);
					}

					for (Event event : events) {
						writer.write(new String(event.getBody()));
					}

					writer.flush();

					transaction.commit();
				} catch (Exception e) {
					transaction.rollback();
				} finally {
					transaction.close();
				}
			}
		}

	}

	private class CategoryWriter implements Closeable {

		private BufferedWriter writer;

		public CategoryWriter(String path) {

		}

		public void write(List<Event> events) {

		}

		@Override
		public void close() throws IOException {
			if (writer != null) {
				writer.close();
			}
		}

	}

	@Override
	public void configure(Context context) {
		threads = context.getInteger("threads", 1);

		sinkers = Executors.newFixedThreadPool(threads,
				new ThreadFactoryBuilder().setNameFormat("hdfs-" + getName() + "-sink-sinker-%d").build());

		batchSize = context.getInteger("batchSize", 1000);
	}

	@Override
	public synchronized void start() {
		try {
			writers = new BufferedWriter[threads];

			for (int index = 0; index < threads; index++) {
				writers[index] = new BufferedWriter(new OutputStreamWriter(
						FileSystem.get(new Configuration()).create(new Path("/tmp/events/flume.data" + index))));
			}
		} catch (Exception e) {
			LOGGER.error("create writer error: " + ExceptionUtils.getFullStackTrace(e));
		}

		sinkStoped = false;

		for (int index = 0; index < threads; index++) {
			sinkers.submit(new Sinker(writers[index]));
		}

		super.start();
	}

	@Override
	public Status process() throws EventDeliveryException {
		return Status.BACKOFF;
	}

	@Override
	public synchronized void stop() {
		sinkStoped = true;

		sinkers.shutdown();

		while (!sinkers.isTerminated()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}

		try {
			for (int index = 0; index < threads; index++) {
				writers[index].close();
			}
		} catch (IOException e) {
		}

		super.stop();
	}

}
