/**
 * 
 */
package com.weibo.dip.flume.extension.sink;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.lifecycle.LifecycleState;
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
public class DIPKafkaMultithreadingHDFSEventSink extends AbstractSink implements Configurable {

	private static final Logger LOGGER = LoggerFactory.getLogger(DIPKafkaMultithreadingHDFSEventSink.class);

	private static final String DIRECTORY_SEPARATOR = "/";

	private static final String FILENAME_SEPARATOR = "-";

	private FileSystem fileSystem;

	private int threads;

	private ExecutorService sinkers;

	private boolean sinkStoped;

	private String rootDirectory;

	private int batchSize;

	private CategoryWriters writers;

	private class Sinker implements Runnable {

		private SimpleDateFormat directoryDateFormat = new SimpleDateFormat("yyyy_MM_dd/HH");

		private SimpleDateFormat filenameDateFormat = new SimpleDateFormat("yyyyMMddHH");

		private String getDirectory(String timestamp) {
			return directoryDateFormat.format(new Date(Long.valueOf(timestamp)));
		}

		private String getFilename(String timestamp) {
			return filenameDateFormat.format(new Date(Long.valueOf(timestamp)));
		}

		private String getFiveMinute(String timestamp) {
			Calendar calendar = Calendar.getInstance();

			calendar.setTimeInMillis(Long.valueOf(timestamp));

			return String.format("%02d", calendar.get(Calendar.MINUTE) / 5);
		}

		private String getLookupPath(Event event) {
			Map<String, String> headers = event.getHeaders();

			String category = headers.get("topic");

			String timestamp = headers.get("timestamp");

			String hostname = headers.get("hostname");

			return rootDirectory + category + DIRECTORY_SEPARATOR + getDirectory(timestamp) + DIRECTORY_SEPARATOR
					+ category + FILENAME_SEPARATOR + hostname + FILENAME_SEPARATOR + getFilename(timestamp)
					+ FILENAME_SEPARATOR + getFiveMinute(timestamp);
		}

		@Override
		public void run() {
			LOGGER.info("Sinker " + Thread.currentThread().getName() + " started");

			while (!sinkStoped) {
				Channel channel = getChannel();

				Transaction transaction = channel.getTransaction();

				transaction.begin();

				try {
					int txnEventCount = 0;

					Map<String, List<Event>> lookupPathEvents = new HashMap<>();

					for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
						Event event = channel.take();
						if (event == null) {
							break;
						}

						String lookupPath = getLookupPath(event);

						List<Event> events = lookupPathEvents.get(lookupPath);

						if (events == null) {
							events = new ArrayList<>();

							lookupPathEvents.put(lookupPath, events);
						}

						events.add(event);
					}

					for (Entry<String, List<Event>> entry : lookupPathEvents.entrySet()) {
						String lookupPath = entry.getKey();
						List<Event> events = entry.getValue();

						writers.write(lookupPath, events);
					}

					transaction.commit();
				} catch (Exception e) {
					LOGGER.error("Sinker write error: " + ExceptionUtils.getFullStackTrace(e));

					transaction.rollback();
				} finally {
					transaction.close();
				}
			}

			LOGGER.info("Sinker " + Thread.currentThread().getName() + " stoped");
		}

	}

	private class CategoryWriters implements Closeable {

		private long lastRollTime = System.currentTimeMillis();

		private long fiveMinutes = 1 * 60 * 1000;

		private Map<String, CategoryWriter> writers = new HashMap<>();

		public void write(String lookupPath, List<Event> events)
				throws UnsupportedEncodingException, IllegalArgumentException, IOException {
			CategoryWriter writer = null;

			synchronized (writers) {
				long now = System.currentTimeMillis();

				if (now - lastRollTime >= fiveMinutes) {
					roll(now);

					lastRollTime = now;
				}

				writer = writers.get(lookupPath);

				if (writer == null) {
					writer = new CategoryWriter(lookupPath);

					writers.put(lookupPath, writer);
				}
			}

			writer.write(events);
		}

		public void roll(long now) throws IOException {
			if (MapUtils.isEmpty(writers)) {
				return;
			}

			List<String> rollLookupPaths = new ArrayList<>();

			for (Entry<String, CategoryWriter> entry : writers.entrySet()) {
				String lookupPath = entry.getKey();
				CategoryWriter writer = entry.getValue();

				if (now - writer.getCreateTime() >= fiveMinutes) {
					rollLookupPaths.add(lookupPath);
				}
			}

			if (CollectionUtils.isNotEmpty(rollLookupPaths)) {
				for (String rollLookupPath : rollLookupPaths) {
					CategoryWriter writer = writers.remove(rollLookupPath);

					try {
						writer.close();

						LOGGER.info("CategoryWriter " + writer.getPath() + " rolled success");
					} catch (IOException e) {
						LOGGER.info("CategoryWriter " + writer.getPath() + " close(rolled) error: "
								+ ExceptionUtils.getFullStackTrace(e));
					}
				}
			}
		}

		@Override
		public void close() throws IOException {
			if (MapUtils.isEmpty(writers)) {
				return;
			}

			for (Entry<String, CategoryWriter> entry : writers.entrySet()) {
				CategoryWriter writer = entry.getValue();

				try {
					writer.close();
				} catch (Exception e) {
					LOGGER.error("CategoryWriter " + writer.getPath() + " close error: "
							+ ExceptionUtils.getFullStackTrace(e));
				}
			}
		}

	}

	private class CategoryWriter implements Closeable {

		private String path;

		private long createTime = System.currentTimeMillis();

		private BufferedWriter writer;

		public CategoryWriter(String path) throws UnsupportedEncodingException, IllegalArgumentException, IOException {
			this.path = path + FILENAME_SEPARATOR + createTime;

			createTime = System.currentTimeMillis();

			writer = new BufferedWriter(
					new OutputStreamWriter(fileSystem.create(new Path(this.path)), CharEncoding.UTF_8));

			LOGGER.info("CategoryWriter " + this.path + " created success");
		}

		public String getPath() {
			return path;
		}

		public long getCreateTime() {
			return createTime;
		}

		public synchronized void write(List<Event> events) throws UnsupportedEncodingException, IOException {
			for (Event event : events) {
				writer.write(new String(event.getBody(), CharEncoding.UTF_8));
			}

			writer.flush();
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
		rootDirectory = context.getString("rootDirectory", "/tmp/");

		batchSize = context.getInteger("batchSize", 1000);

		threads = context.getInteger("threads", 1);
	}

	@Override
	public synchronized void start() {
		try {
			fileSystem = FileSystem.get(new Configuration());
		} catch (IOException e) {
			LOGGER.error("fileSystem get error: " + ExceptionUtils.getFullStackTrace(e));
		}

		writers = new CategoryWriters();

		sinkStoped = false;

		sinkers = Executors.newFixedThreadPool(threads,
				new ThreadFactoryBuilder().setNameFormat("hdfs-" + getName() + "-sink-sinker-%d").build());

		for (int index = 0; index < threads; index++) {
			sinkers.submit(new Sinker());
		}

		super.start();
	}

	@Override
	public Status process() throws EventDeliveryException {
		return Status.BACKOFF;
	}

	@Override
	public synchronized void stop() {
		if (getLifecycleState() == LifecycleState.STOP) {
			return;
		}

		sinkStoped = true;

		sinkers.shutdown();

		while (!sinkers.isTerminated()) {
			try {
				sinkers.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
			}
		}

		try {
			writers.close();
		} catch (IOException e) {
			LOGGER.error("CategoryWriters close error: " + ExceptionUtils.getFullStackTrace(e));
		}

		super.stop();
	}

}
