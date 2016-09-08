/**
 * 
 */
package com.weibo.dip.flume.extension.sink;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
import org.apache.commons.lang.StringUtils;
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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author yurun
 *
 */
public class MultithreadingHDFSEventSink extends AbstractSink implements Configurable {

	private static final Logger LOGGER = LoggerFactory.getLogger(MultithreadingHDFSEventSink.class);

	private static final String DIRECTORY_SEPARATOR = "/";

	private static final String FILENAME_SEPARATOR = "-";

	private FileSystem fileSystem;

	private ExecutorService sinkers;

	private boolean sinkStoped;

	private String hostname;

	private String rootDirectory;

	private String timePartition;

	private int threads;

	private int batchSize;

	private long sinkSleep;

	private String categoryHeaderName;

	private boolean useLocaltime;

	private String timestampHeaderName;

	private long rollTime;

	private CategoryWriters writers;

	private class Sinker implements Runnable {

		private SimpleDateFormat directoryDateFormat = new SimpleDateFormat(timePartition);

		private SimpleDateFormat filenameDateFormat = new SimpleDateFormat("yyyyMMddHH");

		private String getDirectory(String timestamp) throws NumberFormatException {
			return directoryDateFormat.format(new Date(Long.valueOf(timestamp)));
		}

		private String getFilename(String timestamp) throws NumberFormatException {
			return filenameDateFormat.format(new Date(Long.valueOf(timestamp)));
		}

		private String getFiveMinute(String timestamp) throws NumberFormatException {
			Calendar calendar = Calendar.getInstance();

			calendar.setTimeInMillis(Long.valueOf(timestamp));

			return String.format("%02d", calendar.get(Calendar.MINUTE) / 5);
		}

		private String getLookupPath(Event event) throws Exception {
			Map<String, String> headers = event.getHeaders();

			String category = headers.get(categoryHeaderName);

			Preconditions.checkState(StringUtils.isNotEmpty(category),
					"category is empty, categoryHeaderName(" + categoryHeaderName + ") may be wrong, check");

			String timestamp = null;

			if (useLocaltime) {
				timestamp = String.valueOf(System.currentTimeMillis());
			} else {
				timestamp = headers.get(timestampHeaderName);

				Preconditions.checkState(StringUtils.isNotEmpty(category),
						"timestamp is empty, timestampHeaderName(" + timestampHeaderName + ") may be wrong, check");
			}

			return rootDirectory + category + DIRECTORY_SEPARATOR + getDirectory(timestamp) + DIRECTORY_SEPARATOR
					+ category + FILENAME_SEPARATOR + hostname + FILENAME_SEPARATOR + getFilename(timestamp)
					+ FILENAME_SEPARATOR + getFiveMinute(timestamp);
		}

		@Override
		public void run() {
			String sinkName = Thread.currentThread().getName();

			LOGGER.info(sinkName + " started");

			Channel channel = getChannel();

			Transaction transaction = null;

			Map<String, List<Event>> lookupPathEvents = new HashMap<>();

			while (!sinkStoped) {
				transaction = channel.getTransaction();

				transaction.begin();

				try {
					lookupPathEvents.clear();

					int txnEventCount = 0;

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

					if (MapUtils.isEmpty(lookupPathEvents)) {
						try {
							Thread.sleep(sinkSleep);
						} catch (InterruptedException e) {
						}
					}
				} catch (Exception e) {
					LOGGER.error(sinkName + " write error: " + ExceptionUtils.getFullStackTrace(e));

					transaction.rollback();
				} finally {
					transaction.close();
				}
			}

			LOGGER.info(sinkName + " stoped");
		}

	}

	private class CategoryWriters implements Closeable {

		private Map<String, CategoryWriter> writers = new HashMap<>();

		private boolean rollStop = false;

		private ExecutorService rollers = Executors.newSingleThreadExecutor(
				new ThreadFactoryBuilder().setNameFormat("hdfs-" + getName() + "-roll-roller-%d").build());

		public CategoryWriters() {
			rollers.submit(new Roller());
		}

		private class Roller implements Runnable {

			@Override
			public void run() {
				while (!rollStop) {
					try {
						Thread.sleep(rollTime);
					} catch (InterruptedException e) {
						break;
					}

					roll();
				}
			}

			public void roll() {
				synchronized (writers) {
					if (MapUtils.isEmpty(writers)) {
						return;
					}

					long now = System.currentTimeMillis();

					List<String> rollLookupPaths = new ArrayList<>();

					for (Entry<String, CategoryWriter> entry : writers.entrySet()) {
						String lookupPath = entry.getKey();
						CategoryWriter writer = entry.getValue();

						if (now - writer.getCreateTime() >= rollTime) {
							rollLookupPaths.add(lookupPath);
						}
					}

					if (CollectionUtils.isNotEmpty(rollLookupPaths)) {
						for (String rollLookupPath : rollLookupPaths) {
							CategoryWriter writer = writers.remove(rollLookupPath);

							try {
								writer.close();

								LOGGER.info("CategoryWriter " + writer.getPath() + " closed(rolled)");
							} catch (IOException e) {
								LOGGER.info("CategoryWriter " + writer.getPath() + " close(rolled) error: "
										+ ExceptionUtils.getFullStackTrace(e));
							}
						}
					}
				}
			}
		}

		public void write(String lookupPath, List<Event> events) throws IOException {
			CategoryWriter writer = writers.get(lookupPath);

			if (writer == null) {
				synchronized (writers) {
					writer = writers.get(lookupPath);

					if (writer == null) {
						writer = new CategoryWriter(lookupPath);

						writers.put(lookupPath, writer);
					}
				}
			}

			try {
				writer.write(events);
			} catch (Exception e) {
				if (e instanceof AlreadyClosedException) {
					synchronized (writers) {
						writer = writers.get(lookupPath);

						if (writer == null) {
							writer = new CategoryWriter(lookupPath);

							writers.put(lookupPath, writer);
						}

						try {
							writer.write(events);
						} catch (Exception se) {
							LOGGER.error("CategoryWriter " + writer.getPath() + " write(retry) error: "
									+ ExceptionUtils.getFullStackTrace(se));

							throw new IOException(se);
						}
					}
				} else {
					LOGGER.error("CategoryWriter " + writer.getPath() + " write error: "
							+ ExceptionUtils.getFullStackTrace(e));

					throw new IOException(e);
				}
			}
		}

		@Override
		public void close() throws IOException {
			rollStop = true;

			rollers.shutdownNow();

			while (!rollers.isTerminated()) {
				try {
					rollers.awaitTermination(1, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
				}
			}

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

	public static class AlreadyClosedException extends IOException {

		private static final long serialVersionUID = 1L;

	}

	private class CategoryWriter implements Closeable {

		private String path;

		private long createTime;

		private BufferedWriter writer;

		public CategoryWriter(String path) throws UnsupportedEncodingException, IllegalArgumentException, IOException {
			this.createTime = System.currentTimeMillis();

			this.path = path + FILENAME_SEPARATOR + createTime;

			writer = new BufferedWriter(
					new OutputStreamWriter(fileSystem.create(new Path(this.path)), CharEncoding.UTF_8));

			LOGGER.info("CategoryWriter " + this.path + " created");
		}

		public String getPath() {
			return path;
		}

		public long getCreateTime() {
			return createTime;
		}

		private void checkOpen() throws IOException {
			if (writer == null) {
				throw new AlreadyClosedException();
			}
		}

		public synchronized void write(List<Event> events) throws UnsupportedEncodingException, IOException {
			checkOpen();

			for (Event event : events) {
				writer.write(new String(event.getBody(), CharEncoding.UTF_8));
			}

			writer.flush();
		}

		@Override
		public synchronized void close() throws IOException {
			if (writer != null) {
				writer.close();

				writer = null;
			}
		}

	}

	@Override
	public void configure(Context context) {
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			LOGGER.error("get hostname error: " + ExceptionUtils.getFullStackTrace(e));
		}
		LOGGER.info("hostname: {}", hostname);

		Preconditions.checkState(StringUtils.isNotEmpty(hostname), "hostname may get error, check");

		rootDirectory = context.getString("rootDirectory", "/tmp/");
		LOGGER.info("rootDirectory: {}", rootDirectory);

		Preconditions.checkState(StringUtils.isNotEmpty(rootDirectory), "rootDirectory's value must not be empty");

		timePartition = context.getString("timePartition", "yyyy_MM_dd/HH");
		LOGGER.info("timePartition: {}", timePartition);

		Preconditions.checkState(StringUtils.isNotEmpty(timePartition), "timePartition's value must not be empty");

		threads = context.getInteger("threads", 1);
		LOGGER.info("threads: {}", threads);

		Preconditions.checkState(threads > 0, "threads's value must be greater than zero");

		batchSize = context.getInteger("batchSize", 1000);
		LOGGER.info("batchSize: {}", batchSize);

		Preconditions.checkState(batchSize > 0, "batchSize's value must be greater than zero");

		sinkSleep = context.getLong("sinkSleep", 1000L);
		LOGGER.info("sinkSleep: {}", sinkSleep);

		Preconditions.checkState(sinkSleep > 0, "sinkSleep's value must be greater than zero");

		categoryHeaderName = context.getString("categoryHeaderName");
		LOGGER.info("categoryHeaderName: {}", categoryHeaderName);

		Preconditions.checkState(StringUtils.isNotEmpty(categoryHeaderName),
				"categoryHeaderName's value must not be empty");

		useLocaltime = context.getBoolean("useLocaltime", false);
		LOGGER.info("useLocaltime: {}", useLocaltime);

		if (!useLocaltime) {
			timestampHeaderName = context.getString("timestampHeaderName");
			LOGGER.info("timestampHeaderName: {}", timestampHeaderName);

			Preconditions.checkState(StringUtils.isNotEmpty(timestampHeaderName),
					"timestampHeaderName value must not be empty, because of useLocaltime is false");
		}

		rollTime = context.getLong("rollTime", 300000L);
		LOGGER.info("rollTime: {}", rollTime);

		Preconditions.checkState(rollTime > 0, "rollTime's value must be greater than zero");
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
				this.wait(1000);
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
