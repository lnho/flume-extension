/**
 * 
 */
package com.weibo.dip.flume.extension.source;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.kafka.KafkaSourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.kafka.KafkaSourceConstants;
import org.apache.flume.source.kafka.KafkaSourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * @author yurun
 *
 */
public class MultithreadingKafkaSource extends AbstractSource implements EventDrivenSource, Configurable {

	private static final Logger LOGGER = LoggerFactory.getLogger(MultithreadingKafkaSource.class);

	private ConsumerConnector consumer;

	private ConsumerIterator<byte[], byte[]> it;

	private String topics;

	private int threads;

	private int batchUpperLimit;

	private int timeUpperLimit;

	private Properties kafkaProps;

	private boolean kafkaAutoCommitEnabled;

	private String hostname;

	private ExecutorService executor = Executors.newCachedThreadPool();

	private KafkaSourceCounter counter;

	@Override
	public void configure(Context context) {
		topics = context.getString("topics").replaceAll(",", "|");

		Preconditions.checkState(StringUtils.isNotEmpty(topics), "topics's value must not be empty");

		threads = context.getInteger("threads", 1);

		Preconditions.checkState(threads > 0, "threads's value must be greater than zero");

		batchUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_SIZE, KafkaSourceConstants.DEFAULT_BATCH_SIZE);

		timeUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_DURATION_MS,
				KafkaSourceConstants.DEFAULT_BATCH_DURATION);

		kafkaProps = KafkaSourceUtil.getKafkaProperties(context);

		kafkaAutoCommitEnabled = Boolean.parseBoolean(kafkaProps.getProperty(KafkaSourceConstants.AUTO_COMMIT_ENABLED));

		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}

		if (counter == null) {
			counter = new KafkaSourceCounter(getName());
		}
	}

	private class KafkaConsumer implements Runnable {

		private KafkaStream<byte[], byte[]> stream;

		public KafkaConsumer(KafkaStream<byte[], byte[]> stream) {
			this.stream = stream;
		}

		private void flush(List<Event> events) {
			if (CollectionUtils.isNotEmpty(events)) {
				getChannelProcessor().processEventBatch(events);

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(getName() + " write " + events.size() + " events to channel");
				}

				if (!kafkaAutoCommitEnabled) {
					consumer.commitOffsets();
				}

				events.clear();
			}
		}

		@Override
		public void run() {
			LOGGER.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$" + Thread.currentThread().getName() + " started");
			
			ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

			List<Event> events = new ArrayList<Event>();

			long batchEndTime = System.currentTimeMillis() + timeUpperLimit;

			byte[] kafkaMessage = null;

			Map<String, String> headers = null;

			while (iterator.hasNext()) {
				LOGGER.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");

				if (events.size() < batchUpperLimit && System.currentTimeMillis() < batchEndTime) {
					MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();

					kafkaMessage = messageAndMetadata.message();

					headers = new HashMap<String, String>();

					headers.put(KafkaSourceConstants.TOPIC, messageAndMetadata.topic());
					headers.put(KafkaSourceConstants.TIMESTAMP, String.valueOf(System.currentTimeMillis()));
					headers.put("hostname", hostname);

					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Message: " + new String(kafkaMessage) + ", Headers: " + headers);
					}

					events.add(EventBuilder.withBody(kafkaMessage, headers));
				} else {
					flush(events);

					batchEndTime = System.currentTimeMillis() + timeUpperLimit;
				}
			}

			flush(events);

			LOGGER.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$" + Thread.currentThread().getName() + " stoped");
		}

	}

	@Override
	public synchronized void start() {
		LOGGER.info("Starting {}...", this);

		try {
			// initialize a consumer. This creates the connection to ZooKeeper
			consumer = KafkaSourceUtil.getConsumer(kafkaProps);

			TopicFilter topicFilter = new Whitelist(topics);

			List<KafkaStream<byte[], byte[]>> streams = consumer.createMessageStreamsByFilter(topicFilter, threads);

			counter.start();

			for (KafkaStream<byte[], byte[]> stream : streams) {
				executor.submit(new KafkaConsumer(stream));
			}

			super.start();

			LOGGER.info("Kafka source {} started.", getName());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized void stop() {
		if (consumer != null) {
			consumer.shutdown();
		}

		executor.shutdown();

		while (!executor.isTerminated()) {
			try {
				executor.awaitTermination(3, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
			}
		}

		counter.stop();

		super.stop();

		LOGGER.info("Kafka Source {} stopped. Metrics: {}", getName(), counter);
	}

}
