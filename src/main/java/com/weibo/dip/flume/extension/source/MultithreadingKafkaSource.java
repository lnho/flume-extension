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
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
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

	private String topics;

	private int threads;

	private int batchUpperLimit;

	private int timeUpperLimit;

	private Properties kafkaProps;

	private String hostname;

	private ConsumerConnector consumer;

	private ExecutorService executor = Executors.newCachedThreadPool();

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

		kafkaProps.put(KafkaSourceConstants.AUTO_COMMIT_ENABLED, "true");
		kafkaProps.put(KafkaSourceConstants.CONSUMER_TIMEOUT, "-1");

		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	private class KafkaConsumer implements Runnable {

		private KafkaStream<byte[], byte[]> stream;

		private ChannelProcessor channelProcessor;

		public KafkaConsumer(KafkaStream<byte[], byte[]> stream, ChannelProcessor channelProcessor) {
			this.stream = stream;

			this.channelProcessor = channelProcessor;
		}

		private void flush(List<Event> events) {
			if (CollectionUtils.isNotEmpty(events)) {
				try {
					channelProcessor.processEventBatch(events);

					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("KafkaConsumer " + Thread.currentThread().getName() + " flush " + events.size()
								+ " events to channel");
					}

					events.clear();
				} catch (Exception e) {
					LOGGER.error("KafkaConsumer flush error: " + ExceptionUtils.getFullStackTrace(e));
				}
			}
		}

		@Override
		public void run() {
			LOGGER.info("KafkaConsumer " + Thread.currentThread().getName() + " starting...");

			try {
				ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

				List<Event> events = new ArrayList<Event>();

				long batchEndTime = System.currentTimeMillis() + timeUpperLimit;

				byte[] kafkaMessage = null;

				Map<String, String> headers = null;

				while (iterator.hasNext()) {
					MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();

					kafkaMessage = messageAndMetadata.message();

					headers = new HashMap<String, String>();

					headers.put(KafkaSourceConstants.TOPIC, messageAndMetadata.topic());
					headers.put(KafkaSourceConstants.TIMESTAMP, String.valueOf(System.currentTimeMillis()));
					headers.put("hostname", hostname);

					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Message: " + new String(kafkaMessage) + ", Headers: " + headers);
					}

					events.add(EventBuilder.withBody(kafkaMessage, headers));

					if (events.size() >= batchUpperLimit || System.currentTimeMillis() > batchEndTime) {
						flush(events);

						batchEndTime = System.currentTimeMillis() + timeUpperLimit;
					}
				}

				flush(events);
			} catch (Throwable e) {
				LOGGER.error("KafkaConsumer error: " + ExceptionUtils.getFullStackTrace(e));
			}

			LOGGER.info("KafkaConsumer " + Thread.currentThread().getName() + " stoped");
		}

	}

	@Override
	public synchronized void start() {
		LOGGER.info("Starting {}...", this);

		try {
			consumer = KafkaSourceUtil.getConsumer(kafkaProps);

			TopicFilter topicFilter = new Whitelist(topics);

			List<KafkaStream<byte[], byte[]>> streams = consumer.createMessageStreamsByFilter(topicFilter, threads);

			for (KafkaStream<byte[], byte[]> stream : streams) {
				executor.submit(new KafkaConsumer(stream, getChannelProcessor()));
			}

			super.start();

			LOGGER.info("Kafka source {} started.", getName());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized void stop() {
		consumer.shutdown();

		executor.shutdown();

		while (!executor.isTerminated()) {
			try {
				executor.awaitTermination(3, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
			}
		}

		super.stop();

		LOGGER.info("Kafka Source {} stopped.", getName());
	}

}
