/**
 * 
 */
package com.weibo.dip.flume.extension.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.kafka.KafkaSourceConstants;
import org.apache.flume.source.kafka.KafkaSourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

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

	private ConsumerConnector consumerConnector;

	private ExecutorService sourcers;

	@Override
	public void configure(Context context) {
		String[] topicNames = context.getString("topics", "").split(",");

		for (int index = 0; index < topicNames.length; index++) {
			topicNames[index] = topicNames[index].trim();
		}

		topics = StringUtils.join(topicNames, "|");
		LOGGER.info("topics: {}", topics);

		LOGGER.warn("topics's value is empty");

		threads = context.getInteger("threads", 1);
		LOGGER.info("threads: {}", threads);

		Preconditions.checkState(threads > 0, "threads's value must be greater than zero");

		batchUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_SIZE, KafkaSourceConstants.DEFAULT_BATCH_SIZE);
		LOGGER.info("batchUpperLimit: {}", batchUpperLimit);

		timeUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_DURATION_MS,
				KafkaSourceConstants.DEFAULT_BATCH_DURATION);
		LOGGER.info("timeUpperLimit: {}", timeUpperLimit);

		kafkaProps = KafkaSourceUtil.getKafkaProperties(context);
		LOGGER.info("kafkaProps: " + kafkaProps);
	}

	private class KafkaSourcer implements Runnable {

		private String sourcerName;

		private KafkaStream<byte[], byte[]> stream;

		public KafkaSourcer(KafkaStream<byte[], byte[]> stream) {
			this.stream = stream;
		}

		private void flush(List<Event> events) {
			if (CollectionUtils.isNotEmpty(events)) {
				try {
					getChannelProcessor().processEventBatch(events);

					events.clear();
				} catch (Exception e) {
					LOGGER.error(sourcerName + " flush error: " + ExceptionUtils.getFullStackTrace(e));
				}
			}
		}

		@Override
		public void run() {
			sourcerName = Thread.currentThread().getName();

			LOGGER.info(sourcerName + " starting...");

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

					events.add(EventBuilder.withBody(kafkaMessage, headers));

					if (events.size() >= batchUpperLimit || System.currentTimeMillis() > batchEndTime) {
						flush(events);

						batchEndTime = System.currentTimeMillis() + timeUpperLimit;
					}
				}

				flush(events);
			} catch (Throwable e) {
				if (e instanceof InterruptedException) {
					LOGGER.warn(sourcerName + " has been interrupted");
				} else {
					LOGGER.error(sourcerName + " consume error: " + ExceptionUtils.getFullStackTrace(e));
				}
			}

			LOGGER.info(sourcerName + " stoped");
		}

	}

	@Override
	public synchronized void start() {
		LOGGER.info(getName() + " starting...");

		consumerConnector = KafkaSourceUtil.getConsumer(kafkaProps);

		TopicFilter topicFilter = new Whitelist(topics);

		List<KafkaStream<byte[], byte[]>> streams = consumerConnector.createMessageStreamsByFilter(topicFilter,
				threads);

		sourcers = Executors.newFixedThreadPool(threads,
				new ThreadFactoryBuilder().setNameFormat("kafka-" + getName() + "-source-sourcer-%d").build());

		for (KafkaStream<byte[], byte[]> stream : streams) {
			sourcers.submit(new KafkaSourcer(stream));
		}

		super.start();

		LOGGER.info(getName() + " started");
	}

	@Override
	public synchronized void stop() {
		LOGGER.info(getName() + " stoping...");

		consumerConnector.shutdown();

		sourcers.shutdownNow();

		while (!sourcers.isTerminated()) {
			try {
				this.wait(1000);
			} catch (InterruptedException e) {
			}
		}

		super.stop();

		LOGGER.info(getName() + " stoped");
	}

}
