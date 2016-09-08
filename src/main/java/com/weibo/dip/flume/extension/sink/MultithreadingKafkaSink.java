/**
 * 
 */
package com.weibo.dip.flume.extension.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.kafka.KafkaSink;
import org.apache.flume.sink.kafka.KafkaSinkConstants;
import org.apache.flume.sink.kafka.KafkaSinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author yurun
 *
 */
public class MultithreadingKafkaSink extends AbstractSink implements Configurable {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);

	private Properties kafkaProps;

	private String topicHeaderName;

	private int consumers;

	private long sinkSleep;

	private ExecutorService sinkers;

	private boolean sinkStoped = false;

	private int batchSize;

	@Override
	public void configure(Context context) {
		topicHeaderName = context.getString("topicHeaderName");
		LOGGER.info("topicHeaderName: {}", topicHeaderName);

		Preconditions.checkState(StringUtils.isNotEmpty(topicHeaderName), "topicHeaderName's value must not be empty");

		consumers = context.getInteger("consumers");
		LOGGER.info("consumers: {}", consumers);

		Preconditions.checkState(consumers > 0, "consumers's value must be greater than zero");

		batchSize = context.getInteger(KafkaSinkConstants.BATCH_SIZE, KafkaSinkConstants.DEFAULT_BATCH_SIZE);
		LOGGER.info("batchSize: {}" + batchSize);

		Preconditions.checkState(batchSize > 0, "batchSize's value must be greater than zero");

		sinkSleep = context.getLong("sinkSleep", 1000L);
		LOGGER.info("sinkSleep: {}", sinkSleep);

		Preconditions.checkState(sinkSleep > 0, "batchSleep's value must be greater than zero");

		kafkaProps = KafkaSinkUtil.getKafkaProperties(context);
		LOGGER.info("Kafka producer properties: {}", kafkaProps);
	}

	public class Sinker implements Runnable {

		private List<KeyedMessage<String, byte[]>> messageList = new ArrayList<>();

		@Override
		public void run() {
			String sinkName = Thread.currentThread().getName();

			LOGGER.info(sinkName + " started");

			Producer<String, byte[]> producer = null;

			try {
				ProducerConfig config = new ProducerConfig(kafkaProps);

				producer = new Producer<String, byte[]>(config);

				Channel channel = getChannel();

				Transaction transaction = null;

				Event event = null;

				Map<String, String> headers = null;

				String eventTopic = null;

				while (!sinkStoped) {
					try {
						transaction = channel.getTransaction();

						transaction.begin();

						messageList.clear();

						for (int processedEvents = 0; processedEvents < batchSize; processedEvents++) {
							event = channel.take();
							if (event == null) {
								break;
							}

							headers = event.getHeaders();

							eventTopic = headers.get(topicHeaderName);

							Preconditions.checkState(StringUtils.isNotEmpty(eventTopic),
									"eventTopic is emtpy, topicHeaderName(" + topicHeaderName
											+ ") may be wrong, check");

							byte[] eventBody = event.getBody();

							// create a message and add to buffer
							KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(eventTopic, eventBody);

							messageList.add(data);
						}

						// send batch
						if (CollectionUtils.isNotEmpty(messageList)) {
							producer.send(messageList);
						}

						transaction.commit();

						if (CollectionUtils.isNotEmpty(messageList)) {
							try {
								Thread.sleep(sinkSleep);
							} catch (InterruptedException e) {
							}
						}
					} catch (Exception e) {
						LOGGER.error(sinkName + " failed to send events: {}", ExceptionUtils.getFullStackTrace(e));

						transaction.rollback();
					} finally {
						transaction.close();
					}
				}
			} catch (Exception e) {
				LOGGER.error(sinkName + " sink error: " + ExceptionUtils.getFullStackTrace(e));
			} finally {
				if (producer != null) {
					producer.close();
				}
			}

			LOGGER.info(sinkName + " stoped");
		}

	}

	@Override
	public synchronized void start() {
		LOGGER.info(getName() + " starting...");

		sinkers = Executors.newFixedThreadPool(consumers,
				new ThreadFactoryBuilder().setNameFormat("kafka-" + getName() + "-sink-sinker-%d").build());

		for (int index = 0; index < consumers; index++) {
			sinkers.submit(new Sinker());
		}

		super.start();

		LOGGER.info(getName() + " started");
	}

	@Override
	public Status process() throws EventDeliveryException {
		return Status.BACKOFF;
	}

	@Override
	public synchronized void stop() {
		LOGGER.info(getName() + " starting...");

		sinkStoped = true;

		sinkers.shutdown();

		while (!sinkers.isTerminated()) {
			try {
				this.wait(1000);
			} catch (InterruptedException e) {
			}
		}

		super.stop();

		LOGGER.info(getName() + " stoped");
	}

}
