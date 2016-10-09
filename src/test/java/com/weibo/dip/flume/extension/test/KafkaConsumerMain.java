/**
 * 
 */
package com.weibo.dip.flume.extension.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * @author yurun
 *
 */
public class KafkaConsumerMain {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerMain.class);

	private static final AtomicLong COUNT = new AtomicLong(0);

	private static class KafkaConsumer implements Runnable {

		private KafkaStream<byte[], byte[]> stream;

		public KafkaConsumer(KafkaStream<byte[], byte[]> stream) {
			this.stream = stream;
		}

		@Override
		public void run() {
			ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

			while (iterator.hasNext()) {
				MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();

				if (messageAndMetadata != null) {
					COUNT.incrementAndGet();
				}
			}
		}

	}

	public static void main(String[] args) {
		Options scribeClientOptions = new Options();

		scribeClientOptions
				.addOption(Option.builder("zkConnect").hasArg().argName("zookeeper.connect").required().build());
		scribeClientOptions.addOption(Option.builder("topic").hasArg().argName("consumer topic").required().build());
		scribeClientOptions.addOption(
				Option.builder("threads").hasArg().argName("consumer thread number").required(false).build());

		HelpFormatter formatter = new HelpFormatter();

		if (ArrayUtils.isEmpty(args)) {
			formatter.printHelp("Kafka Consumer COMMAND", scribeClientOptions);

			return;
		}

		CommandLineParser parser = new DefaultParser();

		CommandLine commandLine = null;

		try {
			commandLine = parser.parse(scribeClientOptions, args);
		} catch (ParseException e) {
			System.out.println("Error: " + e.getMessage());

			formatter.printHelp("Kafka Consumer COMMAND", scribeClientOptions);

			return;
		}

		String zkConnect = commandLine.getOptionValue("zkConnect");

		LOGGER.info("zkConnect: " + zkConnect);

		String topic = commandLine.getOptionValue("topic");

		LOGGER.info("topic: " + topic);

		String groupId = commandLine.getOptionValue("groupId");

		LOGGER.info("groupId: " + groupId);

		int threads = 1;
		if (commandLine.hasOption("threads")) {
			threads = Integer.valueOf(commandLine.getOptionValue("threads"));
		}

		LOGGER.info("threads: " + threads);

		Properties properties = new Properties();

		properties.put("zookeeper.connect", zkConnect);
		properties.put("group.id", "Kafka_Consumer_" + System.currentTimeMillis());

		ConsumerConfig config = new ConsumerConfig(properties);

		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

		topicCountMap.put(topic, threads);

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		ExecutorService executor = Executors.newCachedThreadPool();

		for (KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new KafkaConsumer(stream));
		}

		executor.shutdown();

		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				consumer.shutdown();
			}

		});

		while (!executor.isTerminated()) {
			try {
				executor.awaitTermination(5, TimeUnit.SECONDS);

				LOGGER.info("kafka consume: " + COUNT.get());
			} catch (InterruptedException e) {
			}
		}
	}

}
