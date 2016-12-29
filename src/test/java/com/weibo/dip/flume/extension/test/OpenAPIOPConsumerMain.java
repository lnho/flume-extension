/**
 * 
 */
package com.weibo.dip.flume.extension.test;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
public class OpenAPIOPConsumerMain {

	private static class KafkaConsumer implements Runnable {

		private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
					String line = new String(messageAndMetadata.message());

					// long logTime =
					// Long.valueOf(line.substring(line.lastIndexOf("_") + 1));
					//
					// long now = System.currentTimeMillis();
					//
					// long delay = now - logTime;
					//
					// if (delay > 60000) {
					// System.out.println(Thread.currentThread().getName() +
					// "delay["
					// + dateFormat.format(new Date(now)) + "]: " + delay);
					// }
				}
			}
		}

	}

	public static void main(String[] args) {
		String zkConnect = "first.zookeeper.dip.weibo.com:2181,second.zookeeper.dip.weibo.com:2181,third.zookeeper.dip.weibo.com:2181/kafka/k1001";

		String topic = "app_dipsinacomkafka12345_wwwanalyzetest";

		int threads = 12;

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
				executor.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
			}
		}
	}

}
