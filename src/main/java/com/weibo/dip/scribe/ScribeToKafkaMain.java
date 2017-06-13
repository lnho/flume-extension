/**
 * 
 */
package com.weibo.dip.scribe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.source.scribe.LogEntry;
import org.apache.flume.source.scribe.ResultCode;
import org.apache.flume.source.scribe.Scribe;
import org.apache.flume.source.scribe.Scribe.Iface;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yurun
 *
 */
public class ScribeToKafkaMain {

	private static final Logger LOGGER = LoggerFactory.getLogger(ScribeToKafkaMain.class);

	private static class Monitor extends Thread {

		private Map<String, AtomicLong> counters = new HashMap<>();

		public void add(String category, long count) {
			if (!counters.containsKey(category)) {
				synchronized (counters) {
					counters.put(category, new AtomicLong(0));
				}
			}

			counters.get(category).incrementAndGet();
		}

		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(10 * 1000);
				} catch (InterruptedException e) {
				}

				synchronized (counters) {
					for (Entry<String, AtomicLong> entry : counters.entrySet()) {
						LOGGER.info("Received [" + entry.getKey() + ", " + entry.getValue().getAndSet(0L) + "]");
					}
				}
			}
		}

	}

	private static class Receiver implements Iface {

		private Producer<String, String> producer;

		private Monitor monitor;

		public Receiver(Producer<String, String> producer, Monitor monitor) {
			this.producer = producer;

			this.monitor = monitor;
		}

		public ResultCode Log(List<LogEntry> list) throws TException {
			if (CollectionUtils.isEmpty(list)) {
				return ResultCode.OK;
			}

			try {
				for (LogEntry entry : list) {
					String category = entry.getCategory();

					producer.send(new ProducerRecord<>(category, entry.getMessage()));

					monitor.add(category, 1L);
				}
				return ResultCode.OK;
			} catch (Exception e) {
				LOGGER.error("scribe receiver send to kafka error: " + ExceptionUtils.getFullStackTrace(e));

				return ResultCode.TRY_LATER;
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Monitor monitor = new Monitor();

		monitor.setDaemon(true);

		monitor.start();

		String kafkaServers = "first.kafka.dip.weibo.com:9092,second.kafka.dip.weibo.com:9092,third.kafka.dip.weibo.com:9092,fourth.kafka.dip.weibo.com:9092,fifth.kafka.dip.weibo.com:9092";

		Map<String, Object> kafkaServerConfig = new HashMap<>();

		kafkaServerConfig.put("bootstrap.servers", kafkaServers);

		kafkaServerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		kafkaServerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(kafkaServerConfig);

		int scribeServerPort = 1467;
		int scribeServerWorkers = 1;
		int scribeServerMaxReadBufferBytes = 134217728;

		Scribe.Processor<Receiver> processor = new Scribe.Processor<Receiver>(new Receiver(producer, monitor));
		TNonblockingServerTransport transport = new TNonblockingServerSocket(scribeServerPort);
		THsHaServer.Args serverArgs = new THsHaServer.Args(transport);

		serverArgs.workerThreads(scribeServerWorkers);
		serverArgs.processor(processor);
		serverArgs.transportFactory(new TFramedTransport.Factory(scribeServerMaxReadBufferBytes));
		serverArgs.protocolFactory(new TBinaryProtocol.Factory(false, false));
		serverArgs.maxReadBufferBytes = scribeServerMaxReadBufferBytes;

		TServer server = new THsHaServer(serverArgs);

		server.serve();

		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				LOGGER.info("scribe to kafka stoping ...");

				server.stop();

				producer.close();

				LOGGER.info("scrbe to kafka stoped");
			}

		});
	}

}
