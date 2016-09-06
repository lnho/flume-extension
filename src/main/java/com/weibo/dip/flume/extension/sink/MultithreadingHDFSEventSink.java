/**
 * 
 */
package com.weibo.dip.flume.extension.sink;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author yurun
 *
 */
public class MultithreadingHDFSEventSink extends AbstractSink implements Configurable {

	// private static final Logger LOGGER =
	// LoggerFactory.getLogger(MultithreadingHDFSEventSink.class);

	private int threads;

	private ExecutorService sinkers;

	private boolean sinkStoped;

	private int batchSize;

	private BufferedWriter writer;

	private class Sinker implements Runnable {

		@Override
		public void run() {
			while (!sinkStoped) {
				Channel channel = getChannel();

				Transaction transaction = channel.getTransaction();

				transaction.begin();

				try {
					int txnEventCount = 0;

					for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
						Event event = channel.take();
						if (event == null) {
							break;
						}

						synchronized (writer) {
							writer.write(new String(event.getBody()));
						}
					}

					transaction.commit();
				} catch (Exception e) {
					transaction.rollback();
				} finally {
					transaction.close();
				}
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
			writer = new BufferedWriter(new OutputStreamWriter(
					FileSystem.get(new Configuration()).create(new Path("/tmp/yurun/flume.data"))));
		} catch (Exception e) {

		}

		sinkStoped = false;

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
		sinkStoped = true;

		sinkers.shutdown();

		while (!sinkers.isTerminated()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}

		try {
			writer.close();
		} catch (IOException e) {
		}

		super.stop();
	}

}
