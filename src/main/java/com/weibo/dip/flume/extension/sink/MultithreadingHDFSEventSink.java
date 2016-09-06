/**
 * 
 */
package com.weibo.dip.flume.extension.sink;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yurun
 *
 */
public class MultithreadingHDFSEventSink extends AbstractSink implements Configurable {

	private static final Logger LOGGER = LoggerFactory.getLogger(MultithreadingHDFSEventSink.class);

	@Override
	public void configure(Context context) {

	}

	@Override
	public synchronized void start() {
		super.start();
	}

	@Override
	public Status process() throws EventDeliveryException {
		return null;
	}

	@Override
	public synchronized void stop() {
		super.stop();
	}

}
