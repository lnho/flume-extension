/**
 * 
 */
package com.weibo.dip.flume.extension.channel;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flume.channel.BasicTransactionSemantics;

/**
 * @author yurun
 *
 */
public class PollingFileChannel extends MultithreadingFileChannel {

	private AtomicInteger polling = new AtomicInteger(0);

	@Override
	protected BasicTransactionSemantics createTransaction() {
		try {
			return getFileChannels().get(Math.abs(polling.getAndIncrement()) % getChannels()).createTransaction();
		} catch (IndexOutOfBoundsException e) {
			return null;
		}
	}

}
