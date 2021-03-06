/**
 * 
 */
package com.weibo.dip.flume.extension.channel;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.channel.file.FileChannelConfiguration;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * @author yurun
 *
 */
public class MultithreadingFileChannel extends BasicChannelSemantics {

	private static final Logger LOGGER = LoggerFactory.getLogger(MultithreadingFileChannel.class);

	private Context context;

	private int channels;

	private String checkpointDirStr;

	private String dataDirStr;

	private List<PublicTransactionFileChannel> fileChannels = null;

	public Context getContext() {
		return context;
	}

	public void setContext(Context context) {
		this.context = context;
	}

	public int getChannels() {
		return channels;
	}

	public void setChannels(int channels) {
		this.channels = channels;
	}

	public String getCheckpointDirStr() {
		return checkpointDirStr;
	}

	public void setCheckpointDirStr(String checkpointDirStr) {
		this.checkpointDirStr = checkpointDirStr;
	}

	public String getDataDirStr() {
		return dataDirStr;
	}

	public void setDataDirStr(String dataDirStr) {
		this.dataDirStr = dataDirStr;
	}

	public List<PublicTransactionFileChannel> getFileChannels() {
		return fileChannels;
	}

	public void setFileChannels(List<PublicTransactionFileChannel> fileChannels) {
		this.fileChannels = fileChannels;
	}

	@Override
	public void configure(Context context) {
		this.context = context;

		channels = context.getInteger("channels");
		LOGGER.info("channels: {}", channels);

		Preconditions.checkState(channels > 0, "channels's value must be greater than zero");

		checkpointDirStr = context.getString("checkpointDir");
		LOGGER.info("checkpointDir: {}", checkpointDirStr);

		Preconditions.checkState(StringUtils.isNotEmpty(checkpointDirStr),
				"checkpointDirStr's value must not be empty");

		dataDirStr = context.getString("dataDir");
		LOGGER.info("dataDir: {}", dataDirStr);

		Preconditions.checkState(StringUtils.isNotEmpty(dataDirStr), "dataDirStr's value must not be empty");

		LOGGER.info("MultipleFileChannel configure success");
	}

	@Override
	public synchronized void start() {
		File checkpointDir = new File(checkpointDirStr);

		if (!checkpointDir.exists()) {
			checkpointDir.mkdirs();
		}

		File dataDir = new File(dataDirStr);

		if (!dataDir.exists()) {
			dataDir.mkdirs();
		}

		fileChannels = new ArrayList<>();

		for (int index = 0; index < channels; index++) {
			PublicTransactionFileChannel fileChannel = new PublicTransactionFileChannel();

			fileChannel.setName(getName() + "_filechannel_" + index);

			Context ctx = new Context(context.getParameters());

			ctx.put(FileChannelConfiguration.CHECKPOINT_DIR,
					new File(checkpointDir, String.valueOf(index)).getAbsolutePath());
			ctx.put(FileChannelConfiguration.DATA_DIRS, new File(dataDir, String.valueOf(index)).getAbsolutePath());

			fileChannel.configure(ctx);

			fileChannels.add(fileChannel);
		}

		for (FileChannel fileChannel : fileChannels) {
			fileChannel.start();
		}

		super.start();
	}

	@Override
	protected BasicTransactionSemantics createTransaction() {
		try {
			return fileChannels.get((int) (System.currentTimeMillis() % channels)).createTransaction();
		} catch (IndexOutOfBoundsException e) {
			return null;
		}
	}

	@Override
	public synchronized void stop() {
		if (getLifecycleState() == LifecycleState.STOP) {
			return;
		}

		for (FileChannel fileChannel : fileChannels) {
			fileChannel.stop();
		}

		fileChannels.clear();

		super.stop();
	}

}
