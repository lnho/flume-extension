/**
 * 
 */
package com.weibo.dip.flume.extension.test.hdfs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yurun
 *
 */
public class WriteHDFSMain {

	private static final Logger LOGGER = LoggerFactory.getLogger(WriteHDFSMain.class);

	private static class Writer extends Thread {

		private boolean stop = false;

		public void setStop(boolean stop) {
			this.stop = stop;
		}

		@Override
		public void run() {
			try {
				LOGGER.info("Writer starting...");

				Configuration conf = new Configuration();

				FileSystem fs = FileSystem.get(conf);

				BufferedWriter writer = new BufferedWriter(
						new OutputStreamWriter(fs.create(new Path("/tmp/yurun/data"))));

				String line = "_accesskey=sinaedgeahsolci14ydn&_ip=221.182.130.57&_port=80&_an=221.182.130.57&_data=d7.sina.com.cn 111.58.151.80 0 TCP_IMS_HIT [29/Aug/2016:00:59:42 +0800] \"GET /pfpghc2/201608/10/c2a4db4ca881411f86477f2febf4e201.jpg HTTP/0.0\" 304 0 \"http://news.sina.cn/sh?vt=4&pos=108\" \"-\" \"-\" \"Mozilla/5.0 (Linux; U; Android 4.3; zh-CN; HUAWEI C8816D Build/HuaweiC8816D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 UCBrowser/10.9.10.788 U3/0.8.0 Mobile Safari/534.30\" *Not IP address [0]*";

				while (!stop) {
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
					}
				}

				writer.write(line);

				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
				}

				writer.write(line);

				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
				}

				writer.write(line);

				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
				}

				writer.close();

				LOGGER.info("Writer stoped");
			} catch (Exception e) {
				LOGGER.error("Writer run error: " + ExceptionUtils.getFullStackTrace(e));
			}
		}

	}

	public static void main(String[] args) throws IOException {
		Writer writer = new Writer();

		writer.start();

		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				LOGGER.info("shutdown starting...");

				writer.setStop(true);

				try {
					Thread.sleep(10 * 1000);
				} catch (InterruptedException e) {
				}

				LOGGER.info("shutdown stoped");
			}

		});
	}

}
