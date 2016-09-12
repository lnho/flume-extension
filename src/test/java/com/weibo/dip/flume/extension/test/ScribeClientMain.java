/**
 * 
 */
package com.weibo.dip.flume.extension.test;

import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.Context;
import org.apache.flume.event.EventBuilder;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weibo.dip.flume.extension.sink.scribe.EventToLogEntrySerializer;
import com.weibo.dip.flume.extension.sink.scribe.FlumeEventSerializer;
import com.weibo.dip.flume.extension.sink.scribe.Scribe;
import com.weibo.dip.flume.extension.sink.scribe.ScribeSinkConfigurationConstants;

/**
 * @author yurun
 *
 */
public class ScribeClientMain {

	private static final Logger LOGGER = LoggerFactory.getLogger(ScribeClientMain.class);

	public static void main(String[] args) {
		Options scribeClientOptions = new Options();

		scribeClientOptions
				.addOption(Option.builder("host").hasArg().argName("scribe(flume) server host").required().build());
		scribeClientOptions
				.addOption(Option.builder("port").hasArg().argName("scribe(flume) server port").required().build());
		scribeClientOptions.addOption(Option.builder("category").hasArg().argName("category name").required().build());
		scribeClientOptions.addOption(Option.builder("lines").hasArg().argName("line number").required(false).build());
		scribeClientOptions.addOption(Option.builder("help").hasArg(false).required(false).build());

		HelpFormatter formatter = new HelpFormatter();

		if (ArrayUtils.isEmpty(args)) {
			formatter.printHelp("Scribe Client COMMAND", scribeClientOptions);

			return;
		}

		CommandLineParser parser = new DefaultParser();

		CommandLine commandLine = null;

		try {
			commandLine = parser.parse(scribeClientOptions, args);
		} catch (ParseException e) {
			System.out.println("Error: " + e.getMessage());

			formatter.printHelp("Scribe Client COMMAND", scribeClientOptions);

			return;
		}

		String host = commandLine.getOptionValue("host");

		int port = Integer.valueOf(commandLine.getOptionValue("port"));

		String category = commandLine.getOptionValue("category");

		long lines = Long.MAX_VALUE;
		if (commandLine.hasOption("lines")) {
			lines = Long.valueOf(commandLine.getOptionValue("lines"));
		}

		TTransport transport = null;

		Scribe.Client client = null;

		try {
			transport = new TFramedTransport(new TSocket(new Socket(host, port)));

			client = new Scribe.Client(new TBinaryProtocol(transport, false, false));

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");

			FlumeEventSerializer serializer = new EventToLogEntrySerializer();

			Map<String, String> parameters = new HashMap<>();

			parameters.put(ScribeSinkConfigurationConstants.CONFIG_SCRIBE_CATEGORY_HEADER, "category");

			serializer.configure(new Context(parameters));

			long count = 0;

			while (count <= lines) {
				String line = sdf.format(new Date(System.currentTimeMillis())) + "_" + UUID.randomUUID().toString();

				Map<String, String> headers = new HashMap<>();

				headers.put("category", category);

				client.Log(Arrays.asList(
						serializer.serialize(EventBuilder.withBody(line.getBytes(CharEncoding.UTF_8), headers))));

				count++;
			}
		} catch (Exception e) {
			LOGGER.error("ScribeClient log error: " + ExceptionUtils.getFullStackTrace(e));
		} finally {
			client = null;

			if (transport != null) {
				transport.close();
			}
		}
	}

}
