package com.weibo.dip.flume.extension.sink;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yurun on 17/4/10.
 */
public class DipSinkCounter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DipSinkCounter.class);

    public static final String HDFS = "hdfs";
    public static final String KAFKA = "kafka";

    private static final String AMPERSAND = "&";

    private final Map<String, Long> counters = new HashMap<>();

    private class ESSinker implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    return;
                }

                sink();
            }
        }

    }

    private static final DipSinkCounter DIP_SINK_COUNTER = new DipSinkCounter();

    public static DipSinkCounter getInstance() {
        return DIP_SINK_COUNTER;
    }

    private DipSinkCounter() {
        Thread sinker = new Thread(new ESSinker());

        sinker.setDaemon(true);

        sinker.start();
    }

    public void add(String sink, String category, long value) {
        synchronized (counters) {
            String name = sink + AMPERSAND + category;

            if (!counters.containsKey(name)) {
                counters.put(name, 0L);
            }

            counters.put(name, counters.get(name) + value);
        }
    }

    public void sink() {
        Map<String, Long> cloneCounters = new HashMap<>();

        synchronized (counters) {
            for (Map.Entry<String, Long> entry : counters.entrySet()) {
                cloneCounters.put(entry.getKey(), entry.getValue());
            }

            counters.clear();
        }

        String username = "admin";
        String password = "esadmin";

        String host = "es.intra.dip.weibo.com";
        int port = 9200;

        int connectTimeout = 3000;
        int socketTimeout = 3000;

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClient restClient = RestClient.builder(new HttpHost(host, port))
            .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
            .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(connectTimeout).setSocketTimeout(socketTimeout))
            .build();

        try {
            SimpleDateFormat indexDateFormat = new SimpleDateFormat("yyyyMM");
            SimpleDateFormat utcDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

            utcDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

            for (Map.Entry<String, Long> entry : cloneCounters.entrySet()) {
                String sink = entry.getKey().split(AMPERSAND)[0];
                String category = entry.getKey().split(AMPERSAND)[1];
                Long value = entry.getValue();

                long timestamp = System.currentTimeMillis();

                String index = "/dip-monitoring-databus-" + indexDateFormat.format(new Date(timestamp)) + "/v1/";

                Map<String, Object> values = new HashMap<>();

                values.put("sink", sink);
                values.put("category", category);
                values.put("value", value);
                values.put("timestamp", utcDateFormat.format(timestamp));

                restClient.performRequest(
                    "POST",
                    index,
                    Collections.singletonMap("pretty", "true"),
                    new NStringEntity(GsonUtil.toJson(values), ContentType.APPLICATION_JSON));

                LOGGER.info("sink: " + sink + ", category: " + category + ", value: " + value);
            }
        } catch (Exception e) {
            LOGGER.warn("counters save to es error: " + ExceptionUtils.getFullStackTrace(e));
        } finally {
            try {
                restClient.close();
            } catch (IOException e) {
                LOGGER.warn("es rest client close error: " + ExceptionUtils.getFullStackTrace(e));
            }
        }
    }

}
