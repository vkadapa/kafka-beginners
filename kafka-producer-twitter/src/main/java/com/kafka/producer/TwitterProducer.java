package com.kafka.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    String consumerKey = "consumer-key";
    String consumerSecret = "consumer-secret";
    String token = "token";
    String secret = "secret";

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        BasicClient twitterClient = createTwitterClient(msgQueue);
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();
        twitterClient.connect();
        String msg;
        // on a different thread, or multiple different threads....
        while (!twitterClient.isDone()) {
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if(msg != null) {
                    logger.info(msg);
                    kafkaProducer.send(new ProducerRecord<>("twitter-tweets", null, msg), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e != null)
                                logger.error("Exception while producing", e);
                        }
                    });
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            twitterClient.stop();
            kafkaProducer.close();
            logger.info("Closed twitter client and Kafka");
        }));
        logger.info("End of Client");

    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //high throuput
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));


        return new KafkaProducer<>(properties);
    }

    private BasicClient createTwitterClient(BlockingQueue<String> msgQueue ) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> kafka = Lists.newArrayList("bitcoin", "usa", "cricket");
        hosebirdEndpoint.trackTerms(kafka);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
