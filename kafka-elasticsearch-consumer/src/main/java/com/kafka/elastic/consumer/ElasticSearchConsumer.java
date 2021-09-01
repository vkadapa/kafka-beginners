package com.kafka.elastic.consumer;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient client = createClient();
        IndexRequest indexRequest = null;
        IndexResponse indexResponse = null;
        String id = null;
        KafkaConsumer<String, String> consumer = kafkaConsumer();
        String indexId;

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            logger.info("Received {} records from Kafka", consumerRecords.count());
            for(ConsumerRecord consumerRecord: consumerRecords) {
                logger.info("Partition {} - Offset {} - Key {} - Value {}", consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                indexId = consumerRecord.topic() + "_" + consumerRecord.partition() + "_" + consumerRecord.offset(); //to make idempotent
                //Use BulkRequest for Batch processing
                indexRequest = new IndexRequest("twitter", "tweets", indexId).source(consumerRecord.value(), XContentType.JSON);
                indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                id = indexResponse.getId();
                logger.info("id: {}", id);
                Thread.sleep(10);
            }
            logger.info("Committing Offsets");
            consumer.commitSync();
        }


        //client.close();
    }

    private static RestHighLevelClient createClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

    }

    private static KafkaConsumer<String, String> kafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "twitter-tweets-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList("twitter-tweets"));

        return consumer;
    }
}
