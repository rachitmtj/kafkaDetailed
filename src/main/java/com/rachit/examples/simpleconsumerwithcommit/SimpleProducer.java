package com.rachit.examples.simpleconsumerwithcommit;

import java.util.*;
import org.apache.kafka.clients.producer.*;
public class SimpleProducer {

    public static void main(String[] args) throws Exception{

        String topicName = "SimpleProducerTopic";
        String key = "Key";
        String value = "Value";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer <String, String>(props);

        for(int i=0;i<=100;i++){

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,key+i,value+i);
            producer.send(record);
        }

        producer.close();

        System.out.println("SimpleProducer Completed.");
    }
}