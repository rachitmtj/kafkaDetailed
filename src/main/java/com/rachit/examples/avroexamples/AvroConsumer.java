package com.rachit.examples.avroexamples;

import java.util.*;
import org.apache.kafka.clients.consumer.*;


public class AvroConsumer{

    public static void main(String[] args) throws Exception{

        String topicName = "AvroClicks1";

        String groupName = "RG";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupName);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");

        KafkaConsumer<String, ClickRecord> consumer = new KafkaConsumer<String, ClickRecord>(props);
        consumer.subscribe(Arrays.asList(topicName));
        try{
            while (true){
                ConsumerRecords<String, ClickRecord> records = consumer.poll(100);
                for (ConsumerRecord<String, ClickRecord> record : records){
                    System.out.println("Session id="+ record.value().getSessionId()
                            + " Channel=" + record.value().getChannel()
                            + " browser=" + record.value().getBrowser());
                }
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
        finally{
            consumer.close();
        }
    }

}