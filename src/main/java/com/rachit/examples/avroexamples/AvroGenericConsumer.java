package com.rachit.examples.avroexamples;

import java.util.*;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;

public class AvroGenericConsumer {

    public static void main(String[] args) throws Exception{

        String topicName = "GenericAvroRecord";

        String groupName = "genericConsumerGroup";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupName);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
        consumer.subscribe(Arrays.asList(topicName));

        try{
            while (true){
                ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
                for (ConsumerRecord<String, GenericRecord> record : records){
                    System.out.println(record.value().get("name"));
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
