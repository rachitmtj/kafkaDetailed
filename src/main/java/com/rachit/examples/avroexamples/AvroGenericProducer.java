package com.rachit.examples.avroexamples;

import java.io.*;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.*;

public class AvroGenericProducer {

    public static void main(String[] args) throws Exception{

        String topicName = "GenericAvroRecord";
        String msg;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8081");

        Producer<String, GenericRecord> producer = new KafkaProducer <String, GenericRecord>(props);

        GenericRecord record = new AvroGenericProducer().buildRecord();

        producer.send(new ProducerRecord<String,GenericRecord>(topicName,"rec1",record)).get();

        System.out.println("Complete");

    }

    public GenericRecord buildRecord() throws IOException {

        String schemaString = null;

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("genericPerson.avsc");

        try {
            schemaString = IOUtils.toString(inputStream);
        } finally {
            inputStream.close();
        }

        Schema schema = new Schema.Parser().parse(schemaString);

        GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
        customerBuilder.set("name","rachit");
        customerBuilder.set("gender","male");
        customerBuilder.set("surname","mutreja");

        GenericData.Record myCustomer = customerBuilder.build();

        return myCustomer;
    }
}
