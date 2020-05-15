package com.rachit.examples.customSerializer;


import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class SupplierProducer {

    public static void main(String args[]) throws Exception{

        /*Properties props = new Properties();

        //Compression type
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        //Linger up to 100 ms before sending batch if size not met
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        //Batch up to 64K buffer sizes.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,  16384 * 4);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");*/

        String topicName = "SupplierTopic";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.rachit.examples.customeSerializer.SupplierSerializer");

        Producer<String, Supplier> producer = new KafkaProducer<String, Supplier>(props);

        Supplier sp1 = new Supplier(111, "Xyz Pvt Ltd.");
        Supplier sp2 = new Supplier(112, "Abc Pvt Ltd.");
        Supplier sp3 = new Supplier(115, "Abc Pvt Ltd.");

        //send with asynchronous callback
        producer.send(new ProducerRecord<String, Supplier>(topicName, "SUP", sp1),new MyProducerCallback());
        //fire and forget
        producer.send(new ProducerRecord<String, Supplier>(topicName, "SUP", sp2));
        //synchronous send
        RecordMetadata metadata = producer.send(new ProducerRecord<String, Supplier>(topicName, "SUP", sp3)).get();

        System.out.println(metadata.toString());

        System.out.println("SupplierProducer Completed.");
        producer.close();

    }

    static class MyProducerCallback implements Callback {

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null)
                System.out.println("AsynchronousProducer failed with an exception");
            else
                System.out.println("AsynchronousProducer call Success:");
        }

    }
}
