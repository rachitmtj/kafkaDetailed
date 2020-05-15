package com.rachit.examples.customSerializer;

import java.nio.ByteBuffer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class SupplierDeserializer implements Deserializer<Supplier> {
    private String encoding = "UTF8";

    public void configure(Map<String, ?> configs, boolean isKey) {
        //Nothing to configure
    }

    public Supplier deserialize(String topic, byte[] data) {

        try {
            if (data == null) {
                System.out.println("Null recieved at deserialize");
                return null;
            }

            ByteBuffer buf = ByteBuffer.wrap(data);
            int id = buf.getInt();

            int sizeOfName = buf.getInt();
            byte[] nameBytes = new byte[sizeOfName];
            buf.get(nameBytes);

            String deserializedName = new String(nameBytes, encoding);

            return new Supplier(id, deserializedName);

        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to Supplier");
        }
    }

    public void close() {
        // nothing to do
    }
}