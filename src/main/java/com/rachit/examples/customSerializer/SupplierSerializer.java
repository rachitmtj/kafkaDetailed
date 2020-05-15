package com.rachit.examples.customSerializer;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Map;
import java.nio.ByteBuffer;

public class SupplierSerializer implements Serializer<Supplier> {

    private String encoding = "UTF8";

    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public byte[] serialize(String topis, Supplier data) {

        int sizeOfName;
        byte[] serializedName;

        try {
            if (data == null)
                return null;

            serializedName = data.getSupplierName().getBytes(encoding);
            sizeOfName = serializedName.length;

            ByteBuffer buf = ByteBuffer.allocate(4 + 4 + sizeOfName);
            buf.putInt(data.getSupplierId());
            buf.putInt(sizeOfName);
            buf.put(serializedName);

            return buf.array();

        } catch (Exception e) {
            throw new SerializationException("Error when serializing Supplier to byte[]");
        }

    }

    public void close() {

    }
}
