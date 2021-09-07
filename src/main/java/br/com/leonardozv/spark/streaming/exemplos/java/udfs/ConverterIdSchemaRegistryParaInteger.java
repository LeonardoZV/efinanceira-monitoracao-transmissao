package br.com.leonardozv.spark.streaming.exemplos.java.udfs;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.spark.sql.api.java.UDF1;

import java.nio.ByteBuffer;

public class ConverterIdSchemaRegistryParaInteger implements UDF1<byte[], Integer> {

    private static final long serialVersionUID = 1L;

    @Override
    public Integer call(byte[] bytes) {

        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        if (buffer.get() != 0x0) {
            throw new SerializationException("Unknown magic byte!");
        }

        return buffer.getInt();

    }

}
