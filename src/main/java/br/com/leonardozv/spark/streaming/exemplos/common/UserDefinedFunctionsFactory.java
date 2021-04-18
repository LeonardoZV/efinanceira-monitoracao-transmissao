package br.com.leonardozv.spark.streaming.exemplos.common;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class UserDefinedFunctionsFactory {

	public static UDF1<byte[], Integer> converterIdSchemaRegistryParaInteger() {

		return new UDF1<>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(byte[] bytes) {

				ByteBuffer buffer = ByteBuffer.wrap(bytes);

				if (buffer.get() != 0x0) {
					throw new SerializationException("Unknown magic byte!");
				}

				return buffer.getInt();

			}

		};

	}

	public static UDF1<WrappedArray<Row>, Map<String, byte[]>> converterHeadersParaMap() {

		return new UDF1<>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Map<String, byte[]> call(WrappedArray<Row> wArray) {

				Map<String, byte[]> map = new HashMap<>();

				JavaConverters.seqAsJavaList(wArray.toSeq()).forEach(row -> map.put((String)row.get(0), (byte[])row.get(1)) );

				return map;

			}

		};

	}

}
