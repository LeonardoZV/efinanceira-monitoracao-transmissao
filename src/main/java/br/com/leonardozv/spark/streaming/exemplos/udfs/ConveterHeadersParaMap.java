package br.com.leonardozv.spark.streaming.exemplos.udfs;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.util.HashMap;
import java.util.Map;

public class ConveterHeadersParaMap implements UDF1<WrappedArray<Row>, Map<String, byte[]>> {

    private static final long serialVersionUID = 1L;

    @Override
    public Map<String, byte[]> call(WrappedArray<Row> wArray) {

        Map<String, byte[]> map = new HashMap<>();

        JavaConverters.seqAsJavaList(wArray.toSeq()).forEach(row -> map.put((String)row.get(0), (byte[])row.get(1)) );

        return map;

    }

}