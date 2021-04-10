package br.com.efinanceira.monitoracao.transmissao.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

public class GerarRelatorioTransmissaoJob {

    public static void executar(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("GerarRelatorioTransmissaoJob")
                .master("local[*]")
                .config("spark.executor.instances", 6)
                .config("spark.executor.cores", 1)
                .getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> data = spark
				.readStream()
				.format("parquet")
				.schema(spark.read().parquet("D:\\hadoop\\bkt-raw-data\\data").schema())
				.option("path", "D:\\hadoop\\bkt-raw-data\\data")
				.load();

		data.createOrReplaceTempView("evento");

		StreamingQuery query = data
				.sqlContext().sql("SELECT payload.data.codigo_produto_operacional, COUNT(*) as quantidade_eventos_transmitidos, COUNT(case when payload.data.codigo_empresa = 341 then 1 else null end) as quantidade_eventos_transmitidos_sucesso, COUNT(case when payload.data.codigo_empresa = 350 then 1 else null end) as quantidade_eventos_transmitidos_erro FROM evento GROUP BY payload.data.codigo_produto_operacional")
				.writeStream()
				.format("console")
				.outputMode("update")
				.option("checkpointLocation", "D:\\hadoop\\bkt-agg-data\\checkpoint")
				.start();

        query.awaitTermination();

    }

}
