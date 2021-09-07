package br.com.leonardozv.spark.streaming.exemplos.java.efinanceira.jobs;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;

public class ConsolidarBaseEventosJob {

    public static void executar(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("ConsolidarBaseEventosJob")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        spark.readStream()
                .format("parquet")
                .schema(spark.read().parquet("D:\\s3\\efinanceira-monitoracao-transmissao\\bkt-staging-data").schema())
                .load("D:\\s3\\efinanceira-monitoracao-transmissao\\bkt-staging-data")
                .writeStream()
                .partitionBy("date")
                .format("parquet")
                .outputMode("append")
                .option("checkpointLocation", "D:\\s3\\efinanceira-monitoracao-transmissao\\bkt-checkpoint-data\\consolidar-base-eventos-job")
                .trigger(Trigger.Once())
                .start("D:\\s3\\efinanceira-monitoracao-transmissao\\bkt-raw-data")
                .awaitTermination();

    }
}
