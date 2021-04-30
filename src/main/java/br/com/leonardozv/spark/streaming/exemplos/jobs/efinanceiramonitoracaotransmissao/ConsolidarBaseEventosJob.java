package br.com.leonardozv.spark.streaming.exemplos.jobs.efinanceiramonitoracaotransmissao;

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
                .option("path", "D:\\s3\\efinanceira-monitoracao-transmissao\\bkt-staging-data")
                .load()
                .writeStream()
                .partitionBy("date")
                .format("parquet")
                .outputMode("append")
                .option("path","D:\\s3\\efinanceira-monitoracao-transmissao\\bkt-raw-data")
                .option("checkpointLocation", "D:\\s3\\efinanceira-monitoracao-transmissao\\bkt-checkpoint-data\\consolidar-base-eventos-job")
                .trigger(Trigger.Once())
                .start()
                .awaitTermination();

    }
}
