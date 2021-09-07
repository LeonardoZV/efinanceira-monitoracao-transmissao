package br.com.leonardozv.spark.streaming.exemplos.java.eventdrivenledger.jobs;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;

public class OptimizeLedgerPostingsJob {

    public static void executar(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("OptimizeLedgerPostingsJob")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        spark.readStream()
                .format("parquet")
                .schema(spark.read().parquet("D:\\s3\\event-driven-ledger\\bkt-staging-data\\ledger-posting-created-events").schema())
                .load("D:\\s3\\event-driven-ledger\\bkt-staging-data\\ledger-posting-created-events")
                .writeStream()
                .partitionBy("date")
                .format("parquet")
                .outputMode("append")
                .option("checkpointLocation", "D:\\s3\\event-driven-ledger\\bkt-checkpoint-data\\OptimizeLedgerPostingsJob")
                .trigger(Trigger.Once())
                .start("D:\\s3\\event-driven-ledger\\bkt-raw-data\\ledger-posting-created-events")
                .awaitTermination();

    }
}
