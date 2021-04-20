package br.com.leonardozv.spark.streaming.exemplos.jobs;

import static org.apache.spark.sql.avro.functions.*;
import static org.apache.spark.sql.functions.*;

import br.com.leonardozv.spark.streaming.exemplos.udfs.ConveterHeadersParaMap;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import java.util.HashMap;
import java.util.Map;

public class CapturarEventosJob {

    public static void executar(String[] args) throws Exception {

        Map<String, String> props = new HashMap<>();

        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", "2BEQE2KDNBJGDH2Y:8nixndjUyjXqTJoXnm3X3GwLZPz5F8umq74/g9ioG2mIi4lm0CWF1nUAf8deIFbP");

        RestService restService = new RestService("https://psrc-4j1d2.westus2.azure.confluent.cloud");

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(restService, 100, props);

        SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata("processamento-ted-value");

        SparkSession spark = SparkSession.builder()
                .appName("CapturarEventosJob")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        spark.udf().registerJava("converterHeadersParaMap", ConveterHeadersParaMap.class.toString(), DataTypes.createMapType(DataTypes.StringType, DataTypes.BinaryType));

        Dataset<Row> rawData = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "pkc-epwny.eastus.azure.confluent.cloud:9092")
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule   required username='BIMCMFF6WU3YBB34'   password='Xnr9geulvxPYeyNeL2r56iyjNG5dwkB2CTnQz+syVZwOUfJIQFxmSJT0+MskxOnQ';")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.group.id", "efinanceira-monitoracao-transmissao")
                .option("subscribe", "processamento-ted")
                .option("startingOffsets", "earliest")
                .option("includeHeaders", "true")
                .load();

        Dataset<Row> parsedData = rawData
                .withColumn("headers", expr("converterHeadersParaMap(headers)"))
                .select(col("topic"),
                        col("partition"),
                        col("offset"),
                        col("headers").getItem("specversion").cast("string").as("specversion"),
                        col("headers").getItem("type").cast("string").as("type"),
                        col("headers").getItem("source").cast("string").as("source"),
                        col("headers").getItem("id").cast("string").as("id"),
                        col("headers").getItem("time").cast("string").cast("timestamp").as("time"),
                        col("headers").getItem("messageversion").cast("string").as("messageversion"),
                        col("headers").getItem("eventversion").cast("string").as("eventversion"),
                        col("headers").getItem("transactionid").cast("string").as("transactionid"),
                        col("headers").getItem("correlationid").cast("string").as("correlationid"),
                        col("headers").getItem("datacontenttype").cast("string").as("datacontenttype"),
                        from_avro(expr("substring(value, 6)"), schemaMetadata.getSchema()).as("payload"))
                .withColumn("date", to_date(col("time")))
                .withWatermark("time", "2 minutes")
                .dropDuplicates("id");

        parsedData.printSchema();

        StreamingQuery query = parsedData
                .writeStream()
//				.format("console")
//       		.outputMode("update")
//				.option("truncate", false)
                .partitionBy("date")
                .format("parquet")
                .outputMode("append")
                .option("path","D:\\s3\\bkt-raw-data\\data")
                .option("checkpointLocation", "D:\\s3\\bkt-raw-data\\checkpoint")
                .trigger(Trigger.Once())
                .start();

        query.awaitTermination();

    }

}