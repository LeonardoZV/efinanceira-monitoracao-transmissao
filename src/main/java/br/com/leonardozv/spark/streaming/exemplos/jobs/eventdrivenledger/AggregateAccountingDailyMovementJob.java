package br.com.leonardozv.spark.streaming.exemplos.jobs.eventdrivenledger;

import br.com.leonardozv.spark.streaming.exemplos.udfs.ConverterHeadersParaMap;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.*;

public class AggregateAccountingDailyMovementJob {

    public static void executar(String[] args) throws Exception {

        Map<String, String> props = new HashMap<>();

        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", "2BEQE2KDNBJGDH2Y:8nixndjUyjXqTJoXnm3X3GwLZPz5F8umq74/g9ioG2mIi4lm0CWF1nUAf8deIFbP");

        RestService restService = new RestService("https://psrc-4j1d2.westus2.azure.confluent.cloud");

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(restService, 100, props);

        SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata("accounting-journal-entry-created-value");

        SparkSession spark = SparkSession.builder()
                .appName("AggregateAccountingDailyMovementJob")
                .master("local[*]")
                .config("spark.cleaner.referenceTracking.cleanCheckpoints", true)
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        spark.udf().registerJava("converterHeadersParaMap", ConverterHeadersParaMap.class.getName(), DataTypes.createMapType(DataTypes.StringType, DataTypes.BinaryType));

        Dataset<Row> eventos = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "pkc-epwny.eastus.azure.confluent.cloud:9092")
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='BIMCMFF6WU3YBB34' password='Xnr9geulvxPYeyNeL2r56iyjNG5dwkB2CTnQz+syVZwOUfJIQFxmSJT0+MskxOnQ';")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.group.id", "event-driven-ledger-moviment")
                .option("subscribe", "accounting-journal-entry-created")
                .option("startingOffsets", "earliest")
                .option("includeHeaders", "true")
                .load()
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
                .withColumn("accounting_timestamp", date_trunc("day", to_timestamp(col("payload.data.accounting_date_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")))
                .withWatermark("accounting_timestamp", "3 months")
                .dropDuplicates("id", "accounting_timestamp");

        eventos.createOrReplaceTempView("eventos");

        eventos.sqlContext()
                .sql("SELECT to_date(accounting_timestamp) as accouting_date, payload.data.company_id, payload.data.account_id, SUM(payload.data.amount) as amount FROM eventos GROUP BY accounting_timestamp, payload.data.company_id, payload.data.account_id")
                .writeStream()
				.format("console")
       		    .outputMode("update")
				.option("truncate", false)
                .option("checkpointLocation", "D:\\s3\\event-driven-ledger\\bkt-checkpoint-data\\capture-accounting-journal-entry-events-job")
//                .trigger(Trigger.Once())
                .start()
                .awaitTermination();

    }

}