package br.com.efinanceira.monitoracao.transmissao.jobs;

import static org.apache.spark.sql.avro.functions.*;
import static org.apache.spark.sql.functions.*;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.catalyst.expressions.Uuid;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class GerarRelatorioTransmissaoJob {

    public static void executar(String[] args) throws Exception {

		Map<String, String> props = new HashMap<>();

		props.put("basic.auth.credentials.source", "USER_INFO");
		props.put("schema.registry.basic.auth.user.info", "2BEQE2KDNBJGDH2Y:8nixndjUyjXqTJoXnm3X3GwLZPz5F8umq74/g9ioG2mIi4lm0CWF1nUAf8deIFbP");

		RestService restService = new RestService("https://psrc-4j1d2.westus2.azure.confluent.cloud");

		SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(restService, 100, props);

		SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata("relatorio-transmissao-value");

		byte[] magicByte = new byte[] { 0x0 };

		byte[] idBytes = ByteBuffer.allocate(4).putInt(schemaMetadata.getId()).array();

        SparkSession spark = SparkSession.builder()
                .appName("GerarRelatorioTransmissaoJob")
                .master("local[*]")
                .getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> rawData = spark
				.readStream()
				.format("parquet")
				.schema(spark.read().parquet("D:\\hadoop\\bkt-raw-data\\data").schema())
				.option("path", "D:\\hadoop\\bkt-raw-data\\data")
				.load();

		rawData.createOrReplaceTempView("evento");

		Dataset<Row> aggregatedData = rawData
				.sqlContext().sql("SELECT payload.data.codigo_produto_operacional, COUNT(*) as quantidade_eventos_transmitidos, COUNT(case when payload.data.codigo_empresa = 341 then 1 else null end) as quantidade_eventos_transmitidos_sucesso, COUNT(case when payload.data.codigo_empresa = 350 then 1 else null end) as quantidade_eventos_transmitidos_erro FROM evento GROUP BY payload.data.codigo_produto_operacional")
				.withColumn("data",	struct("*"))
				.withColumn("value", concat(lit(magicByte), lit(idBytes), to_avro(struct("data"), schemaMetadata.getSchema())))
				.withColumn("headers",
						array(
								struct(lit("specversion").as("key"), lit("1").cast("binary").as("value")),
								struct(lit("type").as("key"), lit("").cast("binary").as("value")),
								struct(lit("source").as("key"), lit("urn:sigla:efinanceira-monitoracao-transmissao-spark").cast("binary").as("value")),
								struct(lit("id").as("key"), expr("uuid()").cast("binary").as("value")),
								struct(lit("time").as("key"), date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast("binary").as("value")),
								struct(lit("messageversion").as("key"), lit("1").cast("binary").as("value")),
								struct(lit("transactionid").as("key"), lit("").cast("binary").as("value")),
								struct(lit("correlationid").as("key"), lit("").cast("binary").as("value")),
								struct(lit("datacontenttype").as("key"), lit("application/avro").cast("binary").as("value"))
						)
				);

		aggregatedData.printSchema();

		StreamingQuery query = aggregatedData
				.writeStream()
				.format("console")
				.outputMode("update")
				.option("truncate", false)
//				.format("kafka")
//				.outputMode("update")
//				.option("kafka.bootstrap.servers", "pkc-epwny.eastus.azure.confluent.cloud:9092")
//				.option("kafka.security.protocol", "SASL_SSL")
//				.option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule   required username='BIMCMFF6WU3YBB34'   password='Xnr9geulvxPYeyNeL2r56iyjNG5dwkB2CTnQz+syVZwOUfJIQFxmSJT0+MskxOnQ';")
//				.option("kafka.sasl.mechanism", "PLAIN")
//				.option("topic", "relatorio-transmissao")
//				.option("includeHeaders", "true")
				.option("checkpointLocation", "D:\\hadoop\\bkt-agg-data\\checkpoint")
//				.trigger(Trigger.Once())
				.start();

        query.awaitTermination();

    }

}
