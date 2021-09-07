package br.com.leonardozv.spark.streaming.exemplos.java.multivisao.jobs;

import br.com.leonardozv.spark.streaming.exemplos.java.udfs.ConverterHeadersParaMap;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.*;

public class RegrasEventoTransferenciaRealizadaJob {

    public static void executar(String[] args) throws Exception {

        Map<String, String> props = new HashMap<>();

        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", "2BEQE2KDNBJGDH2Y:8nixndjUyjXqTJoXnm3X3GwLZPz5F8umq74/g9ioG2mIi4lm0CWF1nUAf8deIFbP");

        RestService restService = new RestService("https://psrc-4j1d2.westus2.azure.confluent.cloud");

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(restService, 100, props);

        SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata("transferencia-realizada-value");

        SparkSession spark = SparkSession.builder()
                .appName("RegrasEventoTransferenciaRealizada")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        spark.udf().registerJava("converterHeadersParaMap", ConverterHeadersParaMap.class.getName(), DataTypes.createMapType(DataTypes.StringType, DataTypes.BinaryType));

        spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "pkc-epwny.eastus.azure.confluent.cloud:9092")
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='BIMCMFF6WU3YBB34' password='Xnr9geulvxPYeyNeL2r56iyjNG5dwkB2CTnQz+syVZwOUfJIQFxmSJT0+MskxOnQ';")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.group.id", "multivisao")
                .option("subscribe", "transferencia-realizada")
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
                .withColumn("date", to_date(col("time")))
                .withWatermark("time", "5 minutes")
                .dropDuplicates("id")
                .writeStream()
                .foreachBatch((evento, batchId) -> {

                    evento.persist();

                    evento.write().format("parquet").mode(SaveMode.Append).save("D:\\s3\\multivisao\\bkt-staging-data\\rt-evento-transferencia-realizada");
                    evento.createOrReplaceTempView("evento");

                    Dataset<Row> contabil = evento.sqlContext().sql("SELECT uuid() as id, id as codigo_evento_produto, CAST('00001' AS STRING) AS codigo_regra_transformacao, CAST('TED001' AS STRING) AS codigo_roteiro_contabil, CAST(NULL AS STRING) AS numero_conta_contabil_interna_debito, CAST(NULL AS STRING) AS numero_conta_contabil_interna_credito, payload.data.codigo_empresa AS codigo_empresa, payload.data.valor AS valor_lancamento FROM evento WHERE payload.data.codigo_produto_operacional = 100 AND payload.data.valor > 0");
                    contabil.write().format("parquet").mode(SaveMode.Append).save("D:\\s3\\multivisao\\bkt-staging-data\\contabil");
                    contabil.createOrReplaceTempView("contabil");
                    contabil.show();

                    Dataset<Row> fiscal = evento.sqlContext().sql("SELECT uuid() as id, id as codigo_evento_produto, CAST('00002' AS STRING) AS codigo_regra_transformacao, CAST('01' AS STRING) AS codigo_imposto, numero_conta_contabil_interna_debito AS numero_conta_contabil_interna_debito, codigo_empresa AS codigo_empresa, valor_lancamento AS valor_recolhido FROM contabil WHERE codigo_regra_transformacao = '00001'");
                    fiscal.write().format("parquet").mode(SaveMode.Append).save("D:\\s3\\multivisao\\bkt-staging-data\\fiscal");
                    fiscal.createOrReplaceTempView("fiscal");
                    fiscal.show();

                    evento.unpersist();

                })
//                .option("checkpointLocation", "D:\\s3\\multivisao\\bkt-checkpoint-data\\rt-evento-transferencia-realizada")
                .trigger(Trigger.Once())
                .start()
                .awaitTermination();

    }

}