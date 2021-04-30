package br.com.leonardozv.spark.streaming.exemplos.jobs.efinanceiramonitoracaotransmissao;

import static org.apache.spark.sql.avro.functions.*;
import static org.apache.spark.sql.functions.*;

import br.com.leonardozv.spark.streaming.exemplos.services.EventoEFinanceiraService;
import br.com.leonardozv.spark.streaming.exemplos.udfs.ConverterHeadersParaMap;
import br.com.leonardozv.spark.streaming.exemplos.udfs.ObterCnpjEmpresaDeclaranteEFinanceira;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.catalyst.expressions.Uuid;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class GerarRelatorioTransmissaoJob {

    public static void executar(String[] args) throws Exception {

		Map<String, String> props = new HashMap<>();

		props.put("basic.auth.credentials.source", "USER_INFO");
		props.put("basic.auth.user.info", "2BEQE2KDNBJGDH2Y:8nixndjUyjXqTJoXnm3X3GwLZPz5F8umq74/g9ioG2mIi4lm0CWF1nUAf8deIFbP");

		RestService restService = new RestService("https://psrc-4j1d2.westus2.azure.confluent.cloud");

		SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(restService, 100, props);

		SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata("relatorio-transmissao-value");

		byte[] magicByte = new byte[] { 0x0 };

		byte[] idBytes = ByteBuffer.allocate(4).putInt(schemaMetadata.getId()).array();

        SparkSession spark = SparkSession.builder()
                .appName("GerarRelatorioTransmissaoJob")
                .master("local[*]")
                .getOrCreate();

		SQLContext sqlContext = new SQLContext(spark.sparkContext());

		spark.sparkContext().setLogLevel("WARN");

		spark.udf().registerJava("obterCnpjEmpresaDeclaranteEFinanceira", ObterCnpjEmpresaDeclaranteEFinanceira.class.getName(), DataTypes.LongType);

		Dataset<Row> cadastroEmpresas = spark
				.read()
				.format("csv")
				.option("sep", ";")
				.load("D:\\s3\\efinanceira-monitoracao-transmissao\\cadastro_empresas.csv")
				.selectExpr("_c0 as numero_cnpj_empresa", "_c1 as nome_empresa_declarante");

        Dataset<Row> stagingData = spark
				.readStream()
				.format("parquet")
				.schema(spark.read().parquet("D:\\s3\\efinanceira-monitoracao-transmissao\\bkt-staging-data").schema())
				.option("path", "D:\\s3\\efinanceira-monitoracao-transmissao\\bkt-staging-data")
				.load()
				.withColumn("numero_cnpj_empresa_declarante", expr("obterCnpjEmpresaDeclaranteEFinanceira(payload.data.codigo_evento_efinanceira)"));

		stagingData.createOrReplaceTempView("evento");

		sqlContext.sql("SELECT numero_cnpj_empresa_declarante, COUNT(*) as quantidade_eventos_transmitidos, COUNT(case when payload.data.codigo_retorno_transmissao = 1 then 1 else null end) as quantidade_eventos_transmitidos_sucesso, COUNT(case when payload.data.codigo_retorno_transmissao = 2 then 1 else null end) as quantidade_eventos_transmitidos_erro FROM evento GROUP BY numero_cnpj_empresa_declarante")
				.join(cadastroEmpresas, col("numero_cnpj_empresa_declarante").equalTo(col("numero_cnpj_empresa")), "inner").drop("numero_cnpj_empresa")
				.withColumn("data",	struct("*"))
				.withColumn("value", concat(lit(magicByte), lit(idBytes), to_avro(struct("data"), schemaMetadata.getSchema())))
				.withColumn("headers ",
						array(
								struct(lit("specversion").as("key"), lit("1").cast("binary").as("value")),
								struct(lit("type").as("key"), lit("").cast("binary").as("value")),
								struct(lit("source").as("key"), lit("urn:sigla:gerar-relatorio-transmissao-job").cast("binary").as("value")),
								struct(lit("id").as("key"), expr("uuid()").cast("binary").as("value")),
								struct(lit("time").as("key"), date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast("binary").as("value")),
								struct(lit("messageversion").as("key"), lit("1").cast("binary").as("value")),
								struct(lit("transactionid").as("key"), lit("").cast("binary").as("value")),
								struct(lit("correlationid").as("key"), lit("").cast("binary").as("value")),
								struct(lit("datacontenttype").as("key"), lit("application/avro").cast("binary").as("value"))
						)
				)
				.writeStream()
//				.format("console")
//				.outputMode("update")
//				.option("truncate", false)
				.format("kafka")
				.outputMode("update")
				.option("kafka.bootstrap.servers", "pkc-epwny.eastus.azure.confluent.cloud:9092")
				.option("kafka.security.protocol", "SASL_SSL")
				.option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule   required username='BIMCMFF6WU3YBB34'   password='Xnr9geulvxPYeyNeL2r56iyjNG5dwkB2CTnQz+syVZwOUfJIQFxmSJT0+MskxOnQ';")
				.option("kafka.sasl.mechanism", "PLAIN")
				.option("topic", "relatorio-transmissao")
				.option("includeHeaders", "true")
				.option("checkpointLocation", "D:\\s3\\efinanceira-monitoracao-transmissao\\bkt-checkpoint-data\\gerar-relatorio-transmissao-job")
				.trigger(Trigger.Once())
				.start()
				.awaitTermination();

    }

}