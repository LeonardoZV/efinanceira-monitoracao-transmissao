package br.com.leonardozv.spark.streaming.exemplos.java;

import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.avro.functions.from_avro;

public class TesteJobJava {

    public static void executar(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
        .appName("TesteJob")
        .master("local[*]")
        .getOrCreate();

//        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
//
//        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
//
//        JavaRDD<Integer> distData = jsc.parallelize(data);

//        List<Tuple2<Object, Integer>> vertices = Arrays.asList(new Tuple2[]{
//                        new Tuple2(1L, 1),
//                        new Tuple2(2L, 1),
//                        new Tuple2(3L, 1)
//        });
//
//        JavaRDD<Tuple2<Object, Integer>> verticesRDD = spark.sparkContext().parallelize(JavaConverters.asScalaIteratorConverter(vertices.iterator()).asScala().toSeq(), 1, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class)).toJavaRDD();
//
//        List<Edge<Integer>> edges = Arrays.asList(new Edge[]{
//                        new Edge(1L, 2L, 1),
//                        new Edge(2L, 3L, 1)
//        });
//
//        JavaRDD<Edge<Integer>> edgesRDD = spark.sparkContext().parallelize(JavaConverters.asScalaIteratorConverter(edges.iterator()).asScala().toSeq(), 1, scala.reflect.ClassTag$.MODULE$.apply(Edge.class)).toJavaRDD();
//
//        Graph<Integer, Integer> followerGraph =
//                GraphImpl
//                        .apply(
//                                verticesRDD.rdd(),
//                                edgesRDD.rdd(),
//                                0,
//                                StorageLevel.MEMORY_AND_DISK(),
//                                StorageLevel.MEMORY_AND_DISK(),
//                                scala.reflect.ClassTag$.MODULE$.apply(Integer.class),
//                                scala.reflect.ClassTag$.MODULE$.apply(Integer.class)
//                        );

//        Map<String, String> props = new HashMap<>();
//
//        props.put("basic.auth.credentials.source", "USER_INFO");
//        props.put("basic.auth.user.info", "2BEQE2KDNBJGDH2Y:8nixndjUyjXqTJoXnm3X3GwLZPz5F8umq74/g9ioG2mIi4lm0CWF1nUAf8deIFbP");
//
//        RestService restService = new RestService("https://psrc-4j1d2.westus2.azure.confluent.cloud");
//
//        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(restService, 100, props);
//
//        SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata("transmissao-efetuada-value");
//
//        SparkSession spark = SparkSession.builder()
//                .appName("CapturarEventosJob")
//                .master("local[*]")
//                .getOrCreate();
//
//        spark.sparkContext().addFile("s3://aws-glue-scripts-428204489288-us-east-2/dependencies/kafka_confluence.py");
//
//        String path = SparkFiles.get("kafka_confluence.py");
//
//        System.out.println("path: " + path);
//
//        spark.readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "pkc-epwny.eastus.azure.confluent.cloud:9092")
//                .option("kafka.security.protocol", "SSL")
//                .option("kafka.keystore.location", "kafka_confluence.py")
//                .option("kafka.keystore.password", "abc")
//                .option("kafka.truststore.location", "kafka_confluence.py")
//                .option("kafka.truststore.location", "abc")
//                .option("kafka.group.id", "efinanceira-monitoracao-transmissao")
//                .option("subscribe", "transmissao-efetuada")
//                .option("startingOffsets", "earliest")
//                .option("includeHeaders", "true")
//                .load()
//                .writeStream()
//				.format("console")
//         		.outputMode("update")
//				.option("truncate", false)
//                .trigger(Trigger.Once())
//                .start()
//                .awaitTermination();

    }

}