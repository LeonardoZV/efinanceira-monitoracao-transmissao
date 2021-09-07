package br.com.leonardozv.spark.streaming.exemplos.scala.eventdrivenledger.jobs

import br.com.leonardozv.spark.streaming.exemplos.scala.eventdrivenledger.models.{GenericMessage, GenericVertexValue, MovementMessage, MovementVertexValue}
import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ProjectMonthlyLedgerBalanceJob {

  def executar(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("ProjectMonthlyLedgerBalanceJob")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("WARN")

    val accountsDF = spark
      .read
      .format("csv")
      .option("sep", ";")
      .load("D:\\s3\\event-driven-ledger\\accounts.csv")
      .selectExpr("_c0 as account_id", "_c1 as account_name", "_c2 as parent_account_id");

    val movementsDF = spark
      .read
      .format("csv")
      .option("sep", ";")
      .load("D:\\s3\\event-driven-ledger\\movements.csv")
      .selectExpr("_c0 as account_id", "_c1 as debit", "_c2 as credit", "_c3 as movement");

    val balancesDF = spark
      .read
      .format("csv")
      .option("sep", ";")
      .load("D:\\s3\\event-driven-ledger\\balances.csv")
      .selectExpr("_c0 as account_id", "_c1 as balance");

    val accountHierarchyVertices: RDD[(VertexId, Long)] = accountsDF
      .rdd.map(r => (r.getAs("account_id").toString.toLong, r.getAs("account_id").toString.toLong))

    val accountHierarchyEdges: RDD[Edge[Int]] = accountsDF
      .filter("parent_account_id is not null").rdd.map(r => Edge(r.getAs("parent_account_id").toString.toLong, r.getAs("account_id").toString.toLong, 1))

    val accountHierarchyGraph: Graph[Long, Int] = Graph(accountHierarchyVertices, accountHierarchyEdges)

    val initializedAcountHierarchyGraph: Graph[GenericVertexValue, Int] = accountHierarchyGraph.mapVertices { (id, v) =>
      GenericVertexValue(
        id = id,
        currentId = id,
        level = 0,
        head = id,
        path = List(id),
        isCyclic = false,
        isLeaf = false
      )
    }

    val accountHierarchyGraphInitialMsg = GenericMessage(
      currentId = 0L,
      level = 0,
      head = 0,
      path = Nil,
      isCyclic = false,
      isLeaf = true
    )

    // Step 1: Mutate the value of the vertices, based on the message received
    def vprog(vertexId: VertexId, value: GenericVertexValue, message: GenericMessage): GenericVertexValue = {
      if (message.level == 0) { //superstep 0 - initialize
        value.copy(level = value.level + 1)
      } else if (message.isCyclic) { // set isCyclic
        value.copy(isCyclic = true)
      } else if (!message.isLeaf) { // set isleaf
        value.copy(isLeaf = false)
      } else { // set new values
        value.copy(
          currentId = message.currentId,
          level = value.level + 1,
          head = message.head,
          path = value.id :: message.path
        )
      }
    }

    // Step 2: For all triplets that received a message -- meaning, any of the two vertices received a message from the previous step -- then compose and send a message.
    def sendMsg(triplet: EdgeTriplet[GenericVertexValue, Int]): Iterator[(VertexId, GenericMessage)] = {
      val src = triplet.srcAttr
      val dst = triplet.dstAttr
      // Handle cyclic reporting structure
      if (src.currentId == triplet.dstId || src.currentId == dst.currentId) {
        if (!src.isCyclic) { // Set isCyclic
          Iterator((triplet.dstId, GenericMessage(
            currentId = src.currentId,
            level = src.level,
            head = src.head,
            path = src.path,
            isCyclic = true,
            isLeaf = src.isLeaf
          )))
        } else { // Already marked as isCyclic (possibly, from previous superstep) so ignore
          Iterator.empty
        }
      } else { // Regular reporting structure
        if (src.isLeaf) { // Initially every vertex is leaf. Since this is a source then it should NOT be a leaf, update
          Iterator((triplet.srcId, GenericMessage(
            currentId = src.currentId,
            level = src.level,
            head = src.head,
            path = src.path,
            isCyclic = false,
            isLeaf = false // This is the only important value here
          )))
        } else { // Set new values by propagating source values to destination
          //Iterator.empty
          Iterator((triplet.dstId, GenericMessage(
            currentId = src.currentId,
            level = src.level,
            head = src.head,
            path = src.path,
            isCyclic = false, // Set to false so that cyclic updating is ignored in vprog
            isLeaf = true // Set to true so that leaf updating is ignored in vprog
          )))
        }
      }
    }

    // Step 3: Merge all inbound messages to a vertex. No special merging needed for this use case.
    def mergeMsg(message1: GenericMessage, message2: GenericMessage): GenericMessage = message2

    val accountHierarchyResults = initializedAcountHierarchyGraph.pregel(accountHierarchyGraphInitialMsg, Int.MaxValue, EdgeDirection.Out)(
      vprog,
      sendMsg,
      mergeMsg
    )

    val accountHierarchyRDD = accountHierarchyResults
      .vertices.map { case (id, v) => (id, v.id, v.level, v.head, v.path.reverse.mkString(">"), v.isCyclic, v.isLeaf) }

    val accountHierarchyDF = spark.createDataFrame(accountHierarchyRDD)
      .selectExpr("_1 as account_id", "_3 as level", "_4 as root_id", "_5 as path", "_6 as isCyclic", "_7 as isLeaf")

    val movimentTotalizationVertices: RDD[(VertexId, MovementVertexValue)] = movementsDF
      .rdd.map(r => (r.getAs("account_id").toString.toLong, MovementVertexValue(r.getAs("debit").toString.toDouble, r.getAs("credit").toString.toDouble, r.getAs("movement").toString.toDouble)))

    val movimentTotalizationEdges: RDD[Edge[Int]] = accountsDF
      .filter("parent_account_id is not null").rdd.map(r => Edge(r.getAs("account_id").toString.toLong, r.getAs("parent_account_id").toString.toLong, 1))

    val movimentTotalizationGraph: Graph[MovementVertexValue, Int] = Graph(movimentTotalizationVertices, movimentTotalizationEdges)

    val initializedMovimentTotalizationGraph: Graph[MovementVertexValue, Int] = movimentTotalizationGraph.mapVertices { (id, v) =>
      MovementVertexValue(
        debit = if(v == null) 0 else v.debit,
        credit =  if(v == null) 0 else v.credit,
        movement =  if(v == null) 0 else v.movement
      )
    }

    val movimentTotalizationinitialMsg = MovementMessage(
      debit = 0.0,
      credit = 0.0,
      movement = 0.0
    )

    val movimentTotalizationResults = initializedMovimentTotalizationGraph.pregel(movimentTotalizationinitialMsg, Int.MaxValue, EdgeDirection.Out)(
        (_, value, message) => MovementVertexValue(value.debit + message.debit, value.credit + message.credit, value.movement + message.movement),
        t => Iterator((t.dstId, MovementMessage(t.srcAttr.debit, t.srcAttr.credit, t.srcAttr.movement))),
        (message1, message2) => MovementMessage(message1.debit + message2.debit, message1.credit + message2.credit, message1.movement + message2.movement)
    )

    val movimentTotalizationRDD = movimentTotalizationResults
      .vertices.map { case (id, v) => (id, v.debit, v.credit, v.movement) }

    val movimentTotalizationDF = spark.createDataFrame(movimentTotalizationRDD)
      .selectExpr("_1 as account_id", "_2 as debit", "_3 as credit", "_4 as movement")

    val finalDF = accountsDF
      .join(accountHierarchyDF, Seq("account_id"), "left")
      .join(movimentTotalizationDF, Seq("account_id"), "left")
      .join(balancesDF, Seq("account_id"), "left")
      .select(col("account_id"),
              col("account_name"),
              col("parent_account_id"),
              when(col("level").isNull, 1).otherwise(col("level")).as("level"),
              when(col("root_id").isNull, col("account_id")).otherwise(col("root_id")).as("root_id"),
              when(col("path").isNull, col("account_id")).otherwise(col("path")).as("path"),
              when(col("isCyclic").isNull, false).otherwise(col("isCyclic")).as("isCyclic"),
              when(col("isLeaf").isNull, false).otherwise(col("isLeaf")).as("isLeaf"),
              when(col("balance").isNull, 0).otherwise(col("balance")).as("initial_balance"),
              when(col("debit").isNull, 0).otherwise(col("debit")).as("debit"),
              when(col("credit").isNull, 0).otherwise(col("credit")).as("credit"),
              when(col("movement").isNull, 0).otherwise(col("movement")).as("movement"))
      .withColumn("final_balance", col("initial_balance") + col("movement"))
      .orderBy(col("path"))
      .show(false)
//      .write
//      .mode(SaveMode.Overwrite)
//      .save("D:\\s3\\event-driven-ledger\\bkt-reports-data\\trial-balance-sheet")

  }

}
