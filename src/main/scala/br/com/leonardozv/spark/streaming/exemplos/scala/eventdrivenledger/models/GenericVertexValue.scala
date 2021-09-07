package br.com.leonardozv.spark.streaming.exemplos.scala.eventdrivenledger.models

// The structure of the vertex values of the graph
case class GenericVertexValue(
                        id: Long, // The employee name
                        currentId: Long, // Initial value is the employeeId
                        level: Int, // Initial value is zero
                        head: Long, // Initial value is this employee's name
                        path: List[Long], // Initial value contains this employee's name only
                        isCyclic: Boolean, // Initial value is false
                        isLeaf: Boolean // Initial value is true
                      )



