package br.com.leonardozv.spark.streaming.exemplos.scala.eventdrivenledger.models

// The structure of the message to be passed to vertices
case class GenericMessage(
                    currentId: Long, // Tracks the most recent vertex appended to path and used for flagging isCyclic
                    level: Int, // The number of up-line supervisors (level in reporting heirarchy)
                    head: Long, // The top-most supervisor
                    path: List[Long], // The reporting path to the the top-most supervisor
                    isCyclic: Boolean, // Is the reporting structure of the employee cyclic
                    isLeaf: Boolean // Is the employee rank and file (no down-line reporting employee)
                  )