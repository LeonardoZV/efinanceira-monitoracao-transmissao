package br.com.leonardozv.spark.streaming.exemplos.scala.eventdrivenledger.models

case class MovementVertexValue(
                            debit: Double,
                            credit: Double,
                            movement: Double
                         )