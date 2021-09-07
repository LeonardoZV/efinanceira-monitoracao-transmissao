package br.com.leonardozv.spark.streaming.exemplos.scala.eventdrivenledger.models

case class MovementMessage(
                            debit: Double,
                            credit: Double,
                            movement: Double
                         )