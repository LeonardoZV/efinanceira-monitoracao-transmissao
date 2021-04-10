package org.apache.spark.sql.avro

import org.apache.spark.sql.Column

object customfunctions {

  def from_avro_schema_registry(data: Column): Column = {
    new Column(SchemaRegistryAvroDataToCatalyst(data.expr, Map.empty))
  }

}