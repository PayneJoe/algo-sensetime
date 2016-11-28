/**
  * Created by yuanpingzhou on 11/17/16.
  *
  * Avro IO utility :
  * convert libsvm format into specified(schema) avro format
  */
package com.sensetime.ad.algo

import scala.collection.mutable

object AvroUtility {
  import org.apache.avro.generic.{GenericRecord,GenericDatumReader,GenericDatumWriter,GenericData}
  import org.apache.avro.file.DataFileReader
  import org.apache.avro.file.DataFileWriter
  import org.apache.avro.Schema
  import scala.io.Source
  import java.io._
  //import scala.collection.mutable.ArrayBuffer
  import collection.mutable._
  import collection.JavaConverters._

  def main(args: Array[String]) {

    if(args.length < 3){
      println("param : SchemaFile SourceFile[text file] OutputFile[avro file]")
      System.exit(1)
    }

    // schema file
    val schemaFile = scala.io.Source.fromFile(args(0)).mkString
    // parse schema file
    val schema_obj =  new Schema.Parser
    val recordSchema = schema_obj.parse(schemaFile)
    // schema writer
    val schemaWriter = new GenericDatumWriter[GenericRecord](recordSchema)
    // avro writer
    val avroFile = new File(args(2))
    val avroWriter = new DataFileWriter[GenericRecord](schemaWriter)
    avroWriter.create(recordSchema,avroFile)

    // TODO
    val record = new GenericData.Record(recordSchema)

    val featureSchema = recordSchema.getField("features").schema()
    println(featureSchema)
    var feat = new GenericData.Array(0,featureSchema)
    println()

    System.exit(1)

    // read from text file
    val textFile = Source.fromFile(args(1))
    textFile.getLines.foreach{
      line =>
        val parts = line.trim.split(" ")
        val label = parts(0).toInt
       // var features = ArrayBuffer[Map[String,Any]]()
        val features = ArrayBuffer[java.util.Map[String,Any]]().asJava

        parts.slice(1,parts.length).foreach {
          field => {
            val kv = field.split(":")
            val feature = HashMap("name" -> kv(0),"term" -> "","value" -> kv(1).toDouble).asJava
            features.add(feature)
          }
        }
        val record = new GenericData.Record(recordSchema)
        println(record)
        println(features)
        record.put("label",label.toInt)
        record.put("features",features)
        avroWriter.append(record)
    }
    avroWriter.close()
    textFile.close()

  }
}
