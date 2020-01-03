package Spark_XML
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import scala.io.Source
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jdbc._
import com.mysql.cj.jdbc.Driver
import com.databricks.spark.xml._

object Spark_XML {
  
  def main(args:Array[String]):Unit={
    
    val conf=new SparkConf().setAppName("Spark_XML1").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark=SparkSession
              .builder()
              .config(conf)
              .getOrCreate()
              
    import spark.implicits._
    
    val data=sc.textFile("file:///E://Workouts//Data//transactions.xml")
    data.foreach(println)
    
    println("Transaction File")
    val transactions_data=spark.read.format("com.databricks.spark.xml")
                   .option("rowTag","Transaction")
                   .load("file:///E://Workouts//Data//transactions.xml")
     transactions_data.show(false)
     transactions_data.printSchema()
     println
     println("Transaction File got processed")
     val Trans_processed=transactions_data.withColumn("naresh",explode($"RetailTransaction.LineItem"))
                       .selectExpr("naresh.sale.Description as Line_Description")
     Trans_processed.show()
    
    println("Book File")
    val book_data=spark.read.format("com.databricks.spark.xml")
                  .option("rowTag","catalog")
                  .load("file:///E://Workouts//Data//book.xml")
      book_data.show() 
      book_data.printSchema()
    val book_processed=book_data.withColumn("naresh",explode($"book"))
                        .selectExpr("naresh._id as Book_ID","naresh.title as Title","naresh.author as Author")
     book_processed.show(false)                   
    
    
  }
  
}