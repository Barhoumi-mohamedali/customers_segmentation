package DataEra.com


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{ DataFrame, Column }
import org.apache.spark.sql.types.{ StructType, StructField }
import scala.util.Try
import org.apache.spark.sql.{ functions => f }
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import scala.math

import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql._
object Ksegmentation3 {


  val sparkConf = new SparkConf()
    .setAppName("KSegmentation")
    .setMaster("local[*]")
    .setExecutorEnv("KSegmentation", "Xmx1gm") 
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("es.nodes","localhost,master2.hadoop.com,worker1.hadoop.com")
    .set("es.port","9201")
   // .set("es.input.json", "true")
   
   // .set("es.nodes.wan.only", "true")
    ///.set("es.read.metadata", "true")
    .set("es.index.auto.create", "true")
    .set("es.batch.size.bytes","100mb")
    .set("es.batch.size.entries", "1000")
    .set("es.batch.write.retry.wait","10s")
    .set("es.batch.write.retry.count","6")
    .set("es.batch.write.retry.limit","50") //HTTP bulk retries 
    .set("es.batch.write.retry.policy","simple")//HTTP bulk retries 
    .set("es.write.operation","index")
   // .set("es.mapping.id","id")
    .set("spark.cores.max", "2")


  val sc = new SparkContext(sparkConf)

  // load file and remove header
  val spark: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
   // .config("spark.es.nodes","master1.hadoop.com")
  //  .config("spark.es.port","9201")
   // .config("spark.es.nodes.wan.only", "true")
   // .config("spark.es.index.auto.create", "true")
    .getOrCreate()


  spark.sparkContext.setLogLevel("WARN")
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import spark.implicits._



  case class CC (

                  VMailMessage: Integer,
                 DayMins: Double,
                 EveMins: Double,
                 NightMins: Double,
                 IntlMins: Double,
                 CustServCalls: Double,
                 DayCalls: Double,
                 DayCharge: Double,
                 EveCalls: Double,
                 EveCharge: Double,
                 NightCalls: Double,
                 NightCharge: Double,
                 IntlCalls: Double,
                 IntlCharge: Double,
                 AreaCode :Double,
                 Phone :Double,
                 AccountLength :Double,
                 Churn:Double,
                 IntlPlan :Double,
                 State:String,
                  FirstDate:String,
                  LastDate: String

                )


  def main(args: Array[String]): Unit = {

  println("Hello Dali")


    Logger.getLogger("org").setLevel(Level.ERROR)
/*
    def convertStringToDateTime(time: String): Option[DateTime] = {
      val pattern = """^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*$""".r
      time match {
        case pattern(n) => Some(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(n))
        case _ => None
      }
    }

    def emptyString( s:String): Option[String]= {
      s match {
        case "" => None
        case s => Some(s)
      }
    }

    val dateTimeFormat = DateTimeFormat.forPattern("dd/MM/YY")

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    */
    val data = sc.textFile("src/main/ressources/callsdatawithDate (2).csv").toDS()


    val header = data.first()

    val rows = data.filter(l => l != header)
    // define case class


    // comma separator split


    val allSplit = rows.map( x => x.split(",").map(_.trim))

    // map parts to case class

    val allData = allSplit.map(p => CC(

      p(0).toInt , p(1).toDouble, p(2).toDouble, p(3).toDouble, p(4).toDouble,
      p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble, p(9).toDouble,
      p(10).toDouble, p(11).toDouble, p(12).toDouble, p(13).toDouble,
      p(14).toDouble, p(15).toDouble, p(16).toDouble, p(17).toDouble,
      p(18).toDouble, p(19).toString, p(20).toString,p(21).toString
    ))

    // convert rdd to dataframe

    val allDF = allData.toDF()

    println("All the Data")

    allDF.show(10)


    // convert back to rdd and cache the data



    val rowsRDD = allDF.rdd.map(r => (r.getInt(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5), r.getDouble(6),
      r.getDouble(7), r.getDouble(8), r.getDouble(9), r.getDouble(10), r.getDouble(11), r.getDouble(12), r.getDouble(13), r.getDouble(14), r.getDouble(15),
      r.getDouble(16), r.getDouble(17), r.getDouble(18), r.getString(19), r.getString(20), r.getString(21)))


    rowsRDD.cache()


    // convert data to RDD which will be passed to KMeans and cache the data. We are passing in Day charge, eve charge, churn  to KMeans. These are the attributes we want to use to assign the instance to a cluster

   // val vectors = allDF.rdd.map(r => Vectors.dense( r.getDouble(7), r.getDouble(8), r.getDouble(17)))

    

    val vectors = allDF.rdd.map(r => Vectors.dense( r.getDouble(7),r.getDouble(17))) //Day charge and Churn


    vectors.cache()

    //KMeans model with 2 clusters and 20 iterations

    val kMeansModel = KMeans.train(vectors,3, 20)


    //Print the center of each cluster
    println("cluster centers :")

  //  kMeansModel.clusterCenters.foreach(println)


    kMeansModel.save(sc,"src/main/ressources/seg41")

    val model=KMeansModel.load(sc,"src/main/ressources/seg41")

    // val resultRDD = rowsRDD.map { r => (r._1, model.predict(Vectors.dense(r._5, r._6, r._7, r._8, r._9, r._10))) }

    val resultRDD =rowsRDD.map { r => (r._16, model.predict(Vectors.dense(r._8 ,r._18))) }


    println("**********************************************************")
    val clusterCount = resultRDD.countByValue

    clusterCount.toList.foreach{ println }

    println("**********************************************************")

    val resultDataFrame=resultRDD.toDF( "Phone" ,"Cluster")

    resultDataFrame.show()

   
    val t = allDF.join(resultDataFrame, "Phone")
    t.show()
     t.groupBy("Cluster").mean("Daycharge","NightCharge").show(6)
     t.groupBy("Cluster").sum("Daycharge","NightCharge").show(7)


//t.saveToEs("test_18_04/segmentation")

/*
    t.filter("Cluster =0").show(5)

    t.filter("Cluster =1").show(5)

    t.filter("Cluster =2").show(5)


    println("describe statistics for cluster 0 :")
    t.filter("Cluster =0").describe().show()  // stddev  : standard deviation
    println("describe statistics for cluster 0 by column  :")

    t.filter("Cluster =0").describe( "DayCharge" , "EveCharge" ,"Churn" ,"Cluster").show()

    println("describe statistics for cluster 1 by column  :")

    t.filter("Cluster =1").describe( "DayCharge" , "EveCharge" ,"Churn" ,"Cluster").show()

    println("describe statistics for cluster 1 :")

    t.filter("Cluster =1").describe().show() */
    spark.stop()

    /*
   // t.filter("Cluster =1").write.format("com.databricks.spark.csv").save("src/main/ressources/myFile.csv") // partitionated
   // t.filter("Cluster =1").coalesce(1).write.csv("src/main/ressources/myFile2Notpartitionated.csv") // not partionated



        // Get the prediction from the model with the ID so we can link them back to other information


        val predictions = rowsRDD.map { r => (r._1, kMeansModel.predict(Vectors.dense(r._6, r._7, r._8, r._9, r._10))) }

       // convert the rdd to a dataframe

        val predDF = predictions.toDF("ID", "CLUSTER")
        predDF.show()
    */

    //calcul taux de fidelisation

/*       println("********************** calcul taux de fidélisation **********************\n\n")

   val editedFirstDate= t.withColumn("day_FirstDate", split(col("FirstDate"), "-").getItem(0))
    .withColumn("month_FirstDate", split(col("FirstDate"), "-").getItem(1))
    .withColumn("year_FirstDate", split(col("FirstDate"), "-").getItem(2))



    var costomersbyDate  =editedFirstDate.filter(   "year_FirstDate=  '2017'   ").count()
    costomersbyDate
   println("*****nombre des clients aux debut de cette periode  en 2017  est : " +costomersbyDate)

    var costomersbyDateEnd  =editedFirstDate.filter(   "year_FirstDate= '2017'  and   Churn ='0' ").count()
   println("*****nombre des clients fidéle de cette periode en 2017  est : " +costomersbyDateEnd)

   // var tauxFidelisation=costomersbyDateEnd/costomersbyDate
    // println("Taux de fidelisation = "+tauxFidelisation)

    def taux( a:Long, b:Long ) : Float = {
      var x:Float = 0
      x = a.toFloat/b
      return x
    }


    println("\n Taux de Fidelisation : "  +taux(costomersbyDateEnd, costomersbyDate)+" %\n")




    println("**********************calcul de taux d’attrition**********************\n\n")

    val costomerslostbyyear  =editedFirstDate.filter(   "year_FirstDate=  '2017'   and churn = '1' ").count()
    costomerslostbyyear.isValidLong
    println("*****nombre des clients perdus dans cette periode  en 2017  est : " +costomerslostbyyear)

    println("\nTaux d'attrition: "  +taux(costomerslostbyyear, costomersbyDate)+" %\n")

    val CLTV = 1/taux(costomersbyDateEnd, costomersbyDate)

    println("**********************calcul de CLTV*********************\n")

    println("\nLa durée de vie d’un client est: "+CLTV+" ans\n")

    // val TauxdeFidelisataion =((costomersbyDateEnd - costomersbyDateAcquis / costomersbyDate) /100)

    //println( ((costomersbyDateEnd-costomersbyDateAcquis / costomersbyDate) /100))
    //allDF.filter(allDF("FirstDate")  >=  "01/01/2017 ").show()


   // println("nombre des clients aux debut de cette periode 01 /01 /2015  est "+countcustomerdeputpreiode.count())

     //  println("nombre des clients aux debut de cette periode 01 /01 /2015  est "+allDF.count())

     // val countcutomersinlastperiode=  t.filter( "LastDate =notchurn" )

     // println("nombre des clients aux fin de cette periode  est "+countcutomersinlastperiode.count())

    //val countcutomersinlastperiode=allDF.filter("LastDate ='notchurn' ")


   // println("nombre des clients aux fin de cette periode  est "+countcutomersinlastperiode.count())



// ce ligne a ete ajouté par mohamed ali barhoumi  et youssef encadré par Mrs hamdi avant d'envoyé vers Jenkins
// ce ligne a ete ajouté par mohamed ali barhoumi  et youssef 

*/
  }
}
