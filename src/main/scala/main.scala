import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.util.Try
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueType._
import java.util.Map.Entry
import java.io.File

object TemplateApplication {
  private def getPropertiesList( key: String ) = {
    val list = Try( config.getConfig( key ).entrySet().toArray ).getOrElse( Array() )
    list.map( x => {
      val p = x.asInstanceOf[Entry[String, ConfigValue]]
      val k = p.getKey

      val v = p.getValue.valueType match {
        case BOOLEAN => config.getBoolean( key + "." + k )
        case STRING => config.getString( key + "." + k )
        case NUMBER => config.getDouble( key + "." + k )
        case _ => config.getString( key + "." + k )
      }
      ( k.replace( "_", "." ), v.toString )
    } )
  }

  // Spark configuration - loading from "resources/application.conf"
  private var config = ConfigFactory.load()
  lazy val SPARK_MASTER_HOST = Try( config.getString( "spark.master_host" ) ).getOrElse( "local" )
  lazy val SPARK_MASTER_PORT = Try( config.getInt( "spark.master_port" ) ).getOrElse( 7077 )
  lazy val SPARK_HOME = Try( config.getString( "spark.home" ) ).getOrElse( "/home/spark" )
  lazy val SPARK_MEMORY = Try( config.getString( "spark.memory" ) ).getOrElse( "1g" )
  lazy val SPARK_OPTIONS = getPropertiesList( "spark.options" )

  def connectToSparkCluster(): SparkContext = {

    // get the name of the packaged
    val thisPackagedJar = new File( "target/scala-2.10" ).listFiles.filter( x => x.isFile && x.getName.toLowerCase.takeRight( 4 ) == ".jar" ).toList.map( _.toString )

    // Scan for external libraries in folder 'lib'
    // All the JAR files in this folder will be shipped to the cluster
    val libs = new File( "lib" ).listFiles.filter( x => x.isFile && x.getName.toLowerCase.takeRight( 4 ) == ".jar" ).toList.map( _.toString )

    val master = if ( SPARK_MASTER_HOST.toUpperCase == "LOCAL" ) "local" else SPARK_MASTER_HOST + ":" + SPARK_MASTER_PORT

    // Spark Context configuration
    val scConf =
      SPARK_OPTIONS.fold(
        new SparkConf()
          .setMaster( master )
          .setAppName( "TemplateApplication" )
          .set( "spark.executor.memory", SPARK_MEMORY )
          .setSparkHome( SPARK_HOME )
          .setJars( libs ++ thisPackagedJar )
          )( ( c, p ) => { // apply each spark option from the configuration file in section "spark.options"
              val ( k, v ) = p.asInstanceOf[( String, String )]
              c.asInstanceOf[SparkConf].set( k, v )
            }
          ).asInstanceOf[SparkConf]
    // Create and return the spark context to be used through the entire KYC application
    new SparkContext( scConf )
  }

  def main(args: Array[String] ) {
    val sc = connectToSparkCluster

    val f = sc.textFile("README.md")

    println(f.count)

  }
}
