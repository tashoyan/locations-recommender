package com.github.tashoyan.recommender.test

import com.github.tashoyan.recommender.test.SparkTestHarness._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.scalatest.{Outcome, fixture}

/**
  * Harness trait for unit tests that require Spark.
  * <p>
  * <b>Usage</b>
  * <p>
  * {{{
  * import org.scalatest.fixture
  *
  * class MyTest extends fixture.FunSuite with SparkTestHarness {
  *
  *   override protected def sparkSettings: Map[String, String] = super.sparkSettings +
  *     ("spark.custom.setting" -> "custom_value")
  *
  *   override protected def onSparkCreate(spark: SparkSession): Unit = {
  *     spark.udf.register(myCustomUdf)
  *   }
  *
  *   test("my test case") { spark: SparkSession =>
  *     import spark.implicits._
  *     ...
  *   }
  *
  * }
  * }}}
  * <p>
  * <b>Enabling Hive support</b>
  * <p>
  * The SparkSession instance can be created with Hive support enabled.
  * <ol>
  *   <li> Make sure that your pom-file includes Hive dependencies
  *   <li> Configure scalatest-maven-plugin in the pom-file as follows:
  *   {{{
  *   <plugin>
  *     <artifactId>scalatest-maven-plugin</artifactId>
  *     <configuration>
  *       <parallel>false</parallel>
  *       <systemProperties>
  *         <java.io.tmpdir>${project.build.directory}</java.io.tmpdir>
  *         <spark.test.harness.enable.hive.support>true</spark.test.harness.enable.hive.support>
  *         <derby.system.home>${project.build.directory}</derby.system.home>
  *       </systemProperties>
  *     </configuration>
  *     ...
  *   </plugin>
  *   }}}
  * </ol>
  * Note:
  * <ul>
  *   <li> It is not possible to reuse a Hive Metastore Derby DB created by one Spark Session instance with another instance.
  *   <li> It is not possible to have two Spark Sessions with distinct Hive Metastores within one JVM
  * </ul>
  * Hence, a Spark Session with Hive support can be created only once and reused across all unit tests.
  */
trait SparkTestHarness extends fixture.FunSuite with OnSparkCreate {

  protected val defaultSparkSettings: Map[String, String] = Map(
    "spark.master" -> "local[4]",
    "spark.serializer" -> classOf[KryoSerializer].getName,
    "spark.kryo.unsafe" -> "true",
    "spark.ui.enabled" -> "false",
    "spark.sql.warehouse.dir" -> s"${sys.props("java.io.tmpdir")}/spark-warehouse",
    "spark.app.name" -> this.getClass.getSimpleName
  )

  /**
    * Override this method to provide non-default Spark settings in your tests.
    * <p>
    * The default implementation returns [[defaultSparkSettings]].
    *
    * @return Spark settings used when creating SparkSession.
    */
  protected def sparkSettings: Map[String, String] = defaultSparkSettings

  /**
    * Override this method to make custom steps on initializing the SparkSession.
    * <p>
    * The default implementation is no-op.
    *
    * @param spark SparkSession used in tests.
    */
  override protected def onSparkCreate(spark: SparkSession): Unit = ()

  override def withFixture(test: OneArgTest): Outcome = {
    val spark = getOrCreateSpark
    onSparkCreate(spark)
    withFixture(test.toNoArgTest(spark))
  }

  override type FixtureParam = SparkSession

  private def getOrCreateSpark: SparkSession = {
    val builder0 = SparkSession.builder()
    val builder = sparkSettings.foldLeft(builder0) { case (aggBuilder, (key, value)) =>
      aggBuilder.config(key, value)
    }

    possiblyEnableHiveSupport(builder)
      .getOrCreate()
  }

  private def possiblyEnableHiveSupport(builder: SparkSession.Builder): SparkSession.Builder = {
    val enableHiveSupport: Boolean = sys.props
      .get(enableHiveSupportProp)
      .exists(_.toBoolean)
    if (enableHiveSupport) {
      builder.enableHiveSupport()
    } else {
      builder
    }
  }

}

object SparkTestHarness {
  val enableHiveSupportProp = "spark.test.harness.enable.hive.support"
}

trait OnSparkCreate {
  protected def onSparkCreate(spark: SparkSession): Unit
}
