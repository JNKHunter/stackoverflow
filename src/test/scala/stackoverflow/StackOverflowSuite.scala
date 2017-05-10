package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FunSuite, Suite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

import stackoverflow.StackOverflow._



trait SharedSparkContext extends BeforeAndAfterAll { self:Suite =>

  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  var conf = new SparkConf(false)

  override def beforeAll(): Unit = {
    _sc = new SparkContext("local[2]", "test", conf)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    /*LocalSparkContext.stop(_sc)
    _sc = null*/
    super.afterAll()
  }
}


@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with SharedSparkContext {

  def filePath = {
    val resource = this.getClass.getClassLoader.getResource("stackoverflow/stackoverflow.csv")
    if (resource == null) sys.error("Please download the dataset as explained in the assignment instructions")
    new File(resource.toURI).getPath
  }

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }


  test("test data rdds") {
    val dataRdd = sc.textFile(filePath)
    assert(dataRdd.count() == 8143801)

    val raw = rawPostings(dataRdd)
    assert(raw.count() == 8143801)

    val grouped = groupedPostings(raw)
    assert(grouped.count() == 2121822)

    val scored  = scoredPostings(grouped)
    assert(scored.count() == 2121822)

    val vectors = vectorPostings(scored)
    assert(vectors.count() == 2121822)
  }


}
