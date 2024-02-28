import com.nuvento.sparkexam.combinedata.JoinData
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import com.nuvento.sparkexam.utils._

class JoinDataTest extends AnyFunSuite with BeforeAndAfter {

  var spark: SparkSession = _

  before {
    spark = SparkSetup.spark
  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }

  test("Test joinData function") {
    SparkSetup.main(Array.empty[String])
    import SparkSetup.spark.implicits._

    // Sample input data
    val firstData = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("customerId", "name")
    val secondData = Seq((1, "Account1"), (2, "Account2"), (4, "Account4")).toDF("customerId", "accountName")

    // Call the function
    val result = JoinData.joinData(firstData, secondData)

    // Expected result
    val expected = Seq((1, "Alice", "Account1"), (2, "Bob", "Account2")).toDF("customerId", "name", "accountName")

    // Compare the result with the expected output
    assert(result.collect().sameElements(expected.collect()))
  }
}