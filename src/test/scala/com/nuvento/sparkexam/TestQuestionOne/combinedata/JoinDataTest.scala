import com.nuvento.sparkexam.combinedata.JoinData
import com.nuvento.sparkexam.handlefiles.ReadData.readFileData
import com.nuvento.sparkexam.handlefiles.Schemas
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import com.nuvento.sparkexam.utils._
import org.apache.spark.sql.types.{StringType, StructField, StructType, IntegerType}

class JoinDataTest extends AnyFunSuite with BeforeAndAfter {
  SparkSetup.main(Array.empty[String])
  import SparkSetup.spark.implicits._


  test("Test joinData function on fixed data") {
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

  test("Test joinData function Count more than 0") {
    // Input data
    val firstData = readFileData[Schemas.customerSchema]("customer_data")
    val secondData = readFileData[Schemas.accountSchema]("account_data")

    // Call the function
    val joinedData: Dataset[_] = JoinData.joinData(firstData, secondData)

    // Test if it is has more than 0
    assert(joinedData.count() > 0)
  }

  test("Test joinData function Schema") {
    // Input data
    val firstData = readFileData[Schemas.customerSchema]("customer_data")
    val secondData = readFileData[Schemas.accountSchema]("account_data")

    // Call the function
    val joinedData: Dataset[_] = JoinData.joinData(firstData, secondData)
    val actualSchema = joinedData.schema

    // Expected
    val expectedSchema = StructType(Seq(
      StructField("customerId", StringType, nullable = true),
      StructField("forename", StringType, nullable = true),
      StructField("surname", StringType, nullable = true),
      StructField("accountId", StringType, nullable = true),
      StructField("balance", IntegerType, nullable = true)
    ))

    // Compare the result with the expected output
    assert(actualSchema == expectedSchema)
  }
}