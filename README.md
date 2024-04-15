# spark_exam 

My Data Academy Data Engineering Phase 2 exam consists of the completion of two questions. 

The first question of our project involves processing raw data and transforming it into a Parquet file format. Our task here is to carefully parse the raw data, apply necessary transformations to clean and structure it according to the specified schema provided in the assignment, and finally store it in the Parquet format.

Building upon the Parquet file generated in the first phase, the second question focuses on further refining the data by selecting necessary columns and enriching it with additional information. We retrieve more raw data, which has also undergone transformation, and seamlessly integrate it with our existing dataset.

##  Tutorial 
First navigate to [src/main/scala/com/nuvento/sparkexam](https://github.com/HaydenMcKenzie/exam_spark/tree/second_attempt/src/main/scala/com/nuvento/sparkexam) and there is two scala files called [QuestionOne.scala](https://github.com/HaydenMcKenzie/exam_spark/blob/second_attempt/src/main/scala/com/nuvento/sparkexam/QuestionOne.scala) and [QuestionTwo.scala](https://github.com/HaydenMcKenzie/exam_spark/blob/second_attempt/src/main/scala/com/nuvento/sparkexam/QuestionTwo.scala) which contain my code for the exam.


- Before QuestionOne and QuestionTwo are run:
  - Reads customer_data.csv into Spark via `customerData` and Schema `RawCustomerData(customerId: String,forename: String,surname: String)`
  - Reads account_data.csv into Spark via `accountData` and Schema `RawAccountData(customerId: String, accountId: String, balance: Integer)`
  - Reads address_data.csv into Spark via `addressData` and Schema `RawAddressData(addressId: String, customerId: String, address: String)`
  

- When the QuestionOne is run:
  - Joins `customerData` and `accountData` with a left join on `"customerId"` column
  - GroupBy `"customerId", "forename", "surname"`
  - Collect List and Struct `"customerId, "accountId", "balance"` and alias as `"accounts"`
  - Count `"accountIds"` and alias as "`numberAccounts`"
  - Add up all `"balance"` and alias as `"totalBalance"`
  - Round `"balance"` to 2 decimal places and alias as `"averageBalance"`
  - Pass new aggregated data into a `Dataset[CustomerAccoutOutput] `
  - It will create an "output" folder in the sparkexam folder which contains the parquet file needed for question two
  - Prints the Dataset


- When the QuestionTwo is run:
  - Reads parquet File from question one via `parquetFile`
  - Parses the address column from addressData into `"number", "road", "city", "country"` via `parsedData`
  - Joins the `parquetFile` and `parsedData` 
  - Selects `"customerId", "forename", "surname", "accounts"` and array `struct("addressId", "customerId", "address", "number", "road", "city", "country")` and alias as `"address"`
  - Puts new aggregated data into a `Dataset[CustomerDocument]`
  - Show the Dataset

**NOTE: THE PARQUET FILE IN OUTPUT FOLDER HAS NOT BEEN PUSHED TO GIT. YOU WILL NEED TO RUN QUESTION ONE BEFORE RUNNING QUESTION TWO**


## Environmental 
- IDE: Intellij IDEA
- Compiler: Scalac - 3.3.1
- Unit Test: AnyFunSuite - version: 3.2.15
- SDK: 11.0.12
- Archetype: build.sbt