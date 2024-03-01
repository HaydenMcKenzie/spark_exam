# spark_exam 

My Data Academy Data Engineering Phase 2 exam consists of the completion of two questions.

##  Tutorial 
First navigate to [src/main/scala/com/nuvento/sparkexam](https://github.com/HaydenMcKenzie/exam_spark/tree/master/src/main/scala/com/nuvento/sparkexam) and there is two scala files called [QuestionOne.scala](https://github.com/HaydenMcKenzie/exam_spark/blob/master/src/main/scala/com/nuvento/sparkexam/QuestionOne.scala) and [QuestionTwo.scala](https://github.com/HaydenMcKenzie/exam_spark/blob/master/src/main/scala/com/nuvento/sparkexam/QuestionTwo.scala) which contain my code for the exam.

- When the QuestionOne is run:
  - Reads customer_data.csv and account_data.csv files
  - Transfroms it to the requirements of exam
  - It will create an output file in the sparkexam folder which contains the parquet file needed for question two
- When the QuestionTwo is run:
  - Reads account_data.csv file and the parquet file from question one
  - Removes columns 
  - Merges remaining columns 

## Environmental 
- IDE: Intellij IDEA
- Compiler: Scalac - 3.3.1
- Unit Test: AnyFunSuite - version: 3.2.15
- SDK: 11.0.12
- Archetype: build.sbt 

## Other
There is a [practice folder](https://github.com/HaydenMcKenzie/exam_spark/tree/master/src/main/scala/com/nuvento/practice) which I have which I used to build my logic and play with certain functions