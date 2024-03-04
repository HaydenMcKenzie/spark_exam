# spark_exam 

My Data Academy Data Engineering Phase 2 exam consists of the completion of two questions. 

The first question of our project involves processing raw data and transforming it into a Parquet file format. Our task here is to carefully parse the raw data, apply necessary transformations to clean and structure it according to the specified schema provided in the assignment, and finally store it in the Parquet format.

Building upon the Parquet file generated in the first phase, the second question focuses on further refining the data by removing unnecessary columns and enriching it with additional information. We retrieve more raw data, which has also undergone transformation, and seamlessly integrate it with our existing dataset.

##  Tutorial 
First navigate to [src/main/scala/com/nuvento/sparkexam](https://github.com/HaydenMcKenzie/exam_spark/tree/master/src/main/scala/com/nuvento/sparkexam) and there is two scala files called [QuestionOne.scala](https://github.com/HaydenMcKenzie/exam_spark/blob/master/src/main/scala/com/nuvento/sparkexam/QuestionOne.scala) and [QuestionTwo.scala](https://github.com/HaydenMcKenzie/exam_spark/blob/master/src/main/scala/com/nuvento/sparkexam/QuestionTwo.scala) which contain my code for the exam.

- When the QuestionOne is run:
  - Reads customer_data.csv and account_data.csv files
  - Transfroms it to the requirements of exam
  - It will create an "output" folder in the sparkexam folder which contains the parquet file needed for question two
  - Prints the Dataset
- When the QuestionTwo is run:
  - Reads account_data.csv file and the parquet file from question one
  - Removes unwanted columns
  - Merges remaining columns 
  - Prints the Dataset

**NOTE: THE PARGUET FILE IN OUTPUT FOLDER HAS NOT BEEN PUSHED TO GIT. YOU WILL NEED TO RUN QUESTION 1 BEFORE RUNNING QUESTION 2**


## Environmental 
- IDE: Intellij IDEA
- Compiler: Scalac - 3.3.1
- Unit Test: AnyFunSuite - version: 3.2.15
- SDK: 11.0.12
- Archetype: build.sbt 

## Other
There is a [practice folder](https://github.com/HaydenMcKenzie/exam_spark/tree/master/src/main/scala/com/nuvento/practice) which I have which I used to build my logic and play with certain functions