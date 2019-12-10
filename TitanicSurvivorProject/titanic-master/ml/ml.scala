val trainData = "/home/ubuntu/titanic/train.csv"
val rawData = sc.textFile(trainData)
val columns = rawData.first().split(",")

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import org.apache.spark.sql.functions._
import sqlContext.implicits._

var tmpdf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(trainData).toDF(columns: _*).cache()

tmpdf.printSchema()
tmpdf.show(20)
tmpdf.describe()

// 뭐로 결정할것인가..
// age

// replace age == null with average age
val avgAge = tmpdf.select(mean("Age")).first()(0).asInstanceOf[Double]
tmpdf = tmpdf.na.fill(avgAge, Seq("Age"))

val toDouble = sqlContext.udf.register("toDouble", ((n: Int) => { n.toDouble }))
val df = tmpdf.drop("Name").drop("Cabin").drop("Ticket").drop("Embarked").withColumn("Survived", toDouble(tmpdf("Survived")))

df.printSchema()

// can't use string => need to convert it to double
import org.apache.spark.ml.feature.StringIndexer
val sexIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("SexIndex")

var demonModel = sexIndexer.fit(df)
demonModel.transform(df).select("Sex", "SexINdex").show(5)

// bucketize fare cost (split into categories)
import org.apache.spark.ml.feature.Bucketizer
val fareSplits = Array(0.0, 50.0, 100.0, 150.0, 200.0, Double.PositiveInfinity) 
val fareBucket = new Bucketizer().setInputCol("Fare").setOutputCol("FareBucket").setSplits(fareSplits)
fareBucket.transform(df).select("Fare", "FareBucket").show(30)


// 어떤 feature를 선택할것인지 명시	
import org.apache.spark.ml.feature.VectorAssembler

val assembler = new VectorAssembler().setInputCols(Array("Pclass", "SexIndex", "Age", "SibSp", "Parch", "FareBucket")).setOutputCol("tmpFeatures")

// features' range is not the same. need to normalize them to 0-1
import org.apache.spark.ml.feature.Normalizer

val normalizer = new Normalizer().setInputCol("tmpFeatures").setOutputCol("features")

// chose an algorithm
import org.apache.spark.ml.classification.LogisticRegression
val logreg = new LogisticRegression().setMaxIter(10)
logreg.setLabelCol("Survived")

import org.apache.spark.ml.Pipeline
val pipeline = new Pipeline().setStages(Array(fareBucket, sexIndexer, assembler, normalizer, logreg))


// diff seed = provides diff random values, by giving same seed, we get the same value
val splits = df.randomSplit(Array(0.7, 0.3))
val train = splits(0).cache()
val test = splits(1).cache()

// get model, based on model we put test data to get result
var model = pipeline.fit(train)
var result = model.transform(test)

// survived 
result.select("PassengerId", "Survived").show(10)


// how good is our model?? (prediction)
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

val predictionAndLabel = result.select("prediction", "Survived").map(row => (row.get(0).asInstanceOf[Double], row.get(1).asInstanceOf[Double])).rdd
val metrics = new BinaryClassificationMetrics(predictionAndLabel)
println(s"Accuracy = ${metrics.areaUnderROC()}\n")


// are we gonna do it this way?
// we only used 0.7, we now use full traindata, use testdata to test data
var model = pipeline.fit(df)

var testdf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/home/ubuntu/titanic/test.csv").toDF().cache()

//preprocess the data (missing)
testdf = testdf.withColumn("Survived", toDouble(testdf("PassengerId")))
var testAvgAge = testdf.select(mean("Age")).first()(0).asInstanceOf[Double]
testdf = testdf.na.fill(testAvgAge, Seq("Age"))
var testAvgFare = testdf.select(mean("Fare")).first()(0).asInstanceOf[Double]
testdf = testdf.na.fill(testAvgFare, Seq("Fare"))

testdf.describe()

val submit_result = model.transform(testdf)

submit_result.select("PassengerId", "prediction").show(20)

val testRdd = submit_result.select("PassengerId", "prediction").rdd
val header = sc.parallelize(Array("PassengerId, Survived"))
val body = testRdd.map(row => row(0).asInstanceOf[Int] + "," + row(1).asInstanceOf[Double].toInt)
header.union(body).saveAsTextFile("/home/ubuntu/titanic/result.csv")

//save model => doesn't work
// model.save("/home/ubuntu/titanic/model.model")

// saving and reading data
import org.apache.spark.ml._
pipeline.write.overwrite.save("/home/ubuntu/titanic/pipeline")
model.write.save("/home/ubuntu/titanic/model")

val pipeline = Pipeline.read.load("/home/ubuntu/titanic/pipeline")
val model = PipelineModel.read.load("/home/ubuntu/titanic/model")