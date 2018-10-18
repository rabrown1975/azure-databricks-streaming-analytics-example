// Databricks notebook source
// MAGIC %md
// MAGIC ##Azure Databricks Streaming Analytics Example
// MAGIC 
// MAGIC ###Prerequisites
// MAGIC 1. An Event Hub namespace with at least one Event Hub with a Shared Access Policy allowing Listen rights.
// MAGIC 2. A Cognitive Services endpoint for the Text Analytics API.
// MAGIC 3. A Databricks Secrets scope with secrest for the Even Hub connection string (ehcs), Event Hub name (ehn) and the Cognitive Services key (csk).  The example below assumes a scope name of "adbm".
// MAGIC 4. A library created an attached from Maven.  Maven coodinate: `com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.4`
// MAGIC 
// MAGIC ###Links
// MAGIC 
// MAGIC [Original Tutorial: Sentiment analysis on streaming data using Azure Databricks](https://docs.microsoft.com/en-us/azure/azure-databricks/databricks-sentiment-analysis-cognitive-services)
// MAGIC 
// MAGIC [Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html)

// COMMAND ----------

// MAGIC %md
// MAGIC Read incoming tweets from the configured Event Hub and set up the incoming stream. Two secrets are loaded.  One for the Event Hub name (ehn) and another for the Event Hub connection string (ehcs).  Both are loaded from the secret scope named "adbm".  Update as appropriate to load the proper values.

// COMMAND ----------

import org.apache.spark.eventhubs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val ehn  = dbutils.secrets.get(scope="adbm",key="ehn")
val ehcs = dbutils.secrets.get(scope="adbm",key="ehcs")

// Build connection string with the above information
val connectionString = ConnectionStringBuilder(s"${ehcs}")
  .setEventHubName(s"${ehn}")
  .build

val customEventhubParameters =
  EventHubsConf(connectionString)
  .setMaxEventsPerTrigger(5)

val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()

//incomingStream.printSchema

// Sending the incoming stream into the console.
// Data comes in batches!
//incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()



// COMMAND ----------

// MAGIC %md
// MAGIC Define data types for working with the Language and Sentiment API.

// COMMAND ----------

import java.io._
import java.net._
import java.util._

class Document(var id: String, var text: String, var language: String = "", var sentiment: Double = 0.0) extends Serializable

class Documents(var documents: List[Document] = new ArrayList[Document]()) extends Serializable {

    def add(id: String, text: String, language: String = "") {
        documents.add (new Document(id, text, language))
    }
    def add(doc: Document) {
        documents.add (doc)
    }
}

// COMMAND ----------

// MAGIC %md
// MAGIC Define classes and objects for parsing JSON strings.

// COMMAND ----------

class CC[T] extends Serializable { def unapply(a:Any):Option[T] = Some(a.asInstanceOf[T]) }
object M extends CC[scala.collection.immutable.Map[String, Any]]
object L extends CC[scala.collection.immutable.List[Any]]
object S extends CC[String]
object D extends CC[Double]

// COMMAND ----------

// MAGIC %md
// MAGIC Defines an object that contains functions to call the Text Analysis API to run language detection and sentiment analysis.  The value for __host__ should be updated as needed for the correct region.  A secret (csk) is loaded containg the Cognitive Services key from the scope named "adbm".  Update as appropriate to load the proper value.

// COMMAND ----------

val csk  = dbutils.secrets.get(scope="adbm",key="csk")

import javax.net.ssl.HttpsURLConnection
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import scala.util.parsing.json._

object SentimentDetector extends Serializable {

  // Cognitive Services API connection settings
  val accessKey = s"${csk}"
  val host = "https://eastus2.api.cognitive.microsoft.com"
  val languagesPath = "/text/analytics/v2.0/languages"
  val sentimentPath = "/text/analytics/v2.0/sentiment"
  val languagesUrl = new URL(host+languagesPath)
  val sentimenUrl = new URL(host+sentimentPath)

  def getConnection(path: URL): HttpsURLConnection = {
    val connection = path.openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestMethod("POST")
    connection.setRequestProperty("Content-Type", "text/json")
    connection.setRequestProperty("Ocp-Apim-Subscription-Key", accessKey)
    connection.setDoOutput(true)
    return connection
  }

  def prettify (json_text: String): String = {
    val parser = new JsonParser()
    val json = parser.parse(json_text).getAsJsonObject()
    val gson = new GsonBuilder().setPrettyPrinting().create()
    return gson.toJson(json)
  }

  // Handles the call to Cognitive Services API.
  // Expects Documents as parameters and the address of the API to call.
  // Returns an instance of Documents in response.
  def processUsingApi(inputDocs: Documents, path: URL): String = {
    val docText = new Gson().toJson(inputDocs)
    val encoded_text = docText.getBytes("UTF-8")
    val connection = getConnection(path)
    val wr = new DataOutputStream(connection.getOutputStream())
    wr.write(encoded_text, 0, encoded_text.length)
    wr.flush()
    wr.close()

    val response = new StringBuilder()
    val in = new BufferedReader(new InputStreamReader(connection.getInputStream()))
    var line = in.readLine()
    while (line != null) {
        response.append(line)
        line = in.readLine()
    }
    in.close()
    return response.toString()
  }

  // Calls the language API for specified documents.
  // Returns a documents with language field set.
  def getLanguage (inputDocs: Documents): Documents = {
    try {
      val response = processUsingApi(inputDocs, languagesUrl)
      // In case we need to log the json response somewhere
      val niceResponse = prettify(response)
      val docs = new Documents()
      val result = for {
            // Deserializing the JSON response from the API into Scala types
            Some(M(map)) <- scala.collection.immutable.List(JSON.parseFull(niceResponse))
            L(documents) = map("documents")
            M(document) <- documents
            S(id) = document("id")
            L(detectedLanguages) = document("detectedLanguages")
            M(detectedLanguage) <- detectedLanguages
            S(language) = detectedLanguage("iso6391Name")
      } yield {
            docs.add(new Document(id = id, text = id, language = language))
      }
      return docs
    } catch {
          case e: Exception => return new Documents()
    }
  }

  // Calls the sentiment API for specified documents. Needs a language field to be set for each of them.
  // Returns documents with sentiment field set, taking a value in the range from 0 to 1.
  def getSentiment (inputDocs: Documents): Documents = {
    try {
      val response = processUsingApi(inputDocs, sentimenUrl)
      val niceResponse = prettify(response)
      val docs = new Documents()
      val result = for {
            // Deserializing the JSON response from the API into Scala types
            Some(M(map)) <- scala.collection.immutable.List(JSON.parseFull(niceResponse))
            L(documents) = map("documents")
            M(document) <- documents
            S(id) = document("id")
            D(sentiment) = document("score")
      } yield {
            docs.add(new Document(id = id, text = id, sentiment = sentiment))
      }
      return docs
    } catch {
        case e: Exception => return new Documents()
    }
  }
}

// User Defined Function for processing content of messages to return their sentiment.
val toSentiment = udf((textContent: String) => {
  println(textContent)
  val inputDocs = new Documents()
  inputDocs.add (textContent, textContent)
  val docsWithLanguage = SentimentDetector.getLanguage(inputDocs)
  val docsWithSentiment = SentimentDetector.getSentiment(docsWithLanguage)
  if (docsWithLanguage.documents.isEmpty) {
    // Placeholder value to display when unable to perform sentiment request for text in unknown language
    (-1).toDouble
  } else {
    docsWithSentiment.documents.get(0).sentiment.toDouble
  }
})

// COMMAND ----------

// MAGIC %md
// MAGIC Prepare a dataframe with the content of the tweet and the sentiment associated with the tweet.

// COMMAND ----------

// Prepare a dataframe with Content and Sentiment columns
val streamingDataFrame = incomingStream.selectExpr(
    "cast (properties[\"userName\"] as string) as User",
    "cast (properties[\"location\"] as string) as Location",
    "cast (body as string) AS Content", 
    "cast (enqueuedTime as Timestamp) as Time", 
    "cast (enqueuedTime as Long) as Timestamp"
  )
  .withColumn("Sentiment", toSentiment($"Content"))
  

// Display the streaming data with the sentiment
streamingDataFrame.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// Stream tweets to parquet files for analysis.
//streamingDataFrame.writeStream.outputMode("append").option("checkpointLocation","/work/output/tweets-checkpoint").start("/work/output/tweets")


