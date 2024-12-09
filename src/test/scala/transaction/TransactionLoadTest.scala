package transaction

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.http.Predef._

import java.nio.file.{Files, Paths}
import scala.concurrent.duration._
import java.time.LocalDateTime
import java.util.zip.ZipFile
import scala.jdk.CollectionConverters._
import java.sql.{Connection, DriverManager}
import java.time.format.DateTimeFormatter

class TransactionLoadTest extends Simulation {

  // Paramètres configurables
  val databaseUrl = "jdbc:oracle:thin:@your_database_host:port/service_name"
  val databaseUser = "your_username"
  val databasePassword = "your_password"
  val txDate = System.getProperty("tx_date", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
  val transactionsDir = "data/transactions"
  val lastFilePath = s"$transactionsDir/dernier_fichier.xml"

  // Date et heure de début de l'injection
  val injectionStartTime = LocalDateTime.now()

  // Fonction pour remplacer la date dans un fichier XML
  def replaceDateInXml(xmlContent: String, newDate: String): String = {
    xmlContent.replaceAll("""<EndDateTime>\d{4}-\d{2}-\d{2}""", s"<EndDateTime>$newDate")
  }

  // Récupération des fichiers XML dans le répertoire
  val xmlFiles = Files.list(Paths.get(transactionsDir))
    .iterator().asScala
    .filter(Files.isRegularFile(_))
    .filter(_.getFileName.toString.endsWith(".xml"))
    .map(_.toString)
    .toList

  // Préparation des transactions via un feeder
  val transactionsFeeder = new Feeder[String] {
    var currentIndex = 0

    def hasNext: Boolean = currentIndex < xmlFiles.size
    def next(): Map[String, String] = {
      val xmlFilePath = xmlFiles(currentIndex)
      val xmlContent = scala.io.Source.fromFile(xmlFilePath).mkString
      val modifiedXml = replaceDateInXml(xmlContent, txDate)

      // Écrire le contenu modifié dans le fichier
      Files.writeString(Paths.get(xmlFilePath), modifiedXml)

      currentIndex += 1
      Map("transaction" -> modifiedXml)
    }
  }

  // Protocole HTTP
  val httpProtocol = http.baseUrl("http://endpoint")

  // Scénario principal d'injection
  val scn = scenario("Injection de Transactions")
    .feed(transactionsFeeder)
    .exec(http("Inject Transaction")
      .post("/transaction-endpoint")
      .body(StringBody("${transaction}"))
      .asXml
    )
    .exec { session =>
      println(s"Transaction ${session("transaction").as[String]} injectée.")
      session
    }
    .exec(http("Inject Last File")
      .post("/transaction-endpoint")
      .body(StringBody(scala.io.Source.fromFile(lastFilePath).mkString))
      .asXml
    )

  // Vérification du traitement dans la base de données
  def checkProcessingStatus(): Boolean = {
    var connection: Connection = null
    try {
      connection = DriverManager.getConnection(databaseUrl, databaseUser, databasePassword)
      val query = s"""
      WITH params AS (
        SELECT '${injectionStartTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))}' AS STARTDATETIME
        FROM DUAL
      )
      SELECT
        COUNT(*) AS processing_count,
        MAX(CASE
          WHEN STATUS = 'COMPLETED' THEN 1
          ELSE 0
        END) AS is_completed
      FROM ORCS_INTEGRATION oi, params
      WHERE INTEG_TIMESTAMP BETWEEN TO_DATE(params.STARTDATETIME, 'YYYY-MM-DD HH24:MI:ss') AND SYSDATE
        AND WORKSTATION_ID = 900
        AND TX_SEQUENCE_NUMBER = 900
        AND STORE = 3050
        AND FLOW_NAME = 'ORCS'
    """
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(query)

      if (resultSet.next()) {
        val processingCount = resultSet.getInt("processing_count")
        val isCompleted = resultSet.getInt("is_completed") == 1
        processingCount > 0 && isCompleted
      } else {
        false
      }
    } catch {
      case e: Exception =>
        println(s"Erreur lors de la vérification : ${e.getMessage}")
        false
    } finally {
      if (connection != null) connection.close()
    }
  }

  // Configuration de la simulation
  setUp(
    scn.inject(
      rampUsers(10).during(30.seconds),
      constantUsersPerSec(5).during(3.hours)
    ).protocols(httpProtocol)
  )

  // Vérification post-test
  after {
    val injectionEndTime = LocalDateTime.now()
    val totalTransactions = transactionsFeeder.currentIndex
    val averageProcessingTime = if (totalTransactions > 0) {
      java.time.Duration.between(injectionStartTime, injectionEndTime).toMillis / totalTransactions
    } else {
      0
    }

    println(s"Nombre total de transactions : $totalTransactions")
    println(s"Temps moyen par transaction : $averageProcessingTime ms")

    val maxWaitTime = 4.hours.toMillis
    val pollInterval = 30.seconds.toMillis
    var totalWaitTime = 0L

    while (!checkProcessingStatus() && totalWaitTime < maxWaitTime) {
      println("Attente de la fin des traitements...")
      Thread.sleep(pollInterval)
      totalWaitTime += pollInterval
    }

    if (totalWaitTime >= maxWaitTime) {
      println("Le traitement n'est pas terminé dans le délai imparti.")
      assert(false)
    }

    assert(averageProcessingTime < 100, "Le temps moyen par transaction dépasse 100ms")
    assert(checkProcessingStatus(), "Le traitement n'est pas terminé")
  }
}
