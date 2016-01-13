package loganalyse

//import org.apache.spark.SparkContext._ import für reduceByKey
import java.time.OffsetDateTime
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object LogAnalyseFuns {

  def getParsedData(data: RDD[String]): (RDD[(Row, Int)], RDD[Row], RDD[Row]) = {

    val parsed_logs = data.map(Utilities.parse_line(_))
    val access_logs = parsed_logs.filter(_._2 == 1).map(_._1).cache()
    val failed_logs = parsed_logs.filter(_._2 == 0).map(_._1)
    (parsed_logs, access_logs, failed_logs)
  }

  /*
 * Berechnen Sie für die Content Size die folgenden Werte:
 *
 * minimum: Minimaler Wert
 * maximum: Maximaler Wert
 * average: Durchschnittswert
 *
 * Geben Sie Das folgende Tripel zurück:
 * (min,max,avg)
 */
  def calculateLogStatistic(data: RDD[Row]): (Long, Long, Long) = {
    val contentSizes = data.map(_.getInt(8).toLong)
    val min = contentSizes.reduce((l, r) => if (r < l) r else l)
    val max = contentSizes.reduce((l, r) => if (l < r) r else l)
    val avg = contentSizes.reduce((l, r) => l + r) / data.count()
    (min, max, avg)
  }


  /*
   * Berechnen Sie für die einzelnen Response Codes die Vorkommen
 * Ergebnis soll eine Liste von Tupeln sein, die als ersten Wert den
 * Response Code hat und als zweiten die Anzahl der Vorkommen.
 *
 */
  def getResponseCodesAndFrequencies(data: RDD[Row]): List[(Int, Int)] = {
    data.map(_.getInt(7)).countByValue().toList.map(x => (x._1, x._2.toInt)).sortBy(_._1)
  }


  /*
 * Berechnen Sie 20 beliebige Hosts, von denen mehr als 10 Mal zugegriffen worden.
 * Ergebnis wird im Test auf der Konsole ausgegeben - es findet kein Test statt.
 */
  def get20HostsAccessedMoreThan10Times(data: RDD[Row]): List[String] = {
    data.map(_.getString(0)).countByValue().filter(_._2 > 10).take(20).map(x => x._1).toList
  }

  /*
   * Berechnen Sie die Top Ten Endpoints.
   * Ergebnis soll eine Liste von Tupeln sein, die den Pfad des Endpoints
   * beinhaltet sowie die Anzahl der Zugriffe.
   * Die Liste soll nach der Anzahl der Zugriffe geordnet werden.
   */

  def getTopTenEndpoints(data: RDD[Row]): List[(String, Int)] = {
    data.map(_.getString(5)).countByValue().toList.sortWith(_._2 > _._2).map(x => (x._1, x._2.toInt)).take(10)
  }

  /*
* Berechnen Sie die Top Ten Endpoints, die zu einer Fehlermeldung führen (Response Code != 200).
* Ergebnis soll eine Liste von Tupeln sein, die den Pfad des Endpoints
* beinhaltet sowie die Anzahl der Zugriffe.
* Die Liste soll nach der Anzahl der Zugriffe geordnet werden.
*/
  def getTopTenErrorEndpoints(data: RDD[Row]): List[(String, Int)] = {
    val errorEndpoints = data.map(x => (x.getString(5), x.getInt(7))).filter(_._2 != 200).map(x => x._1)
    errorEndpoints.countByValue().toList.sortWith(_._2 > _._2).map(x => (x._1, x._2.toInt)).take(10)
  }

  /*
   * Berechnen Sie die Anzahl der Requests pro Tag.
   * Ergebnis soll eine Liste von Tupeln sein, die als ersten Wert den Tag im Juni 95 hat (1..30)
   * und als zweiten die Anzahl der Zugriffe.
   * Sortieren Sie die Liste nach dem Tag.
   */

  def getNumberOfRequestsPerDay(data: RDD[Row]): List[(Int, Int)] = {
    data.map(x => x(3).asInstanceOf[OffsetDateTime].getDayOfMonth).countByValue().toList.map(x => (x._1, x._2.toInt))
  }


  /*
   * Berechnen Sie die Anzahl der Hosts, die im Juni 95 auf den Server zugegriffen haben.
   * Dabei soll jeder Host nur einmal gezählt werden.
   */
  def numberOfUniqueHosts(data: RDD[Row]): Long = {
    data.map(_.getString(0)).distinct().count()
  }


  /*
   * Berechnen Sie die Anzahl der Hosts, die pro Tag auf den Server zugegriffen haben.
   * Dabei soll jeder Host nur einmal gezählt werden.
   * Sortieren Sie die Liste nach dem Tag.
   */
  def numberOfUniqueDailyHosts(data: RDD[Row]): List[(Int, Int)] = {
    val listOfAppearedDays = data.map(x => x(3).asInstanceOf[OffsetDateTime].getDayOfMonth).distinct().countByValue().map(x=> (x._1, x._2.toInt)).toList.sortBy(_._1)
    listOfAppearedDays.map(x => (x._1, data.map(y => (y.getString(0), y(3).asInstanceOf[OffsetDateTime].getDayOfMonth)).filter(_._2 == x._1).distinct().count().toInt))
  }


  /*
   * Berechnen Sie die durchschnittliche Anzahl von Requests pro Host für jeden Tag des Junis.
   * Sortieren Sie die Liste nach dem Tag.
   */
  def averageNrOfDailyRequestsPerHost(data: RDD[Row]): List[(Int, Int)] = {
    val hostDayMap = data.map(y => (y.getString(0), y(3).asInstanceOf[OffsetDateTime].getDayOfMonth))
    val listOfDays = hostDayMap.map(x=> x._2).distinct().countByValue().map(x=> (x._1, x._2.toInt)).toList.sortBy(_._1)
    listOfDays.map(x => (x._1, (hostDayMap.filter(_._2 == x._1).map(z=> z._1).countByValue().values.sum / hostDayMap.filter(_._2 == x._1).distinct().count()).toInt))
  }

  /*
   * Berechnen Sie die Top 25 Hosts, die Error Codes (Response Code=404)
   * verursacht haben. Ergebnis soll dabei ein Set sein, bestehend aus dem
   * Hostnamen und der Anzahl der Requests.
   * Die Reihenfolge des Ergebnisses ist beliebig.
   */

  def top25ErrorCodeResponseHosts(data: RDD[Row]): Set[(String, Int)] = {
    val hostErrorCodeMap = data.map(x => (x.getString(0), x.getInt(7))).filter(_._2 == 404).map(_._1)
    hostErrorCodeMap.countByValue().map(x=> (x._1, x._2.toInt)).toList.sortWith(_._2 > _._2).take(25).toSet
  }


  /*
   * Berechnen Sie die Anzahl der Error-Codes (Response Code=404) die pro Tag erzeugt wurden.
   * Ergebnis soll eine Liste von Tupeln sein, die als erstes den Tag und als Zweites die Anzahl aufweist.
   * Dabei soll eine Sortierung nach dem Tag erfolgen.
   */
  def responseErrorCodesPerDay(data: RDD[Row]): List[(Int, Int)] = {
    val listOfAppearedDays = data.map(x => x(3).asInstanceOf[OffsetDateTime].getDayOfMonth).distinct().countByValue().map(x=> (x._1, x._2.toInt)).toList.sortBy(_._1)
    val error404DayMap = data.map(x => (x(3).asInstanceOf[OffsetDateTime].getDayOfMonth, x.getInt(7))).filter(_._2 == 404).cache()
    listOfAppearedDays.map(x=> (x._1, error404DayMap.filter(_._1 == x._1).count().toInt))
  }


  /*
   * Berechnen Sie die Error Response Codes (404), die in den einzelnen Stunden des Tages auftreten.
   * Ergebnis soll eine Liste von Tupeln sein, deren erstes Element die Stunde bestimmt (0..23) und
   * das Zweite die Anzahl der Error Codes.
   * Sortieren Sie die Liste nach der Stunde.
   */
  def errorResponseCodeByHour(data: RDD[Row]): List[(Int, Int)] = {
    val listOfHours = List((0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (7, 0), (8, 0), (9, 0), (10, 0), (11, 0), (12, 0), (13, 0), (14, 0), (15, 0), (16, 0), (17, 0), (18, 0), (19, 0), (20, 0), (21, 0), (23, 0))
    val error404HoursMap = data.map(x => (x(3).asInstanceOf[OffsetDateTime].getHour, x.getInt(7))).filter(_._2 == 404).cache()
    listOfHours.map(x=> (x._1, error404HoursMap.filter(_._1 == x._1).count().toInt))
  }


  /*
   * Berechnen Sie die Anzahl der Requests pro Wochentag (Montag, Dienstag,...).
   * Ergebnis soll eine Liste von Tupeln sein, die als erstes Element die Anzahl der Requests beinhaltet
   * und als zweites den Wochentag, ausgeschrieben als String.
   * Die Elemente sollen folgendermaßen angeordnet sein: [Montag, Dienstag, Mittwoch, Donnerstag, Freitag, Samstag, Sonntag].
   */
  def getAvgRequestsPerWeekDay(data: RDD[Row]): List[(Int, String)] = {
    val weekday = List(("Montag", "MONDAY"), ("Dienstag", "TUESDAY"), ("Mittwoch", "WEDNESDAY"), ("Donnerstag", "THURSDAY"), ("Freitag", "FRIDAY"), ("Samstag", "SATURDAY"), ("Sonntag", "SUNDAY"))
    val listOfRequestsPerWeekDay = data.map(x => x(3).asInstanceOf[OffsetDateTime].getDayOfWeek.toString).countByValue().map(y=> (y._2.toInt, y._1)).toList
    val nrDays = data.map(x=>(x(3).asInstanceOf[OffsetDateTime].getDayOfWeek.toString, x(3).asInstanceOf[OffsetDateTime].getDayOfMonth)).distinct().map(X=>(X._1,1)).reduceByKey(_+_)
    weekday.map(x=> (listOfRequestsPerWeekDay.filter(_._2 == x._2).head._1 / nrDays.filter(_._1 == x._2).values.first(), x._1))
  }


}
