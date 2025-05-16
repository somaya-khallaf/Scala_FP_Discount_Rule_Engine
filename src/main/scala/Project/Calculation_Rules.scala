package Project

import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.io.Source
import java.time.{ZonedDateTime,Instant, LocalDate}
import java.time.temporal.ChronoUnit.DAYS

object Calculation_Rules extends App {
  val logFile: File = new File("src/main/resources/rules_engine.log")
  val logWriter = new PrintWriter(new FileOutputStream(logFile, true))

  def log(level: String, message: String): Unit = {
    val timestamp = Instant.now()
    logWriter.write(s"TIMESTAMP: $timestamp LOGLEVEL: $level MESSAGE: $message\n")
    logWriter.flush()
  }

  case class Order(timestamp: ZonedDateTime, product_name: String, expiry_date: LocalDate, quantity: Int, unit_price: Double, channel: String, payment_method: String)

  log("INFO", "Starting discount rules engine")

  val dbUrl = "jdbc:postgresql://localhost:5432/discount_db"
  val dbUser = "discount_user"
  val dbPassword = "discount_pass"

  val orders = Source.fromFile("src/main/resources/TRX1000.csv").getLines().toList.tail
  log("INFO", s"Loaded ${orders.size} orders from CSV file")

  val outputFile: File = new File("src/main/resources/calculated_discount.csv")
  val writer = new PrintWriter(new FileOutputStream(outputFile, true))

  // Initialize database resources directly
  val connection: Connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)
  val preparedStatement: PreparedStatement = {
    initializeDatabase(connection)
    prepareInsertStatement(connection)
  }

  def initializeDatabase(connection: Connection): Unit = {
    val statement = connection.createStatement()
    statement.executeUpdate(
      """
            CREATE TABLE IF NOT EXISTS calculated_discounts (
            id SERIAL PRIMARY KEY,
            order_date TIMESTAMP,
            product_name VARCHAR(255),
            expiry_date DATE,
            quantity INT,
            unit_price DECIMAL(10, 2),
            channel VARCHAR(50),
            payment_method VARCHAR(50),
            discount_percentage DECIMAL(5, 2),
            final_price DECIMAL(10, 2)
          )
        """)
    log("INFO", "Database table initialized successfully")
  }

  def prepareInsertStatement(connection: Connection): PreparedStatement = {
    val sql =
      """
          INSERT INTO calculated_discounts
          (order_date, product_name, expiry_date, quantity, unit_price, channel, payment_method, discount_percentage, final_price)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    connection.prepareStatement(sql)
  }

  def toOrder(line: String): Order = {
    val l = line.split(",")
    val timestamp = ZonedDateTime.parse(l(0))
    val product_name = l(1)
    val expiry_date = LocalDate.parse(l(2))
    val quantity = l(3).toInt
    val unit_price = l(4).toDouble
    val channel = l(5)
    val payment_method = l(6)
    Order(timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method)
  }

  def isProductExpiringSoon(order: Order): Boolean =
    DAYS.between(order.timestamp.toLocalDate, order.expiry_date) < 30

  def calculateExpiryDiscount(order: Order): Int =
    (30 - DAYS.between(order.timestamp.toLocalDate, order.expiry_date)).toInt

  def isCheeseOrWineProduct(order: Order): Boolean = {
    val product_lower = order.product_name.toLowerCase
    product_lower.contains("cheese") || product_lower.contains("wine")
  }

  def calculateCheeseWineDiscount(order: Order): Int = {
    val product_lower = order.product_name.toLowerCase
    (if (product_lower.contains("cheese")) 10 else 0) +
      (if (product_lower.contains("wine")) 5 else 0)
  }

  def isSpecialMarchDate(order: Order): Boolean =
    order.timestamp.getMonthValue == 3 && order.timestamp.getDayOfMonth == 23

  def calculateSpecialMarchDiscount(order: Order): Int = 50

  def isBulkPurchase(order: Order): Boolean = order.quantity > 5

  def calculateBulkDiscount(order: Order): Int = {
    order.quantity match {
      case q if q > 15 => 10
      case q if q >= 10 => 7
      case q if q > 5 => 5
      case _ => 0
    }
  }

  def isAppSale(order: Order): Boolean = order.channel.toLowerCase == "app"

  def calculateAppQuantityDiscount(order: Order): Int = {
    val quantityGroup = Math.ceil(order.quantity / 5.0).toInt
    quantityGroup * 5
  }

  def isVisaPayment(order: Order): Boolean = order.payment_method.toLowerCase == "visa"

  def calculateVisaDiscount(order: Order): Int = 5

  def GetDiscountRules(): List[(Order => Boolean, Order => Int)] = {
    List(
      (isProductExpiringSoon, calculateExpiryDiscount),
      (isCheeseOrWineProduct, calculateCheeseWineDiscount),
      (isSpecialMarchDate, calculateSpecialMarchDiscount),
      (isBulkPurchase, calculateBulkDiscount),
      (isAppSale, calculateAppQuantityDiscount),
      (isVisaPayment, calculateVisaDiscount)
    )

  }
  def calculateDiscountAndPrice(order: Order, rules: List[(Order => Boolean, Order => Int)]): (Int, Double) = {
    val discounts = rules.filter(_._1(order)).map(_._2(order)).sortBy(-_).take(2)
    val avgDiscount = if (discounts.nonEmpty) discounts.sum / discounts.size else 0
    val totalPrice = order.unit_price * order.quantity
    val finalPrice = totalPrice - (totalPrice * avgDiscount / 100)
    (avgDiscount, finalPrice)
  }

  def GetOrdersWithDiscount(order: Order, rules: List[(Order => Boolean, Order => Int)]): String = {
    val (avgDiscount, finalPrice) = calculateDiscountAndPrice(order, rules)
    s"${order.timestamp},${order.product_name},${order.expiry_date},${order.quantity},${order.unit_price},${order.channel},${order.payment_method},$avgDiscount,$finalPrice"
  }

  try {
    val rules = GetDiscountRules()

    orders.map(toOrder).foreach { order =>
      try {
        log("INFO", s"Processing order for ${order.product_name}")

        val (avgDiscount, finalPrice) = calculateDiscountAndPrice(order, rules)
        writer.write(GetOrdersWithDiscount(order, rules) + "\n")

        preparedStatement.setTimestamp(1, java.sql.Timestamp.from(order.timestamp.toInstant))
        preparedStatement.setString(2, order.product_name)
        preparedStatement.setDate(3, java.sql.Date.valueOf(order.expiry_date))
        preparedStatement.setInt(4, order.quantity)
        preparedStatement.setDouble(5, order.unit_price)
        preparedStatement.setString(6, order.channel)
        preparedStatement.setString(7, order.payment_method)
        preparedStatement.setDouble(8, avgDiscount)
        preparedStatement.setDouble(9, finalPrice)

        preparedStatement.addBatch()

        log("INFO", s"Completed processing for ${order.product_name}")
      } catch {
        case e: Exception => log("ERROR", s"Error processing order: ${e.getMessage}")
      }
    }

    preparedStatement.executeBatch()
    log("INFO", "Batch insert into database completed")
    log("INFO", "CSV output written")
  } catch {
    case e: Exception => log("ERROR", s"Fatal error in rules engine: ${e.getMessage}")
  } finally {

    preparedStatement.close()
    log("INFO", "Prepared statement closed successfully")

    connection.close()
    log("INFO", "Database connection closed successfully")

    writer.close()
    log("INFO", "Output file writer closed successfully")

    logWriter.close()
    log("INFO", "Log writer closed successfully")

    log("INFO", "Engine shutdown completed")
  }
}
