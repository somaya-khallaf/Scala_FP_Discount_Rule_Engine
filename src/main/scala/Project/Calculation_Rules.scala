package Project

import java.io.{File, FileOutputStream, PrintWriter}
import scala.io.{BufferedSource, Source}
import java.time.{ZonedDateTime, LocalDate}
import java.time.temporal.ChronoUnit.DAYS

object Calculation_Rules extends App {
  val lines = Source.fromFile("src/main/resources/TRX1000.csv").getLines().toList.tail

  val f: File = new File("src/main/resources/calculated_discount.csv")
  val writer = new PrintWriter(new FileOutputStream(f,true))

  // timestamp,product_name,expiry_date,quantity,unit_price,channel,payment_method
  case class Order(timestamp: ZonedDateTime, product_name: String, expiry_date: LocalDate, quantity: Int, unit_price: Double, channel: String, payment_method: String)

  def toCall(line: String): Order = {
    val l = line.split(",")
    val timestamp = ZonedDateTime.parse(l(0))
    val product_name = l(1)
    val expiry_date = LocalDate.parse(l(2))
    val quantity = l(3).toInt
    val unit_price = l(4).toDouble
    val channel = l(5)
    val payment_method = l(6)
    Order(timestamp,product_name,expiry_date,quantity,unit_price,channel,payment_method)
  }
  def getDate(ts: String): String = ts.slice(0,10)
  def isProductExpiringSoon(order: Order):Boolean = DAYS.between(order.expiry_date , order.timestamp)  < 30
  def calculateExpiryDiscount(order: Order):Int = 30 -  DAYS.between(order.expiry_date , order.timestamp).toInt
  def isCheeseOrWineProduct(order: Order):Boolean = {
    val product_lower = order.product_name.toLowerCase
    product_lower.contains("cheese") || product_lower.contains("wine")
  }
  def calculateCheeseWineDiscount(order: Order):Int = {
    val product_lower = order.product_name.toLowerCase
    (if (product_lower.contains("cheese")) 10 else 0) +
      (if (product_lower.contains("wine")) 5 else 0)
  }

  def isSpecialMarchDate(order: Order):Boolean = order.timestamp.getMonthValue == 3 && order.timestamp.getDayOfMonth == 23
  def calculateSpecialMarchDiscount(order: Order):Int = 50

  def isBulkPurchase(order: Order):Boolean = order.quantity > 5
  def calculateBulkDiscount(order: Order):Int = {
    order.quantity match {
      case q if q > 15 => 10
      case q if q >= 10 => 7
      case q if q > 5 => 5
      case _ => 0
    }
  }

  def GetDiscountRules():List[(Order => Boolean, Order => Int)] = {
    List(
      (isProductExpiringSoon, calculateExpiryDiscount),
      (isCheeseOrWineProduct, calculateCheeseWineDiscount),
      (isSpecialMarchDate, calculateSpecialMarchDiscount),
      (isBulkPurchase, calculateBulkDiscount)
    )
  }

  def GetOrdersWithDiscount(order: Order, rules: List[(Order => Boolean, Order => Int)]): String = {
    val discounts = rules.filter(_._1(order)).map(_._2(order)).sorted.take(2)
    val avgDiscount = if (discounts.nonEmpty) discounts.sum / discounts.size else 0

    val timestamp = order.timestamp
    val product_name = order.product_name
    val expiry_date = order.expiry_date
    val quantity = order.quantity
    val unit_price = order.unit_price
    val channel = order.channel
    val payment_method = order.payment_method

    val totalPrice = order.unit_price * order.quantity
    val discountAmount = totalPrice * avgDiscount / 100
    val final_price = totalPrice - discountAmount
    s"$timestamp,$product_name,$expiry_date,$quantity,$unit_price,$channel,$payment_method,$avgDiscount,$final_price"
  }


  def writeLine(line: String): Unit = {
    writer.write(line+"\n")
  }
  lines.map(toCall).map(order => GetOrdersWithDiscount(order, GetDiscountRules())).foreach(writeLine)
  writer.close()


}
