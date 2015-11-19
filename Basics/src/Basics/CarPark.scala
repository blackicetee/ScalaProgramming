package CarPark

/**
 * Created by MrT on 28.10.2015.
 */
class CarPark(maximumParkingSlots: Int) {
  private val parkingSchedule = new Array[String](maximumParkingSlots)

  def simple() { parkingSchedule(0) = "Audi S8"}

  def isCarParked(car: String): Boolean = parkingSchedule contains(car)

  def countFreeSlots(): Int = {
    var i = 0
    for (c <- parkingSchedule)
      if (c == null)
        i += 1
    i
  }


  def addCar(car: String): Unit = {
    if (isCarParked(car) == false) {
      if (countFreeSlots() != 0)
        parkingSchedule(maximumParkingSlots - countFreeSlots()) = car
      else
        Console.err.println("Car-park is full!")
    }
    else
      Console.err.println("Your car is already parked!")
  }

  def pullCar(yourCar: String): Unit = {
    var i = 0
    var res = 0
    if (isCarParked(yourCar) == true) {
      for (car <- parkingSchedule) {
        if (car == yourCar)
          res = i
        else
          i += 1
      }
      parkingSchedule(res) = null
    }
    else {
      Console.err.println("Your car isn`t in the parked yet!")
    }
  }
  def getCarPark(): Array[String] = parkingSchedule


}
object CarPark extends App {
  val myCarPark = new CarPark(5)
  for (car <- myCarPark.getCarPark())
    println(car)
  myCarPark.addCar("Audi S8")
  myCarPark.addCar("VW Golf")
  myCarPark.addCar("VW Golf")
  myCarPark.addCar("Audi CK180")
  myCarPark.addCar("Mercedes S Class")
  myCarPark.addCar("Toyota")
  myCarPark.addCar("Subaru")
  for (car <- myCarPark.getCarPark())
    println(car)
  myCarPark.simple()
  println(myCarPark.isCarParked("Audi S8"))
  println(myCarPark.countFreeSlots())
}