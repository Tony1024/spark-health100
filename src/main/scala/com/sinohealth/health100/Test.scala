package com.sinohealth.health100

object Test {

  def main(args: Array[String]): Unit = {

    val ss = "1,2,4,5,6".split(",")

    val array = new Array[String](ss.length)
    ss.copyToArray(array)
    val array2 = Array("1","2","3","4","5","6","7","8","9","10")
    val array3 = array2.intersect(array)
    for (x <- array3) println(x)

    val output1 = array3.reduce((x,y) => x+","+y)
    println(output1)

    val output2 = array3.reduceRight((y,x) => y+","+x)
    println(output2)

    val output3 = array3.fold("0")((x,y)=>x+","+y)
    println(output3)

    array.combinations(3).toList.foreach(x => {
      for (y <- x) print(y)
      println()
    })

    val s="a"
    s match {
      case "a" => println("right")
      case _ => println("wrong")
    }
  }

}




