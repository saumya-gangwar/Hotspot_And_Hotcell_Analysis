package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
	val rectangleCoords = queryRectangle.split(",")
    val rectangleCoords_x1 = rectangleCoords(0).trim.toDouble
    val rectangleCoords_y1 = rectangleCoords(1).trim.toDouble
    val rectangleCoords_x2 = rectangleCoords(2).trim.toDouble
    val rectangleCoords_y2 = rectangleCoords(3).trim.toDouble
  
	val pointCoords = pointString.split(",")
    val pointCoords_x1 = pointCoords(0).trim.toDouble
    val pointCoords_y1 = pointCoords(1).trim.toDouble
  
    var max_rectangleCoords_x = rectangleCoords_x1
    var min_rectangleCoords_x = rectangleCoords_x2
  
    if(rectangleCoords_x1<rectangleCoords_x2)
    {
      max_rectangleCoords_x = rectangleCoords_x2
      min_rectangleCoords_x = rectangleCoords_x1
    }
  
    var max_rectangleCoords_y = rectangleCoords_y1
    var min_rectangleCoords_y = rectangleCoords_y2
  
    if(rectangleCoords_y1<rectangleCoords_y2)
    {
      max_rectangleCoords_y = rectangleCoords_y2
      min_rectangleCoords_y = rectangleCoords_y1
    }    
    
    if(pointCoords_x1 >= min_rectangleCoords_x && pointCoords_x1 <= max_rectangleCoords_x && pointCoords_y1 >= min_rectangleCoords_y && pointCoords_y1 <= max_rectangleCoords_y)
		return true
	else
		return false
  }

}
