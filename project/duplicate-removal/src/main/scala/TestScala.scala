object TestScala {
  def main(args: Array[String]): Unit = {
    val PATTERN = """^(\S+)\s+(\S+)\s+(\S+)\s+\[([\w:\/]+\s+\-\d{4})\]\s+"(\S+)\s+(\S+)\s+(\S+)"\s+(\d+)\s+(\d+)""".r
    val log = "64.242.88.10 - - [07/Mar/2004:16:10:02 -0800] \"GET /mailman/listinfo/hsdivision HTTP/1.1\" 200 6291"
    log match {
      case PATTERN(ipAddress, clientIdentd, userId, dateTime, method, endpoint, protocol, responseCode,contentSize) => {
        println(ipAddress, clientIdentd, userId, dateTime, method, endpoint, protocol, responseCode,contentSize)
      }
    }
  }
}
