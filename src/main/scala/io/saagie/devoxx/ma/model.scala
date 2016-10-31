package io.saagie.devoxx.ma

/**
  * Created by aurelien on 27/10/16.
  */
object model extends Serializable {
  object Quote extends Serializable {
    val pat = """(\d+),[^,]*,\"([^"]+)\",[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,([^,]*),(\d+),.*""".r
    def parse(s: String): Option[Quote] = {
      try {
        pat.findFirstMatchIn(s).map(x => Quote(x.subgroups(1), 1000*(x.subgroups.head.toLong/1000),  x.subgroups(2).toDouble, x.subgroups(3).toLong))
      } catch {
        case _ : Throwable => None
      }
    }
  }

  case class Quote(symbol: String, ts: Long, price: Double, volume: Long)
}
