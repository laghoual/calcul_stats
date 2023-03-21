import java.io.FileInputStream
import java.security.MessageDigest

package object utils {

  def computeChecksum(fileName: String): String = {
    val fis       = new FileInputStream(fileName)
    val md        = MessageDigest.getInstance("MD5")
    val dataBytes = new Array[Byte](1024)
    var nread     = 0
    while (nread != -1) {
      nread = fis.read(dataBytes)
      if (nread > 0) {
        md.update(dataBytes, 0, nread)
      }
    }
    fis.close()
    md.digest().map("%02x".format(_)).mkString
  }

}
