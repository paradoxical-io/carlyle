package io.paradoxical.carlyle.core.db.extensions

import java.math.BigInteger
import slick.lifted.Rep

object SlickExtensions {
  implicit def blobColumnExtensionMethods(c: Rep[Array[Byte]]): BLobColumnExtensionMethods[Array[Byte]] = new BLobColumnExtensionMethods[Array[Byte]](c)
  implicit def bigIntegerColumnExtensionMethods(c: Rep[BigInteger]): BigIntegerColumnExtensionMethods[BigInteger] = new BigIntegerColumnExtensionMethods[BigInteger](c)
}
