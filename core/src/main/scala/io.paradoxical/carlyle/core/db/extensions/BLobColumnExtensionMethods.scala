package io.paradoxical.carlyle.core.db.extensions

import java.math.BigInteger
import slick.ast.ScalaBaseType._
import slick.ast.{Library, TypedType}
import slick.lifted.{ExtensionMethods, Rep}

final class BLobColumnExtensionMethods[P1](val c: Rep[P1]) extends AnyVal with ExtensionMethods[Array[Byte], P1] {
  protected[this] implicit def b1Type = implicitly[TypedType[Array[Byte]]]

  def length[R](implicit om: o#to[Int, R]) =
    om.column(Library.Length, n)
}

final class BigIntegerColumnExtensionMethods[P1](val c: Rep[P1]) extends AnyVal with ExtensionMethods[BigInteger, P1] {
  protected[this] implicit def b1Type = implicitly[TypedType[BigInteger]]

  def =!=[P2, R] (e: Rep[P2])(implicit om: o#arg[BigInteger, P2]#to[Boolean, R]) =
    om.column(Library.Not, Library.==.typed(om.liftedType, n, e.toNode))
}
