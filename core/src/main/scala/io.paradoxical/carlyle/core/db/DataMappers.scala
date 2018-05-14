//
// Copyright (c) 2011-2017 by Curalate, Inc.
//

package io.paradoxical.carlyle.core.db

import io.paradoxical.rdb.slick.dao.SlickDAO
import io.paradoxical.carlyle.core.model._
import io.paradoxical.carlyle.core.api.modules.BigIntProvider
import java.math.BigInteger
import java.sql.{PreparedStatement, ResultSet}
import java.util.UUID
import javax.inject.Inject
import org.joda.time.DateTime
import slick.ast._
import slick.jdbc._
import slick.relational.RelationalTypesComponent

class DataMappers @Inject()(
  val slickDBProvider: BigIntProvider
) {

  lazy val converter = new SlickDAO.Implicits(slickDBProvider.driver)

  import slickDBProvider.driver.api._

  implicit val batchItemIdMapper: JdbcType[BatchItemGroupId] with BaseTypedType[BatchItemGroupId] = {
    MappedColumnType.base[BatchItemGroupId, String](_.value.toString, x => BatchItemGroupId(UUID.fromString(x)))
  }

  implicit val batchIdMapper: JdbcType[BatchId] with BaseTypedType[BatchId] = {
    MappedColumnType.base[BatchId, Long](_.value, BatchId)
  }

  implicit val jodaSetParam = new SetParameter[DateTime] {
    override def apply(v1: DateTime, v2: PositionedParameters): Unit = v2.setLong(v1.getMillis)
  }
}

trait BigIntegerTypesComponent extends RelationalTypesComponent {
  self: JdbcProfile =>

  implicit def bigIntType = new BigIntegerJdbcType

  implicit val bigIntSetParam = new SetParameter[BigInteger] {
    override def apply(v1: BigInteger, v2: PositionedParameters): Unit = {
      v2.setObject(v1, java.sql.Types.BIGINT)
    }
  }

  class BigIntegerJdbcType extends DriverJdbcType[BigInteger] with NumericTypedType {
    def sqlType = java.sql.Types.BIGINT

    def setValue(v: BigInteger, p: PreparedStatement, idx: Int) = {
      p.setObject(idx, v)
    }

    def getValue(r: ResultSet, idx: Int): BigInteger = {
      val v = r.getObject(idx)
      if (v eq null) {
        null
      } else {
        v match {
          case l: java.lang.Long =>
            BigInteger.valueOf(l)
          case _ =>
            v.asInstanceOf[BigInteger]
        }
      }
    }

    def updateValue(v: BigInteger, r: ResultSet, idx: Int) = {
      r.updateObject(idx, v)
    }
  }
}
