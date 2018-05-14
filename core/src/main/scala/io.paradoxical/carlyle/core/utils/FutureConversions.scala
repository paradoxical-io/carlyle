package io.paradoxical.carlyle.core.utils


import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object FutureConversions {
  import com.twitter.util.{Future => TFuture, Promise => TPromise}
  import scala.concurrent.{Future => SFuture, Promise => SPromise}

  object Implicits {
    implicit def twitterToScalaFuture[T](tFuture: TFuture[T]): TFutureToSFuture[T] = new TFutureToSFuture[T](tFuture)
    implicit def scalaToTwitterFuture[T](sFuture: SFuture[T]): SFutureToTFuture[T] = new SFutureToTFuture[T](sFuture)
  }

  implicit class TFutureToSFuture[T](tFuture: TFuture[T]) {
    /**
     * Convert the twitter future to a scala future.
     *
     * When the twitter future completes successfully, the returned scala future completes successfully.
     * When the twitter future completes with an exception, the returned scala future completes with an exception.
     *
     * @param ctx The execution context of the scala future.
     * @return    The scala future.
     */
    def toSFuture: SFuture[T] = {
      val scalaPromise = SPromise[T]()

      tFuture.onSuccess(v => {
        scalaPromise.success(v)
      })
      tFuture.onFailure(e => {
        scalaPromise.failure(e)
      })

      scalaPromise.future
    }
  }

  implicit class SFutureToTFuture[T](sFuture: SFuture[T]) {
    /**
     * Convert the scala future to a twitter future.
     *
     * When the scala future completes successfully, the returned twitter future completes successfully.
     * When the scala future completes with an exception, the returned twitter future completes with an exception.
     *
     * @param ctx The execution context of the scala future.
     * @return    The twitter future.
     */
    def toTFuture(implicit ctx: ExecutionContext): TFuture[T] = {
      val twitterPromise = TPromise[T]()

      sFuture.onComplete {
        case Success(v) => twitterPromise.setValue(v)
        case Failure(t) => twitterPromise.setException(t)
      }

      twitterPromise
    }
  }
}
