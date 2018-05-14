package io.paradoxical.carlyle.core.guice

import com.google.inject.{AbstractModule, _}
import com.google.inject.assistedinject.FactoryModuleBuilder
import java.lang.reflect.{ParameterizedType, Type}
import javax.inject.Singleton
import scala.collection.mutable
import scala.collection.JavaConverters._

abstract class GuiceModule extends AbstractModule {
  import ClassUtil._

  override def configure(): Unit = {
    if (bindings.nonEmpty) {
      bindings.foreach(_ ())
    }
  }

  protected val bindings: mutable.ArrayBuffer[() => Unit] = new mutable.ArrayBuffer()

  def register[Contract: Manifest, Impl <: Contract : Manifest] = {
    bindings.append(() => bind(clazz[Contract]).to(clazz[Impl]))
  }

  def instance[Contract: Manifest, Impl <: Contract : Manifest](impl: Impl) = {
    bindings.append(() => bind(clazz[Contract]).toInstance(impl))
  }

  def instance[Impl: Manifest](impl: Impl) = {
    bindings.append(() => bind(clazz[Impl]).toInstance(impl))
  }

  def singleton[Contract: Manifest, Impl <: Contract : Manifest] = {
    bindings.append(() => bind(clazz[Contract]).to(clazz[Impl]).in(classOf[Singleton]))
  }

  def singleton[Impl: Manifest] = {
    bindings.append(() => bind(clazz[Impl]).to(clazz[Impl]).in(classOf[Singleton]))
  }

  def factory[Instance: Manifest, Factory: Manifest]: Unit = {
    bindings.append(() =>
      install(
        new FactoryModuleBuilder().
          implement(clazz[Instance], clazz[Instance]).
          build(manifest[Factory].runtimeClass)
      )
    )
  }

  def factory[Contract: Manifest, Instance <: Contract : Manifest, Factory: Manifest]: Unit = {
    bindings.append(() =>
      install(
        new FactoryModuleBuilder().
          implement(clazz[Contract], clazz[Instance]).
          build(manifest[Factory].runtimeClass)
      )
    )
  }
}

object ClassUtil {
  def clazz[T: Manifest]: TypeLiteral[T] = {
    val typ: Type =
      if (manifest[T].typeArguments.isEmpty) {
        manifest[T].runtimeClass
      } else {
        new ParameterizedType {
          override def getActualTypeArguments: Array[Type] = manifest.typeArguments.map(_.runtimeClass).toArray

          override def getRawType: Type = manifest.runtimeClass

          override def getOwnerType: Type = null
        }
      }

    TypeLiteral.get(typ).asInstanceOf[TypeLiteral[T]]
  }
}

object Modules {
  def apply(): List[Module] = List()
}

/**
 * Adds simple modules to the list
 *
 * @param list
 */
class Adder(list: List[Module]) {
  /**
   * Add a particular instance to be injectable.  If the instance type is polymorphic you'll need to specify
   * the type you want to register it _as_. i.e. if have
   * {{{
   *   trait Foo
   *   class Bar extends Foo
   * }}}
   *
   * And you want to inject Foo but are adding an instance of Bar, you will need to do
   *
   * {{{
   *   .instance[Foo](new Bar)
   * }}}
   *
   * Each time the value is requested the {{{source}}} call by name will be re-evaluated
   *
   * @param source
   * @tparam T
   * @return
   */
  def instance[T: Manifest](source: => T): List[Module] = {
    new AbstractModule {
      override def configure(): Unit = {
        bind(ClassUtil.clazz[T]).toProvider(new Provider[T] {
          override def get(): T = source
        })
      }
    } :: list
  }

  /**
   * Add a particular singleton to be injectable.  If the instance type is polymorphic you'll need to specify
   * the type you want to register it _as_. i.e. if have
   * {{{
   *   trait Foo
   *   class Bar extends Foo
   * }}}
   *
   * And you want to inject Foo but are adding an instance of Bar, you will need to do
   *
   * {{{
   *   .singleton[Foo](new Bar)
   * }}}
   *
   * @param source
   * @tparam T
   * @return
   */
  def singleton[T: Manifest](source: => T): List[Module] = {
    new AbstractModule {
      override def configure(): Unit = {
        bind(ClassUtil.clazz[T]).toInstance(source)
      }
    } :: list
  }

  /**
   * Create a dynamic binding from one type to another.
   *
   * I.e.
   *
   * {{{
   *   trait Foo
   *   class Bar extends Foo
   *
   *   .binding[Foo, Bar]
   * }}}
   *
   * This says that if someone requests a Foo to provide them an instance of Bar.
   *
   * Each time someone requests Foo a new Bar will be made
   *
   * @tparam ToBind
   * @tparam BindType
   * @return
   */
  def binding[ToBind: Manifest, BindType <: ToBind : Manifest]: List[Module] = {
    new AbstractModule {
      override def configure(): Unit = {
        bind(ClassUtil.clazz[ToBind]).to(ClassUtil.clazz[BindType])
      }
    } :: list
  }

  /**
   * Create a dynamic binding from one type to another as a singleton.
   *
   * I.e.
   *
   * {{{
   *   trait Foo
   *   class Bar extends Foo
   *
   *   .binding[Foo, Bar]
   * }}}
   *
   * This says that if someone requests a Foo to provide them a singleton instance of Bar.
   *
   * @tparam ToBind
   * @tparam BindType
   * @return
   */
  def bindingSingleton[ToBind: Manifest, BindType <: ToBind : Manifest]: List[Module] = {
    new AbstractModule {
      override def configure(): Unit = {
        bind(ClassUtil.clazz[ToBind]).to(ClassUtil.clazz[BindType]).in(classOf[Singleton])
      }
    } :: list
  }

  /**
   * Bind assisted factories
   * @tparam ToMake
   * @return
   */
  def factoryFor[ToMake: Manifest] = new FactoryMaker[ToMake]

  class FactoryMaker[ToMake: Manifest]() {
    /**
     * If you have an assisted factory pattern, register the factory type
     *
     * i.e.
     *
     * {{{
     *   trait Factory {
     *      def load(name: String): Bar
     *   }
     *
     *   class Bar @Inject()(@Assisted name: String)
     *
     *   .providedBy[Factory]
     * }}}
     * @tparam Factory
     * @return
     */
    def providedBy[Factory: Manifest]: List[Module] = {
      new AbstractModule {
        override def configure(): Unit = {
          install(new FactoryModuleBuilder()
            .implement(ClassUtil.clazz[ToMake], ClassUtil.clazz[ToMake])
            .build(ClassUtil.clazz[Factory]))
        }
      } :: list
    }
  }
}

/**
 * Overrides simple modules in the list
 *
 * @param list
 * @param overrideManifest
 * @tparam Source
 */
class Overrider[Source <: Module](list: List[Module])(implicit overrideManifest: Manifest[Source]) {
  import GuiceUtil._

  def nowGivesInstance[T: Manifest](source: => T): List[Module] = {
    list.overrideModule[Source](
      new AbstractModule {
        override def configure(): Unit = {
          bind(ClassUtil.clazz[T]).toProvider(new Provider[T] {
            override def get(): T = source
          })
        }
      })
  }

  def nowGivesSingleton[T: Manifest](source: => T): List[Module] = {
    list.overrideModule[Source](
      new AbstractModule {
        override def configure(): Unit = {
          bind(ClassUtil.clazz[T]).toInstance(source)
        }
      }
    )
  }
}

object GuiceUtil {
  implicit class RichModules(list: List[Module]) {
    /**
     * Replace the module with another
     *
     * @param module The module to substitute with
     * @param manifest
     * @tparam T The module type to replace
     * @return
     */
    def overrideModule[T <: Module](module: Module)(implicit manifest: Manifest[T]): List[Module] = {
      module :: list.filterNot(m => m.getClass == manifest.runtimeClass.asInstanceOf[Class[T]])
    }

    /**
     * Overlay the bindings in module m over the bindings in module T
     * lets you do partial overrides instead of full overrides
     *
     * See http://google.github.io/guice/api-docs/latest/javadoc/index.html?com/google/inject/util/Modules.html
     * @param m
     * @tparam T
     * @return
     */
    def overlay[T <: Module : Manifest](m: Module): List[Module] = {
      val module = list.find(_.getClass == manifest[T].runtimeClass)

      list.filterNot(_ == module.get) :+ com.google.inject.util.Modules.`override`(module.get).`with`(m)
    }

    def module[Overrides <: Module](implicit manifest: Manifest[Overrides]): Overrider[Overrides] = {
      new Overrider[Overrides](list)
    }

    /**
     * Add things that aren't in the default modules list
     *
     * @return
     */
    def add: Adder = new Adder(list)

    /**
     * Create an injector from a list
     *
     * @return
     */
    def injector(): Injector = {
      Guice.createInjector(list.asJava)
    }
  }

  implicit class RichInjector(injector: Injector) {
    def make[T: Manifest]: T = {
      injector.getInstance(Key.get(ClassUtil.clazz[T]))
    }
  }
}

