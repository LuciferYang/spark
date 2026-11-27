/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst

import scala.collection.mutable
import scala.quoted._

import org.apache.spark.sql.catalyst.TypeTags.TypeTag

/** Generates the [[TypeTag]] of a type at the call site. This is internal to Spark SQL. */
object TypeTagMacros {

  def materialize[T: Type](using q: Quotes): Expr[TypeTag[T]] = {
    import q.reflect._

    val entries = mutable.ArrayBuffer.empty[Expr[Any]]
    val indices = mutable.ArrayBuffer.empty[(TypeRepr, Int)]

    def strip(t: TypeRepr): TypeRepr = t match {
      case AnnotatedType(underlying, _) => strip(underlying)
      case _ =>
        val next = t.widen.dealias
        if (next == t) t else strip(next)
    }

    // `isInstanceOf` cannot test the abstract types of the reflection API, so this matches.
    def isClassType(t: TypeRepr): Boolean = t match {
      case _: AndType | _: OrType => false
      case _ => t.typeSymbol.isClassDef
    }

    // Prints a type the way Scala 2 reflection does, which the error messages show.
    def shortName(sym: Symbol): String = {
      val full = sym.fullName.replace("$.", ".").stripSuffix("$")
      val omitted = Seq("scala.Predef.", "scala.package.", "scala.", "java.lang.")
        .find(p => full.startsWith(p) && !full.substring(p.length).contains('.'))
      omitted.map(p => full.substring(p.length)).getOrElse(full)
    }

    def scala2Name(t: TypeRepr): String = t match {
      case AnnotatedType(underlying, _) => scala2Name(underlying)
      case AppliedType(tycon, args) =>
        scala2Name(tycon) + args.map(scala2Name).mkString("[", ",", "]")
      case TypeBounds(_, _) => "_"
      case tr: TypeRef => shortName(tr.typeSymbol)
      case other => other.show
    }

    def hasStaticClass(t: TypeRepr): Boolean = {
      val s = strip(t)
      if (s.typeSymbol == defn.ArrayClass) s.typeArgs.headOption.exists(hasStaticClass)
      else isClassType(s)
    }

    def isSubtypeOf[U: Type](t: TypeRepr): Boolean = t <:< TypeRepr.of[U]

    def typeArgsOf(t: TypeRepr): List[TypeRepr] = t.typeArgs.map {
      case TypeBounds(_, hi) => hi
      case arg => arg
    }

    def isDerivedValueClass(t: TypeRepr): Boolean = {
      val s = strip(t)
      isClassType(s) && s <:< TypeRepr.of[AnyVal] &&
        !defn.ScalaPrimitiveValueClasses.contains(s.typeSymbol)
    }

    // The value parameters of a method type, after its type parameters are replaced by `targs`.
    def paramsOf(info: TypeRepr, targs: List[TypeRepr]): List[(String, TypeRepr)] = info match {
      case pt: PolyType if pt.paramNames.size == targs.size => paramsOf(pt.appliedTo(targs), Nil)
      case pt: PolyType => paramsOf(pt.resType, Nil)
      case mt: MethodType => mt.paramNames.zip(mt.paramTypes) ++ paramsOf(mt.resType, Nil)
      case _ => Nil
    }

    // The constructor parameters of a product type, or the error to report when it has none.
    // Each parameter comes with its declared type and its type with the type arguments of `t`.
    def constructorParams(t: TypeRepr): Either[Int, List[(String, TypeRepr, TypeRepr)]] = {
      val sym = t.typeSymbol
      if (sym.flags.is(Flags.Trait)) {
        val companion = sym.companionModule
        if (companion.isNoSymbol) {
          Left(TypeSpec.NoConstructor)
        } else {
          companion.moduleClass.methodMember("apply") match {
            case Nil => Left(TypeSpec.NoConstructor)
            case List(apply) =>
              val params = paramsOf(companion.termRef.memberType(apply), Nil)
              Right(params.map { case (name, tpe) => (name, tpe, tpe) })
            case _ => Left(TypeSpec.NoPrimaryConstructor)
          }
        }
      } else {
        val ctor = sym.primaryConstructor
        if (ctor.isNoSymbol) Left(TypeSpec.NoPrimaryConstructor)
        else {
          val info = t.memberType(ctor)
          Right(paramsOf(info, Nil).zip(paramsOf(info, typeArgsOf(t))).map {
            case ((name, declared), (_, actual)) => (name, declared, actual)
          })
        }
      }
    }

    def classOfExpr(t: TypeRepr): Expr[Class[_]] =
      Literal(ClassOfConstant(t)).asExprOf[Class[_]]

    def describe(original: TypeRepr, t: TypeRepr): Expr[Any] = {
      if (t =:= TypeRepr.of[Nothing]) {
        spec('{ classOf[AnyRef] }, "Nothing", "scala.Nothing", TypeSpec.NothingType)
      } else if (t =:= TypeRepr.of[Null]) {
        spec('{ classOf[AnyRef] }, "Null", "scala.Null", TypeSpec.NullType)
      } else if (!isClassType(t)) {
        t.asType match {
          case '[a] =>
            Expr.summon[TypeTag[a]].getOrElse {
              report.errorAndAbort(s"No TypeTag available for ${t.show}")
            }
        }
      } else {
        val sym = t.typeSymbol
        val isArray = sym == defn.ArrayClass
        val cls = if (!isArray || hasStaticClass(t)) classOfExpr(t) else '{ null }
        val args = typeArgsOf(t).map(entry)
        val hasNewBuilder = isSubtypeOf[scala.collection.Iterable[_]](t) && {
          val companion = sym.companionModule
          !companion.isNoSymbol && companion.moduleClass.methodMember("newBuilder").nonEmpty
        }
        val enumParent = t match {
          case TypeRef(prefix, _) if isSubtypeOf[Enumeration#Value](t) &&
              prefix.termSymbol.flags.is(Flags.Module) =>
            classOfExpr(prefix.widen)
          case _ => '{ null }
        }
        val isProduct = (isSubtypeOf[Product](t) || isSubtypeOf[DefinedByConstructorParams](t)) &&
          !isSubtypeOf[Option[_]](t)
        val (fieldNames, fieldTypes, constructorError) = if (isProduct) {
          constructorParams(t) match {
            case Left(error) => (Nil, Nil, error)
            case Right(params) =>
              // As in the Scala 2.13 build, a field whose declared type is a value class is
              // encoded as the underlying type; a type parameter bound to one is not.
              val resolved = params.map { case (name, declared, paramType) =>
                val fieldType = if (isDerivedValueClass(declared)) {
                  val valueClass = strip(paramType)
                  constructorParams(valueClass).toOption.flatMap(_.headOption)
                    .map(_._3).getOrElse(paramType)
                } else {
                  paramType
                }
                (name, entry(fieldType))
              }
              (resolved.map(_._1), resolved.map(_._2), 0)
          }
        } else {
          (Nil, Nil, 0)
        }
        '{
          new TypeSpec(
            $cls,
            ${ Expr(scala2Name(original)) },
            ${ Expr(sym.fullName.replace("$.", ".").stripSuffix("$")) },
            TypeSpec.Regular,
            ${ Expr(args) },
            ${ Expr(hasNewBuilder) },
            $enumParent,
            ${ Expr(fieldNames) },
            ${ Expr(fieldTypes) },
            ${ Expr(constructorError) })
        }
      }
    }

    def spec(cls: Expr[Class[_]], typeName: String, className: String, kind: Int): Expr[Any] =
      '{
        new TypeSpec($cls, ${ Expr(typeName) }, ${ Expr(className) }, ${ Expr(kind) },
          Nil, false, null, Nil, Nil, 0)
      }

    def entry(original: TypeRepr): Int = {
      val t = strip(original)
      indices.find(_._1 =:= t) match {
        case Some((_, index)) => index
        case None =>
          val index = entries.length
          entries += '{ null }
          indices += (t -> index)
          entries(index) = describe(original, t)
          index
      }
    }

    val root = TypeRepr.of[T]
    if (!isClassType(strip(root)) && !(strip(root) =:= TypeRepr.of[Nothing]) &&
        !(strip(root) =:= TypeRepr.of[Null])) {
      report.errorAndAbort(s"No TypeTag available for ${root.show}")
    }
    entry(root)
    '{ TypeTag.fromEntries[T](${ Expr.ofList(entries.toList) }) }
  }
}
