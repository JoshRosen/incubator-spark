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

package org.apache.spark.tools

import scala.reflect.runtime.{universe=>ru}
import scala.reflect.runtime.universe._

import org.apache.spark.api.java._

import org.apache.spark.rdd.{OrderedRDDFunctions, DoubleRDDFunctions, PairRDDFunctions, RDD}
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{PairDStreamFunctions, DStream}
import org.apache.spark.streaming.api.java._

private[spark]
case class SparkMethod(name: String, returnType: Type, parameters: Seq[Type]) {
  override def toString: String = {
    // TODO: might be nice to print the parameter names
    s"$name(${parameters.mkString(", ")}): $returnType"
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[SparkMethod]

  override def equals(other: Any): Boolean = other match {
    case that: SparkMethod =>
      (that canEqual this) &&
        name == that.name &&
        returnType.typeConstructor =:= that.returnType.typeConstructor &&
        parameters.size == that.parameters.size &&
        parameters.zip(that.parameters).forall { case (a, b) => a.typeConstructor =:= b.typeConstructor }
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(name)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

/**
 * A tool for identifying methods that need to be ported from Scala to the Java API.
 *
 * It uses reflection to find methods in the Scala API and rewrites those methods' signatures
 * into appropriate Java equivalents.  If those equivalent methods have not been implemented in
 * the Java API, they are printed.
 */
object JavaAPICompletenessChecker {

  private def toSparkMethod(method: MethodSymbol): SparkMethod = {
    val name = method.name.toString
    // TODO: only handles the first parameter list
    val parameters = method.paramss
    val parameterTypes =
      if (parameters.size != 0) {
        parameters.head.map(_.typeSignature)
      } else {
        Seq.empty
      }
    SparkMethod(name, method.returnType, parameterTypes)
  }

  private def toJavaType(scalaType: Type): Type = {
    val substitutions = Seq(
      typeOf[scala.collection.Map[_, _]] -> typeOf[java.util.Map[Any, Any]],
      // TODO: the JavaStreamingContext API accepts Array arguments
      // instead of Lists, so this isn't a trivial translation / sub:
      typeOf[scala.collection.Seq[_]] -> typeOf[java.util.List[Any]],
      //typeOf[scala.Boolean] -> typeOf[java.lang.Boolean],
      //typeOf[scala.Double] -> typeOf[java.lang.Double],
      //typeOf[scala.Int] -> typeOf[java.lang.Integer],
      //typeOf[scala.Float] -> typeOf[java.lang.Float],
      //typeOf[scala.Long] -> typeOf[java.lang.Long],
      typeOf[RDD[_]] -> typeOf[JavaRDD[Any]],
      typeOf[DStream[_]] -> typeOf[JavaDStream[Any]],
      typeOf[scala.Seq[_]] -> typeOf[java.util.List[Any]],
      typeOf[scala.Function2[_, _, _]] -> typeOf[org.apache.spark.api.java.function.Function2[Any, Any, Any]],
      typeOf[scala.Array[_]] -> typeOf[java.util.List[Any]],
      typeOf[Option[_]] -> typeOf[com.google.common.base.Optional[Any]],
      typeOf[Function1[_, Unit]] -> typeOf[VoidFunction[Any]],
      typeOf[Function1[_, _]] -> typeOf[org.apache.spark.api.java.function.Function[Any, Any]],
      typeOf[scala.collection.Iterator[_]] -> typeOf[java.util.Iterator[Any]],
      typeOf[SparkContext] -> typeOf[JavaSparkContext],
      typeOf[StreamingContext] -> typeOf[JavaStreamingContext]
      //typeOf[scala.collection.mutable.Queue] -> typeOf[java.util.Queue]
      //double" -> "java.lang.Double"
    )

    def substituted(typ: Type) =
      substitutions.find(x => typ <:< x._1).map(_._2).getOrElse(typ)

    val javaType: Type = scalaType match {
      case tr @ TypeRef(pre, sym, args) if args.size != 0 =>
        if (sym.asType.toType <:< typeOf[RDD[_]] && args.head.erasure =:= typeOf[(Any, Any)].erasure) {
          args.head match {
            case TypeRef(_, _, tupleArgs) =>
              typeRef(pre, typeOf[JavaPairRDD[Any, Any]].typeSymbol, tupleArgs.map(toJavaType))
          }
        } else {
          substituted(tr) match {
            case TypeRef(newPre, newSym, _) =>
              typeRef(newPre, newSym, args.map(toJavaType))
          }
        }
      case typ: Type =>
        substituted(typ)
    }
    javaType
  }

  private def toJavaMethod(method: SparkMethod): SparkMethod = {
    val params = method.parameters.map(toJavaType)
    SparkMethod(method.name, toJavaType(method.returnType), params)
  }

  private def printMissingMethods[ScalaClass, JavaClass](implicit st: TypeTag[ScalaClass], jt: TypeTag[JavaClass]) {
    def publicMethods[Clazz](implicit tt: TypeTag[Clazz]) =  {
      val typ: ru.type#Type = ru.typeOf[Clazz]
      val members = typ.baseClasses.flatMap(_.typeSignature.members) ++ typ.members
      members
        .filter(_.isMethod).map(_.asMethod)
        .filter(_.isPublic)
        .filterNot(_.name.toString.contains("$"))
    }

    val javaEquivalents = publicMethods[ScalaClass].map(m => toJavaMethod(toSparkMethod(m))).toSet

    val javaMethods = publicMethods[JavaClass].map(toSparkMethod).toSet

    val missingMethods = javaEquivalents -- javaMethods

    for (method <- missingMethods) {
      println(method)
    }
  }

  def main(args: Array[String]) {
    println("Missing RDD methods")
    printMissingMethods[RDD[_], JavaRDD[_]]
    println()

    println("Missing PairRDD methods")
    printMissingMethods[PairRDDFunctions[_, _], JavaPairRDD[_, _]]
    println()

    println("Missing DoubleRDD methods")
    printMissingMethods[DoubleRDDFunctions, JavaDoubleRDD]

    println("Missing OrderedRDD methods")
    printMissingMethods[OrderedRDDFunctions[_, _, _], JavaPairRDD[_, _]]
    println()

    println("Missing SparkContext methods")
    printMissingMethods[SparkContext, JavaSparkContext]
    println()

    println("Missing StreamingContext methods")
    printMissingMethods[StreamingContext, JavaStreamingContext]
    println()

    println("Missing DStream methods")
    printMissingMethods[DStream[_], JavaDStream[_]]
    println()

    println("Missing PairDStream methods")
    printMissingMethods[PairDStreamFunctions[_, _], JavaPairDStream[_, _]]
    println()
  }
}
