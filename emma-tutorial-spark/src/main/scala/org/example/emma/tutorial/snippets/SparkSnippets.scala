/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example.emma.tutorial
package snippets

import data.openflights
import data.openflights._

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.emmalanguage.api.spark.toRDD

object SparkSnippets {

  private def airports(implicit spark: SparkSession) =
    openflights.airports.as[RDD]

  private def airlines(implicit spark: SparkSession) =
    openflights.airlines.as[RDD]

  private def routes(implicit spark: SparkSession) =
    openflights.routes.as[RDD]

  // ---------------------------------------------------------
  // Example 1
  // ---------------------------------------------------------

  private[snippets] def `example 1(a)`(implicit spark: SparkSession) = {
    val berlinAirports = airports
      .filter(a => a.latitude > 52.3)
      .filter(a => a.latitude < 52.6)
      .map(a => Location(
        a.name,
        a.latitude,
        a.longitude))
      .filter(l => l.longitude > 13.2)
      .filter(l => l.longitude < 13.7)

    berlinAirports.collect()
  }

  private[snippets] def `example 1(b)`(implicit spark: SparkSession) = {
    val berlinAirports = airports
      .filter(a => a.latitude > 52.3)
      .filter(a => a.latitude < 52.6)
      .filter(a => a.longitude > 13.2)
      .filter(a => a.longitude < 13.7)
      .map(a => Location(
        a.name,
        a.latitude,
        a.longitude))

    berlinAirports.collect()
  }

  private[snippets] def `example 1(c)`(implicit spark: SparkSession) = {
    import spark.implicits._
    val berlinAirports = airports.toDS()
      .filter($"latitude" > 52.3)
      .filter($"latitude" < 52.6)
      .select($"name", $"latitude", $"longitude")
      .filter($"longitude" > 13.2)
      .filter($"longitude" < 13.7)
      .as[Location[String]]

    berlinAirports.collect()
  }

  private[snippets] def `example 1(d)`(implicit spark: SparkSession) = {
    import spark.implicits._
    airports.toDS().createOrReplaceTempView("airports")

    val berlinAirports = spark.sql(s"""
      |SELECT a.name,
      |       a.latitude,
      |       a.longitude
      |FROM   airports AS a
      |WHERE  a.latitude > 52.3
      |AND    a.latitude < 52.6
      |AND    a.longitude > 13.2
      |AND    a.longitude < 13.7
      """.stripMargin.trim
    ).as[Location[String]]

    berlinAirports.collect()
  }

  // ---------------------------------------------------------
  // Example 2
  // ---------------------------------------------------------

  private[snippets] def `example 2(a)`(implicit spark: SparkSession) = {
    val xs = airlines.keyBy(al => Option(al.id))
      .join(routes.keyBy(_.airlineID))
      .values
    val ys = xs.keyBy(_._2.srcID)
      .join(airports.keyBy(ap => Some(ap.id)))
      .values
    val zs = ys.map({
      case ((al, _), ap) =>
        (al.name, ap.country)
    })

    zs.collect()
  }

  private[snippets] def `example 2(b)`(implicit spark: SparkSession) = {
    val xs = airports.keyBy(ap => Option(ap.id))
      .join(routes.keyBy(_.srcID))
      .values
    val ys = xs.keyBy(_._2.airlineID)
      .join(airlines.keyBy(al => Some(al.id)))
      .values
    val zs = ys.map({
      case ((ap, _), al) =>
        (al.name, ap.country)
    })

    zs.collect()
  }

  private[snippets] def `example 2(c)`(implicit spark: SparkSession) = {
    import spark.implicits._
    airports.toDS().createOrReplaceTempView("airports")
    airlines.toDS().createOrReplaceTempView("airlines")
    routes.toDS().createOrReplaceTempView("routes")

    val zs = spark.sql(s"""
      |SELECT al.name, ap.country
      |FROM   airports AS ap,
      |       routes   AS rt,
      |       airlines AS al
      |WHERE  rt.srcID     = ap.id
      |AND    rt.airlineID = al.id
      """.stripMargin.trim
    ).as[(String, String)]

    zs.collect()
  }

  // ---------------------------------------------------------
  // Example 3
  // ---------------------------------------------------------

  private[snippets] def `example 3(a)`(implicit spark: SparkSession) = {
    val aggs = routes
      .groupBy(_.src)
      .map({ case (key, itr) =>
        val v = itr.toSeq
        val k = key
        val x = v.count(_.airline == "AB")
        val y = v.count(_.airline == "LH")
        k -> (x, y)
      })

    aggs.collect()
  }

  private[snippets] def `example 3(b)`(implicit spark: SparkSession) = {
    val aggs = routes
      .map(r =>
        r.src -> (AB(r.airline), LH(r.airline)))
      .reduceByKey((u, v) => {
        val x = u._1 + v._1
        val y = u._2 + v._2
        (x, y)
      })

    aggs.collect()
  }

  private val AB = (v: String) => if (v == "AB") 1L else 0L
  private val LH = (v: String) => if (v == "LH") 1L else 0L

  private[snippets] def `example 3(c)`(implicit spark: SparkSession) = {
    import spark.implicits._
    routes.toDS().createOrReplaceTempView("routes")

    val aggs = spark.sql(s"""
      |SELECT   r.src,
      |         SUM( CASE r.airline WHEN 'AB'
      |                THEN 1
      |                ELSE 0 END ) AS x,
      |         SUM( CASE r.airline WHEN 'LH'
      |                THEN 1
      |                ELSE 0 END ) AS y
      |FROM     routes AS r
      |GROUP BY r.src
      """.stripMargin.trim
    ).as[(String, Long, Long)]

    aggs.map({ case (k, x, y) => k -> (x, y) }).collect()
  }

  // ---------------------------------------------------------
  // Example 4
  // ---------------------------------------------------------

  private[snippets] def `example 4(a)`(implicit spark: SparkSession) = {
    val hs = hubs(50)
    val rs = reachableA(2)(hs)
    rs.collect()
  }

  private[snippets] def `example 4(b)`(implicit spark: SparkSession) = {
    val hs = hubs(50)
    val rs = reachableB(2)(hs)
    rs.collect()
  }

  private def hubs(M: Int)(implicit spark: SparkSession) =  {
    val rs =
      ({
        routes.map(_.src -> 1L)
      } union {
        routes.map(_.dst -> 1L)
      }).reduceByKey(_ + _)
      .filter(_._2 < M)
      .map(_._1)

    rs.collect().toSet
  }

  private def reachableA(n: Int)(hubs: Set[String])(implicit spark: SparkSession) = {
    val hubroutes = routes
      .filter(r => hubs(r.src) && hubs(r.dst))

    var paths = hubroutes
      .map(r => Path(r.src, r.dst))
    for (_ <- 0 until n) {
      val delta = hubroutes.keyBy(_.dst)
        .join(paths.keyBy(_.src)).values
        .map(x => Path(x._1.src, x._2.dst))
      paths = (paths union delta).distinct()
    }

    paths
  }

  private def reachableB(n: Int)(hubs: Set[String])(implicit spark: SparkSession) = {
    val hubroutes = routes
      .filter(r => hubs(r.src) && hubs(r.dst))
      .cache()
    var paths = hubroutes
      .map(r => Path(r.src, r.dst))
    for (_ <- 0 until n) {
      val delta = hubroutes.keyBy(_.dst)
        .join(paths.keyBy(_.src)).values
        .map(x => Path(x._1.src, x._2.dst))
      paths = (paths union delta).distinct()
    }

    paths
  }
}
