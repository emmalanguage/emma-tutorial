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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.emmalanguage.api.flink.toDataSet

object FlinkSnippets {

  private def airports(implicit flink: ExecutionEnvironment) =
    openflights.airports.as[DataSet]

  private def airlines(implicit flink: ExecutionEnvironment) =
    openflights.airlines.as[DataSet]

  private def routes(implicit flink: ExecutionEnvironment) =
    openflights.routes.as[DataSet]

  private def withTableEnv[T](f: BatchTableEnvironment => T)(implicit flink: ExecutionEnvironment): T =
    f(TableEnvironment.getTableEnvironment(flink))

  // ---------------------------------------------------------
  // Example 1
  // ---------------------------------------------------------

  private[snippets] def `example 1(a)`(implicit flink: ExecutionEnvironment) = {
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

  private[snippets] def `example 1(b)`(implicit flink: ExecutionEnvironment) = {
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

  private[snippets] def `example 1(c)`(implicit flink: ExecutionEnvironment) =
    withTableEnv(tenv => {
      val berlinAirports = airports.toTable(tenv)
        .filter("latitude > 52.3")
        .filter("latitude < 52.6")
        .select('name, 'latitude, 'longitude)
        .filter("longitude > 13.2")
        .filter("longitude < 13.7")
        .toDataSet[Location[String]]

      berlinAirports.collect()
    })

  private[snippets] def `example 1(d)`(implicit flink: ExecutionEnvironment) =
    withTableEnv(tenv => {
      tenv.registerDataSet("airports", airports)

      val berlinAirports = tenv.sql(s"""
        |SELECT a.name,
        |       a.latitude,
        |       a.longitude
        |FROM   airports AS a
        |WHERE  a.latitude > 52.3
        |AND    a.latitude < 52.6
        |AND    a.longitude > 13.2
        |AND    a.longitude < 13.7
        """.stripMargin.trim
      ).toDataSet[Location[String]]

      berlinAirports.collect()
    })

  // ---------------------------------------------------------
  // Example 2
  // ---------------------------------------------------------

  private[snippets] def `example 2(a)`(implicit flink: ExecutionEnvironment) = {
    val xs = (airlines join routes)
      .where(al => Some(al.id))
      .equalTo(_.airlineID)
    val ys = (xs join airports)
      .where(_._2.srcID)
      .equalTo(ap => Some(ap.id))
    val zs = ys.map(_ match {
      case ((al, _), ap) =>
        (al.name, ap.country)
    })

    zs.collect()
  }

  private[snippets] def `example 2(b)`(implicit flink: ExecutionEnvironment) = {
    val xs = (airports join routes)
      .where(ap => Some(ap.id))
      .equalTo(_.srcID)
    val ys = (xs join airlines)
      .where(_._2.airlineID)
      .equalTo(al => Some(al.id))
    val zs = ys.map(_ match {
      case ((ap, _), al) =>
        (al.name, ap.country)
    })

    zs.collect()
  }

  private[snippets] def `example 2(c)`(implicit flink: ExecutionEnvironment) =
    withTableEnv(tenv => {
      tenv.registerDataSet("airports", airports)
      tenv.registerDataSet("airlines", airlines)
      tenv.registerDataSet("routes", routes)

      val zs = tenv.sql(s"""
        |SELECT al.name, ap.country
        |FROM   airports AS ap,
        |       routes   AS rt,
        |       airlines AS al
        |WHERE  rt.srcID     = ap.id
        |AND    rt.airlineID = al.id
        """.stripMargin.trim
      ).toDataSet[(String, String)]

      zs.collect()
    })

  // ---------------------------------------------------------
  // Example 3
  // ---------------------------------------------------------

  private[snippets] def `example 3(a)`(implicit flink: ExecutionEnvironment) = {
    val aggs = routes
      .groupBy(_.src)
      .reduceGroup(itr => {
        val v = itr.toSeq
        val k = v.head.src
        val x = v.count(_.airline == "AB")
        val y = v.count(_.airline == "LH")
        k -> (x, y)
      })

    aggs.collect()
  }

  private[snippets] def `example 3(b)`(implicit flink: ExecutionEnvironment) = {
    val aggs = routes
      .map(r =>
        r.src -> (AB(r.airline), LH(r.airline)))
      .groupBy(_._1)
      .reduce((u, v) => {
        val k = u._1
        val x = u._2._1 + v._2._1
        val y = u._2._2 + v._2._2
        k -> (x, y)
      })

    aggs.collect()
  }

  private val AB = (v: String) => if (v == "AB") 1L else 0L
  private val LH = (v: String) => if (v == "LH") 1L else 0L

  private[snippets] def `example 3(c)`(implicit flink: ExecutionEnvironment) =
    withTableEnv(tenv => {
      tenv.registerDataSet("routes", routes)

      val aggs = tenv.sql(s"""
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
      ).toDataSet[(String, Int, Int)]

      aggs.map(_ match { case (k, x, y) => k -> (x, y) }).collect()
    })

  // ---------------------------------------------------------
  // Example 4
  // ---------------------------------------------------------

  private[snippets] def `example 4(a)`(implicit flink: ExecutionEnvironment) = {
    val hs = hubs(50)
    val rs = reachableA(2)(hs)
    rs.collect()
  }

  private[snippets] def `example 4(b)`(implicit flink: ExecutionEnvironment) = {
    val hs = hubs(50)
    val rs = reachableB(2)(hs)
    rs.collect()
  }

  private def hubs(M: Int)(implicit flink: ExecutionEnvironment) = {
    val rs = ({
      routes.map(_.src -> 1L)
    } union {
      routes.map(_.dst -> 1L)
    }).groupBy("_1").sum("_2")
      .filter(_._2 < M)
      .map(_._1)

    rs.collect().toSet
  }

  private def reachableA(n: Int)(hubs: Set[String])(implicit flink: ExecutionEnvironment) = {
    val hubroutes = routes
      .filter(r => hubs(r.src) && hubs(r.dst))

    var paths = hubroutes
      .map(r => Path(r.src, r.dst))
    for (_ <- 0 until n) {
      val delta = (hubroutes join paths)
        .where(_.dst).equalTo(_.src)
        .map(x => Path(x._1.src, x._2.dst))
      paths = (paths union delta).distinct()
    }

    paths
  }

  private def reachableB(n: Int)(hubs: Set[String])(implicit flink: ExecutionEnvironment) = {
    val hubroutes = routes
      .filter(r => hubs(r.src) && hubs(r.dst))

    val paths = hubroutes
      .map(r => Path(r.src, r.dst))
      .iterate(n)(paths => {
        val delta = (hubroutes join paths)
          .where(_.dst).equalTo(_.src)
          .map(x => Path(x._1.src, x._2.dst))
        (paths union delta).distinct()
      })

    paths
  }
}
