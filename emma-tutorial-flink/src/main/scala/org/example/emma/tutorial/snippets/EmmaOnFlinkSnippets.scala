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

import data.openflights._
import lib.openflights._

import org.apache.flink.api.scala._
import org.emmalanguage.api._

object EmmaOnFlinkSnippets {

  // ---------------------------------------------------------
  // Example 1
  // ---------------------------------------------------------

  private[snippets] def `example 1`(implicit flink: ExecutionEnvironment) = emma.onFlink {
    val berlinAirports = for {
      a <- airports
      if a.latitude > 52.3
      if a.latitude < 52.6
      if a.longitude > 13.2
      if a.longitude < 13.7
    } yield Location(
      a.name,
      a.latitude,
      a.longitude)

    berlinAirports.collect()
  }

  // ---------------------------------------------------------
  // Example 2
  // ---------------------------------------------------------

  private[snippets] def `example 2`(implicit flink: ExecutionEnvironment) = emma.onFlink {
    val zs = for {
      ap <- airports
      rt <- routes
      al <- airlines
      if rt.srcID == Some(ap.id)
      if rt.airlineID == Some(al.id)
    } yield (al.name, ap.country)

    zs.collect()
  }

  // ---------------------------------------------------------
  // Example 3
  // ---------------------------------------------------------

  private[snippets] def `example 3`(implicit flink: ExecutionEnvironment) = emma.onFlink {
    val aggs = for {
      Group(k, v) <- routes.groupBy(_.src)
    } yield {
      val x = v.count(_.airline == "AB")
      val y = v.count(_.airline == "LH")
      k -> (x, y)
    }

    aggs.collect()
  }

  // ---------------------------------------------------------
  // Example 4
  // ---------------------------------------------------------

  private[snippets] def `example 4`(implicit flink: ExecutionEnvironment) = emma.onFlink {
    val hs = hubs(50)
    val rs = reachable(2)(hs)
    rs.collect()
  }
}
