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
package lib

import data.openflights._

import org.emmalanguage.api._

@emma.lib
object openflights {
  def hubs(M: Int) = {
    val rs = for {
      Group(k, g) <- ({
        routes.map(_.src)
      } union {
        routes.map(_.dst)
      }).groupBy(x => x)
      if g.size < M
    } yield k

    rs.collect().toSet
  }

  def reachable(n: Int)(hubs: Set[String]) = {
    val hubroutes = routes
      .withFilter(r => hubs(r.src) && hubs(r.dst))

    var paths = hubroutes
      .map(r => Path(r.src, r.dst))
    for (_ <- 0 until n) {
      val delta = for {
        r <- hubroutes
        p <- paths if r.dst == p.src
      } yield Path(r.src, p.dst)
      paths = (paths union delta).distinct
    }

    paths
  }
}
