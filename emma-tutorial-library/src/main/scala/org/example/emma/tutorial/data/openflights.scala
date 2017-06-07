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
package data

import org.emmalanguage.api._

import sys.process._

import java.net.URI
import java.nio.file.Paths

@emma.lib
object openflights {
  
  type ID = Int

  case class Airport
  (
    //@formatter:off
    id        : ID,
    name      : String,
    city      : Option[String],
    country   : String,
    iata      : Option[String],
    icao      : Option[String],
    latitude  : Double,
    longitude : Double,
    utcOffset : Double,
    dsTime    : Option[Double],
    timeZone  : Option[String],
    tpe       : Option[String],
    source    : String
    //@formatter:on
  )

  case class Airline
  (
    //@formatter:off
    id        : ID,
    name      : String,
    alias     : Option[String],
    iata      : Option[String],
    icao      : Option[String],
    callsign  : Option[String],
    country   : Option[String],
    active    : String
    //@formatter:on
  )

  case class Route
  (
    //@formatter:off
    airline   : String,
    airlineID : Option[ID],
    src       : String,
    srcID     : Option[ID],
    dst       : String,
    dstID     : Option[ID],
    isShared  : Option[String],
    stops     : Int,
    equipment : Option[String]
    //@formatter:on
  )

  case class Location[A]
  (
    //@formatter:off
    name      : A,
    latitude  : Double,
    longitude : Double
    //@formatter:on
  )

  case class Path
  (
    //@formatter:off
    src       : String,
    dst       : String
    //@formatter:on
  )

  val repo = new URI("https://raw.githubusercontent.com/emmalanguage/openflights/master/data/")
  val base = Paths.get(sys.props("java.io.tmpdir"), "openflights")
  val file = (name: String) => base.resolve(name)
  val frmt = CSV(delimiter = ',')

  val downloadOpenFlightsData = () => {
    if (!base.toFile.exists()) {
      println(s"Creating `openflights` folder at $base.")
      base.toFile.mkdirs()
    }
    for {
      name <- Seq("airports.dat", "airlines.dat", "routes.dat")
      src = repo.resolve(name).toURL
      dst = file(name).toFile
      if !dst.exists()
    } {
      println(s"Downloading `$src` under `$dst`.")
      (src #> dst).!!
    }
  }

  def airports: DataBag[Airport] = DataBag.readCSV[Airport](file("airports.dat").toString, frmt)

  def airlines: DataBag[Airline] = DataBag.readCSV[Airline](file("airlines.dat").toString, frmt)

  def routes: DataBag[Route] = DataBag.readCSV[Route](file("routes.dat").toString, frmt)
}
