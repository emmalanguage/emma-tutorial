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

import org.emmalanguage.SparkAware

import scala.util.hashing.MurmurHash3

class EmmaOnSparkSnippetsSpec extends BaseSnippetsSpec with SparkAware {

  import EmmaOnSparkSnippets._

  // ---------------------------------------------------------
  // Example 1
  // ---------------------------------------------------------

  "example 1" in withDefaultSparkSession(spark => {
    val res = `example 1`(spark)
    val act = MurmurHash3.unorderedHash(res)
    val exp = -1658157757
    act shouldEqual exp
  })

  // ---------------------------------------------------------
  // Example 2
  // ---------------------------------------------------------

  "example 2" in withDefaultSparkSession(spark => {
    val res = `example 2`(spark)
    val act = MurmurHash3.unorderedHash(res)
    val exp = -1314915106
    act shouldEqual exp
  })

  // ---------------------------------------------------------
  // Example 3
  // ---------------------------------------------------------

  "example 3" in withDefaultSparkSession(spark => {
    val res = `example 3`(spark)
    val act = MurmurHash3.unorderedHash(res)
    val exp = 2070458786
    act shouldEqual exp
  })

  // ---------------------------------------------------------
  // Example 4
  // ---------------------------------------------------------

  "example 4" in withDefaultSparkSession(spark => {
    val res = `example 4`(spark)
    val act = MurmurHash3.unorderedHash(res)
    val exp = 1281597418
    act shouldEqual exp
  })
}
