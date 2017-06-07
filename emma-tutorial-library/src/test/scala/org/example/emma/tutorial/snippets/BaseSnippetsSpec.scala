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

import org.emmalanguage.test.util._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FreeSpec
import org.scalatest.Matchers

import java.io.File

class BaseSnippetsSpec extends FreeSpec with Matchers with BeforeAndAfterAll {

  private val codegenDir = tempPath("codegen")

  override def beforeAll(): Unit = {
    new File(codegenDir).mkdirs()
    addToClasspath(new File(codegenDir))
    openflights.downloadOpenFlightsData()
  }

  override def afterAll(): Unit = {
    deleteRecursive(new File(codegenDir))
  }
}
