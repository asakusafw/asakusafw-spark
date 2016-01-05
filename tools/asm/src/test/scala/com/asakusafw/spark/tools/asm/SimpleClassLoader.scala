/*
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.spark.tools.asm

import scala.collection.mutable

class SimpleClassLoader(parent: ClassLoader) extends ClassLoader(parent) {

  private val bytes = mutable.Map.empty[String, Array[Byte]]

  def put(name: String, bytes: => Array[Byte]) = {
    if (this.bytes.contains(name)) {
      this.bytes.get(name)
    } else {
      this.bytes.put(name, bytes)
    }
  }

  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    Option(findLoadedClass(name)).getOrElse {
      bytes.get(name).map { bytes =>
        defineClass(name, bytes, 0, bytes.length)
      }.getOrElse(super.loadClass(name, resolve))
    }
  }
}
