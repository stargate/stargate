/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stargate.utils;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Simple helper methods to work with Java 8 Streams.
 *
 * <p>Mainly exists to reduce the verbosity of a few operations.
 */
public abstract class Streams {
  private Streams() {}

  /**
   * Creates a sequential stream from an iterable.
   *
   * <p>This is simply a shortcut for {@code StreamSupport.stream(iterable.splitIterator(), false)}.
   *
   * @param iterable the iterable describing the stream elements.
   * @return a new sequential stream on {@code iterable}.
   */
  public static <T> Stream<T> of(Iterable<T> iterable) {
    return StreamSupport.stream(iterable.spliterator(), false);
  }

  public static <T> Stream<T> of(Iterator<T> iterator) {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
  }
}
