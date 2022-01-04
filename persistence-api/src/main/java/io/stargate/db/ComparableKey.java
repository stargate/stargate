/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.db;

import java.util.Objects;

/**
 * Represents a row key (e.g., a partition key) decorated in a way that the objects can be compared
 * with each other such that the results of those comparisons are consistent with how the underlying
 * {@link Persistence} implementation compares those keys.
 *
 * @param <T> type of the persistence-specific decorated key class, irrelevant to users of this
 *     class.
 */
public final class ComparableKey<T extends Comparable<? super T>>
    implements Comparable<ComparableKey<?>> {

  private final Class<T> delegateClass;
  private final T delegate;

  public ComparableKey(Class<T> delegateClass, T delegate) {
    this.delegateClass = delegateClass;
    this.delegate = delegate;
  }

  @Override
  public int compareTo(ComparableKey<?> o) {
    if (equals(o)) {
      return 0;
    }

    if (delegateClass != o.delegateClass) {
      // This is not an expected use case. It is handled only for the sake of supporting the
      // contract of the compareTo() method whose signature in this class allows comparing objects
      // with different delegate classes.
      int cmp = delegateClass.getName().compareTo(o.delegateClass.getName());
      if (cmp != 0) {
        return cmp;
      }

      throw new IllegalStateException(
          "Mixing different delegate classes with the same name: " + delegateClass.getName());
    }

    @SuppressWarnings("unchecked")
    T other = (T) o.delegate;
    return delegate.compareTo(other);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ComparableKey<?> that = (ComparableKey<?>) o;
    return Objects.equals(delegate, that.delegate);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public String toString() {
    return "ComparableKey{" + delegate + '}';
  }
}
