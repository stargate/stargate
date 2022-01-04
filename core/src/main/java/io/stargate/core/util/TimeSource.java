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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.core.util;

/** A simple interface capable of providing current time timestamps in millis and micros. */
public interface TimeSource {

  /** Implementation of the {@link TimeSource} based on {@link System} time. */
  TimeSource SYSTEM =
      new TimeSource() {

        @Override
        public long currentTimeMicros() {
          return currentTimeMillis() * 1000;
        }

        @Override
        public long currentTimeMillis() {
          return System.currentTimeMillis();
        }
      };

  /** Microseconds since {@link java.lang.System#currentTimeMillis() epoch}. */
  long currentTimeMicros();

  /** @return the current time in milliseconds */
  long currentTimeMillis();
}
