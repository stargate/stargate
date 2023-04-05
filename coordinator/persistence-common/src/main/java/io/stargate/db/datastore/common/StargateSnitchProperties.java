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

package io.stargate.db.datastore.common;

/** Resolves the default rack and datacenter from the {@link System} properties. */
public class StargateSnitchProperties {

  private final String dc;
  private final String rack;

  public StargateSnitchProperties(String defaultDc, String defaultRack) {
    this.dc = System.getProperty("stargate.datacenter", defaultDc);
    this.rack = System.getProperty("stargate.rack", defaultRack);
  }

  public String getDc() {
    return dc;
  }

  public String getRack() {
    return rack;
  }
}
