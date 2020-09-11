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
package io.stargate.coordinator;

import io.stargate.db.Persistence;
import io.stargate.filterchain.FilterChain;

public class Coordinator {
    private Persistence persistence;
    private FilterChain readFilterChain;
    private FilterChain writeFilterChain;

    public Persistence getPersistence() {
        return persistence;
    }

    public void setPersistence(Persistence persistence) {
        this.persistence = persistence;
    }

    public FilterChain getReadFilterChain() {
        return readFilterChain;
    }

    public void setReadFilterChain(FilterChain readFilterChain) {
        this.readFilterChain = readFilterChain;
    }

    public FilterChain getWriteFilterChain() {
        return writeFilterChain;
    }

    public void setWriteFilterChain(FilterChain writeFilterChain) {
        this.writeFilterChain = writeFilterChain;
    }

    public String read(String query) {
        System.out.println("Performing read in Coordinator");

        getReadFilterChain().doFilter();

        return "";
    }

    public String write() {
        System.out.println("Performing write in Coordinator");

        getWriteFilterChain().doFilter();

        return "";
    }
}
