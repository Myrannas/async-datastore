/*
 * Copyright (c) 2011-2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.asyncdatastoreclient

/**
 * Represents query group by.

 * A group describes the property name by which query results are grouped.
 */
class Group internal constructor(private val name: String) {
    internal val pb: com.google.datastore.v1.PropertyReference
        get() = com.google.datastore.v1.PropertyReference.newBuilder()
                .setName(name)
                .build()
}
