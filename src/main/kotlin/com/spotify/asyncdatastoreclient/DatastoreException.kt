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
 * Datastore exception.
 *
 *
 * All Datastore and client exceptions are encapsulated or wrapped
 * by `DatastoreException`.
 */
class DatastoreException : Exception {

    var statusCode: Int? = null
        private set

    constructor(message: String) : super(message) {}

    constructor(statusCode: Int, message: String) : super(message) {
        this.statusCode = statusCode
    }

    constructor(t: Throwable) : super(t) {}

    override fun initCause(cause: Throwable): Throwable {
        if (cause is DatastoreException) {
            statusCode = cause.statusCode
        }
        return this
    }
}
