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

import com.google.api.client.auth.oauth2.Credential
import com.google.common.collect.ImmutableList

import com.google.common.base.MoreObjects.firstNonNull

val SCOPES: List<String> = ImmutableList.of(
        "https://www.googleapis.com/auth/datastore")

private val DEFAULT_CONNECT_TIMEOUT = 5000
private val DEFAULT_MAX_CONNECTIONS = -1
private val DEFAULT_REQUEST_TIMEOUTS = 5000
private val DEFAULT_REQUEST_RETRIES = 5
private val DEFAULT_HOST = "https://datastore.googleapis.com"
private val DEFAULT_VERSION = "v1"

/**
 * Datastore configuration class used to initialise `Datastore`.
 *
 *
 * Use `DatastoreConfig.builder()` build a config object by supplying
 * options such as `connectTimeout()` and `project()`.
 *
 *
 * Defaults are assigned for any options not provided.
 */
class DatastoreConfig private constructor(val project: String,
                                          val namespace: String,
                                          val credential: Credential?,
                                          val connectTimeout: Int = DEFAULT_CONNECT_TIMEOUT,
                                          val maxConnections: Int = DEFAULT_MAX_CONNECTIONS,
                                          val requestTimeout: Int = DEFAULT_REQUEST_TIMEOUTS,
                                          val requestRetry: Int = DEFAULT_REQUEST_RETRIES,
                                          val host: String = DEFAULT_HOST,
                                          val version: String = DEFAULT_VERSION)

