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

import com.google.datastore.v1.Mutation
import com.google.datastore.v1.Mutation.newBuilder

/**
 * A delete statement.

 * Delete a single entity based on its `Key`.
 */
class Delete internal constructor(key: Key) : KeyedStatement(key), MutationStatement {
    override fun getPb(namespace: String): Mutation {
        val mutationKey = key.getPb(namespace)
        return newBuilder().setDelete(mutationKey).build()
    }
}
