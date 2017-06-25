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

import com.google.api.client.util.Lists
import com.google.datastore.v1.Mutation
import java.util.stream.Collectors

/**
 * A batch mutation statement.

 * Will batch a collection of mutation operations into a single operation.
 */
class Batch {

    private val statements = Lists.newArrayList<MutationStatement>()

    /**
     * Adds a statement to this batch.

     * @param statement the mutation statement to add.
     * *
     * @return this batch statement.
     */
    fun add(statement: MutationStatement): Batch {
        statements.add(statement)
        return this
    }

    fun getPb(namespace: String) = statements.map { m -> m.getPb(namespace) }

}
