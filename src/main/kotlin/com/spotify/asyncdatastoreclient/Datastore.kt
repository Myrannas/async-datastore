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

package com.spotify.datastoreclient

import com.spotify.asyncdatastoreclient.*

import java.io.Closeable

/**
 * The Datastore class encapsulates the Cloud Datastore API and handles
 * calling the datastore backend.
 *
 *
 * To create a Datastore object, call the static method `Datastore.create()`
 * passing configuration. A scheduled task will begin that automatically refreshes
 * the API access token for you.
 *
 *
 * Call `close()` to perform all necessary clean up.
 */
interface Datastore {

    /**
     * Start a new transaction.

     * The returned `TransactionResult` contains the transaction if the
     * request is successful.

     * @return the result of the transaction request.
     */
    suspend fun transaction(): TransactionResult

    /**
     * Rollback a given transaction.

     * You normally rollback a transaction in the event of d Datastore failure.

     * @param transaction the transaction.
     * *
     * @return the result of the rollback request.
     */
    suspend fun rollback(transaction: TransactionResult): RollbackResult

    /**
     * Commit a given transaction.

     * You normally manually commit a transaction after performing read-only
     * operations without mutations.

     * @param transaction the transaction.
     * *
     * @return the result of the commit request.
     */
    suspend fun commit(transaction: TransactionResult): MutationResult

    /**
     * Execute a allocate ids statement.

     * @param statement the statement to execute.
     * *
     * @return the result of the allocate ids request.
     */
    suspend fun execute(statement: AllocateIds, namespace: String?): AllocateIdsResult

    /**
     * Execute a multi-keyed query statement.

     * @param statements the statements to execute.
     * *
     * @return the result of the query request.
     */
    suspend fun execute(statements: Array<KeyQuery>, transaction: TransactionResult?, namespace: String?): QueryResult


    /**
     * Execute a keyed query statement in a given transaction.

     * @param statement the statement to execute.
     * *
     * @param transaction the transaction to execute the query.
     * *
     * @return the result of the query request.
     */
    suspend fun execute(statement: KeyQuery, transaction: TransactionResult?, namespace: String?): QueryResult

     /**
     * Execute a mutation query statement in a given transaction.

     * @param statement the statement to execute.
     * *
     * @param transaction the transaction to execute the query.
     * *
     * @return the result of the mutation request.
     */
    suspend fun execute(statement: MutationStatement, transaction: TransactionResult?, namespace: String?): MutationResult

    /**
     * Execute several mutations in a single transaction

     * @param statement the statement to execute.
     * *
     * @param transaction the transaction to execute the query.
     * *
     * @return the result of the mutation request.
     */
    suspend fun execute(statement: Array<MutationStatement>, transaction: TransactionResult?, namespace: String?): MutationResult

    /**
     * Execute a batch mutation query statement.

     * @param batch to execute.
     * @param transaction the transaction to execute the query.
     * @param namespace An optional namespace for this query
     * *
     * @return the result of the mutation request.
     */
    suspend fun execute(batch: Batch, transaction: TransactionResult?, namespace: String?): MutationResult

     /**
     * Execute a query statement in a given transaction.

     * @param statement the statement to execute.
     * *
     * @param txn the transaction to execute the query.
     * *
     * @return the result of the query request.
     */
    suspend fun execute(statement: Query, transaction: TransactionResult?, namespace: String?): QueryResult

    companion object {

        fun create(config: DatastoreConfig): Datastore {
            return DatastoreImpl(config)
        }
    }
}
