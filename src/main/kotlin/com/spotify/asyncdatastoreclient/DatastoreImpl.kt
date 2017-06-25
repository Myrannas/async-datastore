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

import com.google.common.base.Throwables
import com.google.datastore.v1.*
import com.spotify.datastoreclient.Datastore
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * The Datastore implementation.
 */
internal class DatastoreImpl(private val config: DatastoreConfig) : Datastore {

    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
    @Volatile
    private var accessToken: String? = null

    private var datastore: DatastoreGrpc.DatastoreFutureStub = DatastoreGrpc.newFutureStub(
            ManagedChannelBuilder
                    .forTarget(config.host)
                    .build()
    )

    init {
        if (config.credential != null) {
            // block while retrieving an access token for the first time
            refreshAccessToken()
        }
    }

    companion object {
        val log: Logger = LoggerFactory.getLogger(Datastore::class.java)
    }

    override fun close() {
        executor.shutdown()
    }

    fun refreshAccessToken() {
        val credential = config.credential

        if (credential != null) {
            val expiresIn = credential.expiresInSeconds

            // trigger refresh if token is about to expire
            if (credential.accessToken == null || expiresIn != null && expiresIn <= 60) {
                try {
                    credential.refreshToken()
                    val accessTokenLocal = credential.accessToken
                    if (accessTokenLocal != null) {
                        this.accessToken = accessTokenLocal
                    }
                } catch (e: IOException) {
                    log.error("Storage exception", Throwables.getRootCause(e))
                }
            }
        }
    }

    override suspend fun transaction(): TransactionResult/* = async(CommonPool)*/ {
        try {
            val request = BeginTransactionRequest.newBuilder().build()
            val result = datastore.beginTransaction(request).await()
            return TransactionResult.build(result)
        } catch (e: StatusException) {
            throw DatastoreException(e)
        }
    }

    override suspend fun rollback(transaction: TransactionResult): RollbackResult {
        val request = RollbackRequest.newBuilder()
                .setTransaction(transaction.transaction)
                .build()
        val result = datastore.rollback(request).await()
        return RollbackResult.build(result)
    }

    override suspend fun commit(transaction: TransactionResult): MutationResult {
        return execute(arrayOf<MutationStatement>(), transaction, null)
    }

    override suspend fun execute(statement: AllocateIds, namespace: String?): AllocateIdsResult {
        val request = AllocateIdsRequest
                .newBuilder()
                .addAllKeys(statement.getPb(namespace ?: config.namespace))
                .build()
        val result = datastore.allocateIds(request).await()
        return AllocateIdsResult.build(result)
    }

    override suspend fun execute(statement: KeyQuery, transaction: TransactionResult?, namespace: String?): QueryResult {
        return execute(arrayOf(statement), transaction, namespace)
    }

    override suspend fun execute(statements: Array<KeyQuery>, transaction: TransactionResult?, namespace: String?): QueryResult {
        val keys = statements.map { it.key.getPb(namespace ?: config.namespace) }
        val request = with(LookupRequest.newBuilder()) {
            addAllKeys(keys)
            if (transaction != null) {
                setReadOptions(ReadOptions.newBuilder().setTransaction(transaction.transaction))
            }
            build()
        }
        val result = datastore.lookup(request).await()
        return QueryResult.build(result)
    }

    override suspend fun execute(statement: Array<MutationStatement>, transaction: TransactionResult?, namespace: String?): MutationResult {
        val request = with(CommitRequest.newBuilder()) {
            addAllMutations(statement.map { it.getPb(namespace ?: config.namespace) })
            if (transaction != null) {
                setTransaction(transaction.transaction)
            } else {
                mode = CommitRequest.Mode.NON_TRANSACTIONAL
            }
            build()
        }

        val result = datastore.commit(request).await()
        return MutationResult.build(result)
    }

    override suspend fun execute(statement: MutationStatement, transaction: TransactionResult?, namespace: String?): MutationResult {
        return execute(arrayOf(statement), transaction, namespace)
    }

    override suspend fun execute(batch: Batch, transaction: TransactionResult?, namespace: String?): MutationResult {
        return execute(arrayOf<MutationStatement>(batch.getPb(namespace ?: config.namespace)), transaction, namespace)
    }

    @Override
    override suspend fun execute(statement: Query, transaction: TransactionResult?, namespace: String?): QueryResult {
        val queryNamespace = namespace ?: config.namespace

        val request = with(RunQueryRequest.newBuilder()) {
            query = statement.getPb(queryNamespace)
            partitionId = PartitionId.newBuilder().setNamespaceId(namespace).build()
            if (transaction != null) {
                setReadOptions(ReadOptions.newBuilder().setTransaction(transaction.transaction))
            }
            build()
        }

        val result = datastore.runQuery(request).await()
        return QueryResult.build(result)
    }
}