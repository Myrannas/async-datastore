package com.spotify.asyncdatastoreclient

import com.google.common.util.concurrent.FutureCallback
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.suspendCoroutine

suspend fun <T> ListenableFuture<T>.await(): T =
        suspendCoroutine { cont: Continuation<T> ->
            Futures.addCallback(this, object : FutureCallback<T> {
                override fun onFailure(t: Throwable) {
                    cont.resumeWithException(t)
                }

                override fun onSuccess(result: T?) {
                    cont.resume(result!!)
                }
            })
        }
