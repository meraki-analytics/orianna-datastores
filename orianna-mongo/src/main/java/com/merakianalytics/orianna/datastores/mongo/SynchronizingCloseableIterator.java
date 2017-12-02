package com.merakianalytics.orianna.datastores.mongo;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.merakianalytics.datapipelines.iterators.CloseableIterator;
import com.merakianalytics.orianna.types.common.OriannaException;
import com.mongodb.CursorType;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.client.FindIterable;

public class SynchronizingCloseableIterator<T> implements CloseableIterator<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronizingCloseableIterator.class);
    private final AsyncBatchCursor<T> cursor;
    private final BlockingQueue<T> queue = new LinkedBlockingQueue<>();

    public SynchronizingCloseableIterator(final FindIterable<T> find) {
        final CompletableFuture<AsyncBatchCursor<T>> future = new CompletableFuture<>();
        find.noCursorTimeout(true).cursorType(CursorType.NonTailable).batchCursor((final AsyncBatchCursor<T> cursor, final Throwable exception) -> {
            if(exception != null) {
                future.completeExceptionally(exception);
            } else {
                future.complete(cursor);
            }
        });

        try {
            cursor = future.get();
        } catch(InterruptedException | ExecutionException e) {
            LOGGER.error("Error on MongoDB query!", e);
            throw new OriannaException("Error on MongoDB query!", e);
        }
    }

    @Override
    public void close() {
        cursor.close();
        queue.clear();
    }

    @Override
    public boolean hasNext() {
        if(!queue.isEmpty()) {
            return true;
        }

        final CompletableFuture<List<T>> future = new CompletableFuture<>();
        cursor.next((final List<T> results, final Throwable exception) -> {
            if(exception != null) {
                future.completeExceptionally(exception);
            } else {
                future.complete(results);
            }
        });

        try {
            final List<T> results = future.get();
            if(results == null) {
                return false;
            }

            for(final T result : results) {
                queue.add(result);
            }
            return true;
        } catch(InterruptedException | ExecutionException e) {
            LOGGER.error("Error on MongoDB query!", e);
            throw new OriannaException("Error on MongoDB query!", e);
        }
    }

    @Override
    public T next() {
        if(!hasNext()) {
            return null;
        }
        return queue.poll();
    }
}
