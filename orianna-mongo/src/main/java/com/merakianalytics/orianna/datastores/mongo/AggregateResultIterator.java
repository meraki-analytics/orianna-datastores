package com.merakianalytics.orianna.datastores.mongo;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.merakianalytics.datapipelines.iterators.CloseableIterator;
import com.merakianalytics.orianna.types.common.OriannaException;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.client.AggregateIterable;

public class AggregateResultIterator<T, B extends BsonValue, I> implements CloseableIterator<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregateResultIterator.class);

    private final Function<B, I> converter;
    private final AsyncBatchCursor<T> cursor;
    private boolean empty = false;
    private final Function<T, I> index;
    private final Iterator<B> order;
    private final BlockingQueue<T> queue = new LinkedBlockingQueue<>();;

    public AggregateResultIterator(final AggregateIterable<T> result, final Iterator<B> order, final Function<T, I> index, final Function<B, I> converter) {
        final CompletableFuture<AsyncBatchCursor<T>> future = new CompletableFuture<>();
        result.batchCursor((final AsyncBatchCursor<T> cursor, final Throwable exception) -> {
            if(exception != null) {
                future.completeExceptionally(exception);
            } else {
                future.complete(cursor);
            }
        });

        this.order = order;
        this.index = index;
        this.converter = converter;

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
        final boolean hasNext = order.hasNext();
        if(!hasNext) {
            close();
        }
        return hasNext;
    }

    @Override
    public T next() {
        if(!hasNext()) {
            return null;
        }

        final I nextIndex = converter.apply(order.next());
        if(nextIndex == null) {
            return null;
        }

        if(queue.isEmpty() && !empty) {
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
                if(results == null || results.isEmpty()) {
                    empty = true;
                    close();
                    return null;
                }

                for(final T result : results) {
                    queue.add(result);
                }
            } catch(InterruptedException | ExecutionException e) {
                LOGGER.error("Error on MongoDB query!", e);
                throw new OriannaException("Error on MongoDB query!", e);
            }
        }

        if(!queue.isEmpty()) {
            if(nextIndex.equals(index.apply(queue.peek()))) {
                return queue.poll();
            }
        }
        return null;
    }
}
