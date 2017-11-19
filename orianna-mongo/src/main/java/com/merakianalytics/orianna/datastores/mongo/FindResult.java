package com.merakianalytics.orianna.datastores.mongo;

import com.merakianalytics.datapipelines.iterators.CloseableIterator;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

public class FindResult<T> implements CloseableIterator<T> {
    public FindResult(FindIterable<T> iterable) {
        this.cursor = iterable.iterator();
    }
    
    private final MongoCursor<T> cursor;
    
    @Override
    public boolean hasNext() {
        return cursor.hasNext();
    }

    @Override
    public T next() {
        return cursor.next();
    }

    @Override
    public void close() {
        cursor.close();
    }

}
