package com.zemnitskiy.dpps.support;

import org.apache.ignite.cache.query.QueryCursor;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.List;

/** In-memory {@link QueryCursor} for unit tests. */
public final class ListQueryCursor<T> implements QueryCursor<T> {

    private final List<T> items;
    private boolean closed;

    public ListQueryCursor(List<T> items) {
        this.items = items;
    }

    public static <K, V> ListQueryCursor<Cache.Entry<K, V>> ofEntries(List<Cache.Entry<K, V>> entries) {
        return new ListQueryCursor<>(entries);
    }

    public static <K, V> Cache.Entry<K, V> entry(K key, V value) {
        return new Cache.Entry<>() {
            @Override
            public K getKey() {
                return key;
            }

            @Override
            public V getValue() {
                return value;
            }

            @Override
            public <T> T unwrap(Class<T> clazz) {
                throw new UnsupportedOperationException();
            }
        };
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public List<T> getAll() {
        return items;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public Iterator<T> iterator() {
        return items.iterator();
    }
}
