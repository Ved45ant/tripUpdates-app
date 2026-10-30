package com.example.tripUpdates.service.business;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Ticker;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Size-bounded cache with a different time-to-live for "positive" and "negative" values.
 *
 * Eviction policy: Caffeine's W-TinyLFU when the size bound is hit, plus per-entry expiry. Negative
 * results (not found / not allowed) get a much shorter TTL so a newly created or newly granted
 * record becomes visible quickly, while positive results are what protect the database on the hot path.
 */
public class TtlCache<K, V> {

    private final Cache<K, V> cache;
    /** One load per missing key at a time; waiters park on the future (cheap on a virtual thread). */
    private final ConcurrentHashMap<K, CompletableFuture<V>> inFlight = new ConcurrentHashMap<>();

    public TtlCache(long maxSize, Duration positiveTtl, Duration negativeTtl, Predicate<V> isNegative, Ticker ticker) {
        long positiveNanos = positiveTtl.toNanos();
        long negativeNanos = negativeTtl.toNanos();
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .ticker(ticker)
                .expireAfter(new Expiry<K, V>() {
                    @Override
                    public long expireAfterCreate(K key, V value, long currentTime) {
                        return isNegative.test(value) ? negativeNanos : positiveNanos;
                    }

                    @Override
                    public long expireAfterUpdate(K key, V value, long currentTime, long currentDuration) {
                        return isNegative.test(value) ? negativeNanos : positiveNanos;
                    }

                    @Override
                    public long expireAfterRead(K key, V value, long currentTime, long currentDuration) {
                        return currentDuration; // reads never extend the lifetime: staleness stays bounded
                    }
                })
                .build();
    }

    /**
     * Cached value, or the loader's result. The loader (a database query) deliberately runs OUTSIDE
     * Caffeine's {@code get(key, loader)}: that holds a ConcurrentHashMap bin monitor while loading, which pins a
     * virtual thread's carrier for the whole blocking query. With a few thousand clients connecting at once the
     * carriers were all pinned waiting for pooled connections held by threads that could no longer run.
     */
    public V get(K key, Function<K, V> loader) {
        V cached = cache.getIfPresent(key);
        if (cached != null) {
            return cached;
        }
        CompletableFuture<V> mine = new CompletableFuture<>();
        CompletableFuture<V> other = inFlight.putIfAbsent(key, mine);
        if (other != null) {
            try {
                return other.join();
            } catch (CompletionException e) {
                throw e.getCause() instanceof RuntimeException r ? r : e;
            }
        }
        try {
            V loaded = loader.apply(key);
            cache.put(key, loaded);
            mine.complete(loaded);
            return loaded;
        } catch (RuntimeException e) {
            mine.completeExceptionally(e);
            throw e;
        } finally {
            inFlight.remove(key, mine);
        }
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }

    public long size() {
        return cache.estimatedSize();
    }
}
