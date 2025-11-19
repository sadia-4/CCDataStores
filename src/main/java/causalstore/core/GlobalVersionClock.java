package causalstore.core;

import java.util.concurrent.atomic.AtomicLong;

public final class GlobalVersionClock {
    private static final AtomicLong counter = new AtomicLong();

    private GlobalVersionClock() {
    }

    public static long next() {
        return counter.incrementAndGet();
    }
}
