package com.wiggle.server.store;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.function.Function;

/**
 * The repositories of the transaction open on the calling thread. Each runs on whichever store
 * {@link ShardTransactions} bound for the thread, so a caller holds no transaction object; outside
 * a transaction a call is refused.
 */
public final class Repositories {

    private Repositories() {}

    private static final ShardRepository SHARDS = bound(ShardRepository.class, ReadRepositories::shards, WriteRepositories::shards);
    private static final AuthRepository AUTH = bound(AuthRepository.class, ReadRepositories::auth, WriteRepositories::auth);
    private static final SearchRepository SEARCH = bound(SearchRepository.class, ReadRepositories::search, WriteRepositories::search);
    private static final InstanceRepository INSTANCES = bound(InstanceRepository.class, ReadRepositories::instances, WriteRepositories::instances);
    private static final TokenRepository TOKENS = bound(TokenRepository.class, ReadRepositories::tokens, WriteRepositories::tokens);
    private static final ScheduleRepository SCHEDULES = bound(ScheduleRepository.class, ReadRepositories::schedules, WriteRepositories::schedules);
    private static final ClusterRepository CLUSTER = bound(ClusterRepository.class, ReadRepositories::cluster, WriteRepositories::cluster);
    private static final CompensationRepository COMPENSATION = bound(CompensationRepository.class, ReadRepositories::compensation, WriteRepositories::compensation);
    private static final EventRepository EVENTS = bound(EventRepository.class, ReadRepositories::events, WriteRepositories::events);
    private static final DynamicNodeRepository DYNAMIC_NODES = bound(DynamicNodeRepository.class, null, WriteRepositories::dynamicNodes);
    private static final GraphStore GRAPH = bound(GraphStore.class, ReadRepositories::graph, WriteRepositories::graph);
    private static final TaskLocks LOCKS = bound(TaskLocks.class, null, WriteRepositories::locks);

    public static ShardRepository shards() { return SHARDS; }
    public static AuthRepository auth() { return AUTH; }
    public static SearchRepository search() { return SEARCH; }
    public static InstanceRepository instances() { return INSTANCES; }
    public static TokenRepository tokens() { return TOKENS; }
    public static ScheduleRepository schedules() { return SCHEDULES; }
    public static ClusterRepository cluster() { return CLUSTER; }
    public static CompensationRepository compensation() { return COMPENSATION; }
    public static EventRepository events() { return EVENTS; }
    public static DynamicNodeRepository dynamicNodes() { return DYNAMIC_NODES; }
    public static GraphStore graph() { return GRAPH; }
    public static TaskLocks locks() { return LOCKS; }

    /** Holds the open write transaction's token and instance writes back until it commits ({@link WriteRepositories#bufferWrites()}). */
    public static void bufferWrites() {
        CurrentTx.requireWrite().bufferWrites();
    }

    /**
     * {@code type}, delegating each call to the transaction open on the calling thread: a method of a
     * {@code *Reads} interface to {@code reads} of its repositories, any other to {@code writes} of them,
     * which only repositories that can write have.
     */
    private static <T> T bound(Class<T> type, Function<ReadRepositories, Object> reads,
                               Function<WriteRepositories, Object> writes) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) ->
                switch (method.getName()) {
                    case "toString" -> type.getSimpleName() + "(bound)";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> {
                        ReadRepositories open = CurrentTx.require();
                        Object target;
                        if (reads != null && method.getDeclaringClass().getSimpleName().endsWith("Reads")) {
                            target = reads.apply(open);
                        } else if (open instanceof WriteRepositories w) {
                            target = writes.apply(w);
                        } else {
                            throw new IllegalStateException(type.getSimpleName() + "." + method.getName()
                                    + " cannot run in a read-only transaction");
                        }
                        try {
                            yield method.invoke(target, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    }
                }));
    }
}
