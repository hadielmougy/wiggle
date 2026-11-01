package com.wiggle.server.store;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

/**
 * The repositories of the transaction open on the calling thread. Each runs on whichever store
 * {@link ShardTransactions} bound for the thread, so a caller holds no transaction object; outside
 * a transaction a call is refused.
 */
public final class Repositories {

    private Repositories() {}

    private static final ShardRepository SHARDS = bound(ShardRepository.class);
    private static final AuthRepository AUTH = bound(AuthRepository.class);
    private static final SearchRepository SEARCH = bound(SearchRepository.class);
    private static final InstanceRepository INSTANCES = bound(InstanceRepository.class);
    private static final TokenRepository TOKENS = bound(TokenRepository.class);
    private static final ScheduleRepository SCHEDULES = bound(ScheduleRepository.class);
    private static final ClusterRepository CLUSTER = bound(ClusterRepository.class);
    private static final CompensationRepository COMPENSATION = bound(CompensationRepository.class);
    private static final EventRepository EVENTS = bound(EventRepository.class);
    private static final DynamicNodeRepository DYNAMIC_NODES = bound(DynamicNodeRepository.class);
    private static final GraphStore GRAPH = bound(GraphStore.class);
    private static final TaskLocks LOCKS = bound(TaskLocks.class);

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

    /** Whether the open write transaction rolls its writes back on a throw ({@link Tx#transactional()}). */
    public static boolean transactional() {
        return CurrentTx.requireWrite().transactional();
    }

    /** {@code type}, delegating each call to the transaction open on the calling thread. */
    private static <T> T bound(Class<T> type) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) ->
                switch (method.getName()) {
                    case "toString" -> type.getSimpleName() + "(bound)";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> {
                        try {
                            yield method.invoke(CurrentTx.require(), args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    }
                }));
    }
}
