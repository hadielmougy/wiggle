package com.wiggle.client.flow;

import java.io.Serializable;
import java.util.List;

/**
 * The collection a {@link WiggleFlow#thenForEach} fans out over, named by a <em>reference to the
 * context's own accessor</em>: {@code Basket::items}. Nothing is invoked -- the reference is read for
 * the component's name, which is the key the engine looks the collection up under at run time.
 *
 * <p>This is a context <em>key</em>, not a step. No worker binds it and no node is added for it: the
 * collection is already in the persisted context when the instance arrives at the forEach, put there
 * by whatever step ran before. That is why the reference names the context type rather than the step
 * contract -- {@code Basket::items}, not {@code s::items}.
 *
 * <p>The key must match the persisted JSON exactly, and it does: a record component is written under
 * {@link java.lang.reflect.RecordComponent#getName() its own name}
 * (see {@code RecordMapper}), which is the name read off the reference. Unlike a step name, it is
 * neither folded nor overridable with {@link com.wiggle.client.worker.Handles @Handles} -- there is
 * no handler here to rename.
 *
 * <p>What the type argument buys: {@code E} is the element type, inferred by the compiler from the
 * accessor's own return type, so the body is typed without a {@code Class<E>} witness and a renamed
 * component carries the key with it.
 *
 * @param <A> the context type the collection is read from
 * @param <E> the element type -- each element is one branch's whole context
 */
@FunctionalInterface
public interface FlowItems<A, E> extends Serializable {

    List<E> apply(A ctx);
}
