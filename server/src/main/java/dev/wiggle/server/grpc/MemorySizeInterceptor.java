package dev.wiggle.server.grpc;

import com.google.protobuf.MessageLite;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Feeds the {@link MemoryGuard} the serialized size of every request and response message, and
 * releases a call's bytes when it ends -- so the guard always knows the total memory held by
 * in-flight request+response cycles. Applied to every RPC; the poll path is the only one that
 * acts on it.
 */
final class MemorySizeInterceptor implements ServerInterceptor {

    private final MemoryGuard guard;

    MemorySizeInterceptor(MemoryGuard guard) { this.guard = guard; }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        // Bytes this call currently holds; released exactly once when the call ends (getAndSet(0)
        // makes a second release a no-op, so close + cancel/complete can't double-release).
        AtomicLong callBytes = new AtomicLong();

        ServerCall<ReqT, RespT> counted = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override public void sendMessage(RespT message) {
                long sz = serializedSize(message);
                callBytes.addAndGet(sz);
                guard.add(sz);
                super.sendMessage(message);
            }
            @Override public void close(Status status, Metadata trailers) {
                guard.release(callBytes.getAndSet(0));
                super.close(status, trailers);
            }
        };

        ServerCall.Listener<ReqT> listener = next.startCall(counted, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
            @Override public void onMessage(ReqT message) {
                long sz = serializedSize(message);
                callBytes.addAndGet(sz);
                guard.add(sz);
                super.onMessage(message);
            }
            @Override public void onCancel() {
                guard.release(callBytes.getAndSet(0));
                super.onCancel();
            }
            @Override public void onComplete() {
                guard.release(callBytes.getAndSet(0));
                super.onComplete();
            }
        };
    }

    private static long serializedSize(Object message) {
        return message instanceof MessageLite m ? m.getSerializedSize() : 0L;
    }
}
