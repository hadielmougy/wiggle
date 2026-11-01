// docs:begin quickstart
package demo;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;

import java.time.Duration;

public final class Hello {
    public record Greeting(String name, String message) {}

    interface GreetingSteps {
        Greeting greet(Greeting context);
    }

    static final class GreetingHandler implements GreetingSteps {
        @Override
        public Greeting greet(Greeting context) {
            return new Greeting(context.name(), "Hello, " + context.name());
        }
    }

    public static void main(String[] args) throws Exception {
        FlowSpec hello = FlowSpec.define("hello", 1, Greeting.class, GreetingSteps.class,
                (f, s) -> f.thenApply(s::greet));

        try (WiggleClient client = new WiggleClient("localhost:8080");
             Worker worker = new Worker(client, "hello-worker")
                     .registerHandler("hello", new GreetingHandler())) {
            client.register(hello);
            worker.start();

            String id = client.start(hello, new Greeting("Ada", ""));
            InstanceView result = client.awaitCompletion(id, Duration.ofSeconds(10));

            System.out.println(result.status());  // COMPLETED
            System.out.println(result.context()); // {name=Ada, message=Hello, Ada}
        }
    }
}
// docs:end quickstart



