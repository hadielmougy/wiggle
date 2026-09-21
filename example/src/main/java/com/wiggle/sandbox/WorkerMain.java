package com.wiggle.sandbox;

import com.wiggle.client.DirectConnection;
import com.wiggle.client.WiggleConnection;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.*;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.RecordMapper;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerMain {

    /** Per-pod endpoints of the same cell (shared DB): workers alternate across them, starts randomize. */
    private static final List<String> TARGETS = List.of("127.0.0.1:18500", "127.0.0.1:18501");
    private static final int INSTANCES_TO_START = 1000;
    private static final int LINES_PER_ORDER = 3;
    private static final AtomicInteger SUCCESSFUL_INSTANCES = new AtomicInteger();
    private static final CountDownLatch COMPLETION_LATCH = new CountDownLatch(INSTANCES_TO_START);
    /** Order id -> the started order's JSON shape; every step deep-checks its context against this. */
    private static final Map<String, Map<String, Object>> EXPECTED_ORDERS = new ConcurrentHashMap<>();

    /** Every step runs on its own queue, named after it, served by its own dedicated worker. */
    private static final List<String> STEP_QUEUES = List.of(
            "validate", "inStock", "priceLine", "applyDiscount", "collectLines",
            "riskCheck", "authorise", "mergePayment", "capture",
            "reserveStock", "printLabel", "merge", "notify0");

    // -- the input model: three levels of records under the order, a list of lines, money as minor units --

    record Geo(double lat, double lng) { }

    record Address(String street, String city, String zip, Geo geo) { }

    record Customer(String name, String tier, Address address) { }

    record Card(String last4, int expiryMonth, int expiryYear) { }

    record Money(String currency, long amountMinor) { }

    record PaymentInfo(String method, Card card, Money amount) { }

    record Line(String sku, int qty, Money unit, Long lineTotal, Long discountMinor, Boolean priced) {
        Line priced(long total, long discount) { return new Line(sku, qty, unit, total, discount, true); }
    }

    record Order(String id, String status, String verificationToken,
                 Customer customer, PaymentInfo payment, List<Line> lines,
                 Long pricingTotal, Map<String, Object> audit) {

        Order stamp(String key, Object value) {
            Map<String, Object> next = audit == null ? new LinkedHashMap<>() : new LinkedHashMap<>(audit);
            next.put(key, value);
            return new Order(id, status, verificationToken, customer, payment, lines, pricingTotal, next);
        }

        Order priced(List<Line> pricedLines, long total) {
            return new Order(id, status, verificationToken, customer, payment,
                    List.copyOf(pricedLines), total, audit);
        }
    }

    interface OrderSteps {                       // the steps, as a contract
        Order validate(Order o);
        boolean inStock(Order o);
        Line priceLine(Line line);
        Line applyDiscount(Line line);
        Order collectLines(@Context Order base, List<Line> priced);
        Order riskCheck(Order o);
        Order authorise(Order o);
        Order mergePayment(@Context Order base, Order risk, Order auth);
        Order capture(Order o);
        CompensableActivity<Order, Order> reserveStock();
        Order printLabel(Order o);
        Order merge(@Context Order base, Order payment, Order shipping);
        CompensableActivity<Order, Order> notify0();
    }

    // -- deep validation: the started order must survive, bit for bit, inside every later context --

    static final class OrderValidator {

        static Order requireValid(Order order, String where) {
            if (order == null) fail(where, "order is null");
            Map<String, Object> expected = EXPECTED_ORDERS.get(order.id());
            if (expected == null) fail(where, "unknown order id '" + order.id() + "'");
            requireContains(expected, RecordMapper.toJson(order), "$", where);
            return order;
        }

        /** A forEach item is one line; its owning order rides in the frozen base context. */
        static Line requireValidLine(Line line, String where) {
            if (line == null) fail(where, "line is null");
            Order base = Step.base(Order.class);
            Map<String, Object> expectedOrder = EXPECTED_ORDERS.get(base.id());
            if (expectedOrder == null) fail(where, "unknown base order id '" + base.id() + "'");
            Object expectedLine = ((List<?>) expectedOrder.get("lines")).stream()
                    .filter(l -> line.sku().equals(((Map<?, ?>) l).get("sku")))
                    .findFirst().orElse(null);
            if (expectedLine == null) fail(where, "line sku '" + line.sku() + "' not in order '" + base.id() + "'");
            requireContains(expectedLine, RecordMapper.toJson(line), "$.lines[" + line.sku() + "]", where);
            return line;
        }

        /**
         * Structural containment, not equality: steps only ever ADD (an audit stamp, a line total),
         * so everything the start payload held must still be present and identical, at every depth.
         * A null in the expectation means the field was unset at start and a step may fill it.
         */
        private static void requireContains(Object expected, Object actual, String path, String where) {
            if (expected == null) return;
            switch (expected) {
                case Map<?, ?> em -> {
                    if (!(actual instanceof Map<?, ?> am)) fail(where, path + " is not an object: " + actual);
                    else em.forEach((k, v) -> requireContains(v, am.get(k), path + "." + k, where));
                }
                case List<?> el -> {
                    if (!(actual instanceof List<?> al) || al.size() != el.size()) {
                        fail(where, path + " list mismatch: " + actual);
                    } else {
                        for (int i = 0; i < el.size(); i++) {
                            requireContains(el.get(i), al.get(i), path + "[" + i + "]", where);
                        }
                    }
                }
                case Number en -> {
                    if (!(actual instanceof Number an) || en.doubleValue() != an.doubleValue()) {
                        fail(where, path + " expected=" + en + " actual=" + actual);
                    }
                }
                default -> {
                    if (!String.valueOf(expected).equals(String.valueOf(actual))) {
                        fail(where, path + " expected='" + expected + "' actual='" + actual + "'");
                    }
                }
            }
        }

        private static void fail(String where, String message) {
            System.out.println("invalid: " + where + " -> " + message);
            throw new IllegalArgumentException(where + ": " + message);
        }
    }

    private static Order randomOrder(String id) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        List<Line> lines = new ArrayList<>();
        for (int i = 0; i < LINES_PER_ORDER; i++) {
            lines.add(new Line("sku-" + id + "-" + i, rnd.nextInt(1, 9),
                    new Money("USD", rnd.nextLong(100, 99_999)), null, null, null));
        }
        return new Order(id,
                "NEW-" + UUID.randomUUID().toString().substring(0, 6),
                UUID.randomUUID().toString(),
                new Customer("cust-" + UUID.randomUUID().toString().substring(0, 8),
                        rnd.nextBoolean() ? "GOLD" : "STANDARD",
                        new Address(rnd.nextInt(1, 999) + " Main St", "Springfield",
                                String.valueOf(rnd.nextInt(10_000, 99_999)),
                                new Geo(rnd.nextInt(-89, 89) + 0.25, rnd.nextInt(-179, 179) + 0.75))),
                new PaymentInfo("CARD",
                        new Card(String.valueOf(rnd.nextInt(1000, 9999)), rnd.nextInt(1, 12), 2030),
                        new Money("USD", rnd.nextLong(1_000, 999_999))),
                List.copyOf(lines), null, Map.of());
    }

    public static void main(String[] args) throws InterruptedException {
        List<DirectConnection> conns = TARGETS.stream().map(WiggleConnection::direct).toList();
        List<com.wiggle.client.WiggleClient> clients = conns.stream().map(DirectConnection::client).toList();
        var client = clients.getFirst();
        FlowSpec spec = FlowSpec.define("test-flow", 8, RetryPolicy.fixed(5, Duration.ofSeconds(1)), Order.class, OrderSteps.class, (f, s) -> {
            var flow = f.execution(ExecutionMode.LOCAL_ASYNC);
            var checked = flow.apply(s::validate, "validate").thenFilter(s::inStock, "inStock");

            // dynamic fan-out over the order lines; each line hops queues mid-item
            var priced = checked.thenForEach(Order::lines, line ->
                            line.thenApply(s::priceLine, "priceLine")
                                    .thenApply(s::applyDiscount, RetryPolicy.exponential(5, Duration.ofMillis(100)), "applyDiscount"))
                    .combine(s::collectLines, "collectLines");

            // the payment arm is itself a fork: risk and authorisation in parallel, then capture
            var payment = Wiggle.allOf(
                            priced.thenApply(s::riskCheck, RetryPolicy.exponential(5, Duration.ofMillis(100)), "riskCheck"),
                            priced.thenApply(s::authorise, RetryPolicy.exponential(5, Duration.ofMillis(100)), "authorise"))
                    .combineWithContext(s::mergePayment, "mergePayment")
                    .thenApply(s::capture, "capture");

            var shipping = priced.thenApplyCompensable(s::reserveStock, "reserveStock")
                    .thenApply(s::printLabel, "printLabel");

            return Wiggle.allOf(payment, shipping)
                    .combineWithContext(s::merge, "merge")
                    .thenApplyCompensable(s::notify0, "notify0");
        });
        client.register(spec);

        List<Worker> workers = new ArrayList<>();
        for (int i = 0; i < STEP_QUEUES.size(); i++) {
            String step = STEP_QUEUES.get(i);
            Worker w = new Worker(clients.get(i % clients.size()), "w-" + step,
                    WorkerOptions.defaults()
                            .withConcurrency(32)
                            .withLocalBatchSize(100)
                            .withQueues(step)).registerHandler("test-flow", new StepHandler(step));
            w.start();
            workers.add(w);
        }

        for (int i = 0; i < INSTANCES_TO_START; i++) {
            Order order = randomOrder("ord-" + i);
            @SuppressWarnings("unchecked")
            Map<String, Object> expected = (Map<String, Object>) RecordMapper.toJson(order);
            EXPECTED_ORDERS.put(order.id(), expected);
            clients.get(ThreadLocalRandom.current().nextInt(clients.size())).start(spec, order);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                workers.forEach(Worker::close)));

        COMPLETION_LATCH.await();
        System.out.println("instances processed successfully: " + SUCCESSFUL_INSTANCES.get());
        workers.forEach(Worker::close);
    }

    /**
     * One handler serving exactly one step's queue. Every method asserts it is the assigned
     * step before doing the work, so a task landing on the wrong worker is a routing bug and
     * fails loudly rather than being quietly absorbed.
     */
    @ForFlow("test-flow")
    static class StepHandler implements OrderSteps {

        private final String serves;

        StepHandler(String serves) {
            this.serves = serves;
        }

        private void assigned(String step) {
            if (!serves.equals(step)) {
                throw new IllegalStateException("step '" + step + "' landed on the '" + serves + "' worker");
            }
        }

        @Override
        public Order validate(Order o) {
            assigned("validate");
            o = OrderValidator.requireValid(o, "validate@" + serves);
            return o.stamp("validatedBy", "w-" + serves);
        }

        @Override
        public boolean inStock(Order o) {
            assigned("inStock");
            OrderValidator.requireValid(o, "inStock@" + serves);
            return true;
        }

        @Override
        public Line priceLine(Line line) {
            assigned("priceLine");
            line = OrderValidator.requireValidLine(line, "priceLine@" + serves);
            Order base = Step.base(Order.class);
            if (!"USD".equals(line.unit().currency())
                    || !line.unit().currency().equals(base.payment().amount().currency())) {
                throw new IllegalStateException("currency mismatch on " + line.sku());
            }
            return line;   // pricing happens in applyDiscount, on its own queue
        }

        @Override
        public Line applyDiscount(Line line) {
            assigned("applyDiscount");
            line = OrderValidator.requireValidLine(line, "applyDiscount@" + serves);
            if (line.qty() % 4 == 0 && Step.attempt() <= 1) {
                throw new RuntimeException("discount service flapped");
            }
            long total = line.qty() * line.unit().amountMinor();
            long discount = "GOLD".equals(Step.base(Order.class).customer().tier()) ? total / 10 : 0;
            return line.priced(total - discount, discount);
        }

        @Override
        public Order collectLines(@Context Order base, List<Line> priced) {
            assigned("collectLines");
            base = OrderValidator.requireValid(base, "collectLines.base@" + serves);
            if (priced.size() != base.lines().size()) {
                throw new IllegalStateException("collectLines: expected " + base.lines().size()
                        + " priced lines, got " + priced.size());
            }
            long total = 0;
            for (Line line : priced) {
                OrderValidator.requireValidLine(line, "collectLines.line@" + serves);
                if (!Boolean.TRUE.equals(line.priced()) || line.lineTotal() == null) {
                    throw new IllegalStateException("collectLines: line " + line.sku() + " not priced");
                }
                total += line.lineTotal();
            }
            return base.priced(priced, total).stamp("linesPriced", priced.size());
        }

        @Override
        public Order riskCheck(Order o) {
            assigned("riskCheck");
            o = OrderValidator.requireValid(o, "riskCheck@" + serves);
            if (Step.attempt() <= 1) {
                throw new RuntimeException("risk service flapped");
            }
            return o.stamp("riskScore", o.payment().amount().amountMinor() % 100);
        }

        @Override
        public Order authorise(Order o) {
            assigned("authorise");
            o = OrderValidator.requireValid(o, "authorise@" + serves);
            return o.stamp("authorised", true);
        }

        @Override
        public Order mergePayment(@Context Order base, Order risk, Order auth) {
            assigned("mergePayment");
            base = OrderValidator.requireValid(base, "mergePayment.base@" + serves);
            OrderValidator.requireValid(risk, "mergePayment.risk@" + serves);
            OrderValidator.requireValid(auth, "mergePayment.auth@" + serves);
            return base.stamp("riskScore", risk.audit().get("riskScore"))
                    .stamp("authorised", auth.audit().get("authorised"));
        }

        @Override
        public Order capture(Order o) {
            assigned("capture");
            o = OrderValidator.requireValid(o, "capture@" + serves);
            if (!Boolean.TRUE.equals(o.audit().get("authorised")) || o.audit().get("riskScore") == null) {
                throw new IllegalStateException("capture before payment merge: " + o.audit());
            }
            return o.stamp("captured", true);
        }

        @Override
        public CompensableActivity<Order, Order> reserveStock() {
            return new CompensableActivity<>() {
                @Override
                public Order execute(Order ctx) {
                    assigned("reserveStock");
                    ctx = OrderValidator.requireValid(ctx, "reserveStock@" + serves);
                    if (Step.attempt() <= 2) {
                        throw new RuntimeException("invalid attempt");
                    }
                    return ctx.stamp("stockReserved", true);
                }

                @Override
                public void compensate(Compensation<Order, Order> comp) {
                }
            };
        }

        @Override
        public Order printLabel(Order o) {
            assigned("printLabel");
            o = OrderValidator.requireValid(o, "printLabel@" + serves);
            if (!Boolean.TRUE.equals(o.audit().get("stockReserved"))) {
                throw new IllegalStateException("label before stock reservation: " + o.audit());
            }
            return o.stamp("labelPrinted", true);
        }

        @Override
        public Order merge(@Context Order base, Order payment, Order shipping) {
            assigned("merge");
            base = OrderValidator.requireValid(base, "merge.base@" + serves);
            OrderValidator.requireValid(payment, "merge.payment@" + serves);
            OrderValidator.requireValid(shipping, "merge.shipping@" + serves);
            if (!Boolean.TRUE.equals(payment.audit().get("captured"))
                    || !Boolean.TRUE.equals(shipping.audit().get("labelPrinted"))) {
                throw new IllegalStateException("merge with unfinished arm: payment=" + payment.audit()
                        + " shipping=" + shipping.audit());
            }
            return base.stamp("captured", true).stamp("labelPrinted", true).stamp("merged", true);
        }

        @Override
        public CompensableActivity<Order, Order> notify0() {
            return new CompensableActivity<>() {
                @Override
                public void compensate(Compensation<Order, Order> comp) {
                }

                @Override
                public Order execute(Order ctx) {
                    assigned("notify0");
                    ctx = OrderValidator.requireValid(ctx, "notify0@" + serves);
                    System.out.println("instance done successfully: " + ctx.id());
                    SUCCESSFUL_INSTANCES.incrementAndGet();
                    COMPLETION_LATCH.countDown();
                    return ctx.stamp("notified", true);
                }
            };
        }
    }
}
