package com.wiggle.relay;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * The relay context. Every field is a pure function of the shipment id and the number of stages
 * applied, so any process can rebuild what a stage must receive without sharing state.
 */
public record Shipment(String id, String status, Party sender, Party recipient, List<Parcel> parcels,
                       Route route, Charges charges, Handoff handoff,
                       Map<String, String> tags, List<String> trail) {

    public record Geo(double lat, double lng) { }

    public record Party(String name, String email, String street, String city, String zip, Geo geo) { }

    public record Parcel(String ref, int weightGrams, int lengthMm, boolean fragile, String slot) { }

    public record Route(String originHub, String destinationHub, int distanceKm, List<String> legs) { }

    public record Charges(String currency, long baseMinor, long weightMinor, long fragileMinor,
                          long totalMinor, boolean paid) { }

    public record Handoff(String carrier, String trackingNo, int etaHours) { }

    private static final String[] CITIES = {"Springfield", "Zürich", "São Paulo", "Kraków", "Ōsaka", "Reykjavík"};
    private static final String[] NAMES = {"Ada Lovelace", "Zoë Ñúñez", "Łukasz Wiśniewski", "Björk Guðmundsdóttir",
            "Søren Kierkegaard", "Grace O'Hopper"};
    private static final String[] CARRIERS = {"ups", "dhl", "fedex", "postnl"};

    /** The shipment as the starter submits it. */
    public static Shipment seed(String id) {
        SplittableRandom rnd = random(id);
        List<Parcel> parcels = new ArrayList<>();
        int count = rnd.nextInt(1, 6);
        for (int i = 0; i < count; i++) {
            parcels.add(new Parcel(id + "/p" + i, rnd.nextInt(50, 30_000), rnd.nextInt(80, 1_200),
                    rnd.nextBoolean(), null));
        }
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("channel", rnd.nextBoolean() ? "web" : "api");
        tags.put("priority", String.valueOf(rnd.nextInt(1, 4)));
        tags.put("note", "line1\nline2 \"quoted\" \\ back\tslash");
        return new Shipment(id, "NEW", party(rnd), party(rnd), List.copyOf(parcels), null, null, null,
                tags, List.of());
    }

    /** Stage 1: canonicalises both parties. */
    public Shipment intake() {
        return new Shipment(id, "INTAKEN", canonical(sender), canonical(recipient), parcels, route, charges,
                handoff, tags, append("intake"));
    }

    /** Stage 2: plans the route between the parties' hubs. */
    public Shipment enrich() {
        SplittableRandom rnd = random(id + "#route");
        String origin = hub(sender);
        String destination = hub(recipient);
        int distance = rnd.nextInt(5, 4_000);
        List<String> legs = List.of(origin, "X-" + Math.floorMod((origin + destination).hashCode(), 97), destination);
        return new Shipment(id, "ROUTED", sender, recipient, parcels,
                new Route(origin, destination, distance, legs), charges, handoff, tags, append("enrich"));
    }

    /** Stage 3: prices the shipment from its parcels and route. */
    public Shipment price() {
        long base = 499 + route.distanceKm() * 3L;
        long weight = 0;
        long fragile = 0;
        for (Parcel p : parcels) {
            weight += (p.weightGrams() / 100L) * 7L;
            if (p.fragile()) fragile += 250;
        }
        return new Shipment(id, "PRICED", sender, recipient, parcels, route,
                new Charges("EUR", base, weight, fragile, base + weight + fragile, false), handoff, tags,
                append("price"));
    }

    /** Stage 4: reserves a hub slot for every parcel. */
    public Shipment reserve() {
        List<Parcel> slotted = new ArrayList<>();
        for (int i = 0; i < parcels.size(); i++) {
            Parcel p = parcels.get(i);
            slotted.add(new Parcel(p.ref(), p.weightGrams(), p.lengthMm(), p.fragile(),
                    route.originHub() + "-" + (p.fragile() ? "F" : "S") + i));
        }
        return new Shipment(id, "RESERVED", sender, recipient, List.copyOf(slotted), route, charges, handoff,
                tags, append("reserve"));
    }

    /** Stage 5: hands the shipment to a carrier. */
    public Shipment dispatch() {
        String carrier = CARRIERS[Math.floorMod(id.hashCode(), CARRIERS.length)];
        String tracking = carrier.toUpperCase() + Long.toHexString(random(id + "#track").nextLong() & 0xffffffffffL);
        return new Shipment(id, "DISPATCHED", sender, recipient, parcels, route, charges,
                new Handoff(carrier, tracking, 12 + route.distanceKm() / 80), tags, append("dispatch"));
    }

    /** Stage 6: marks the charges paid and closes the shipment. */
    public Shipment settle() {
        Map<String, String> settled = new LinkedHashMap<>(tags);
        settled.put("settledTotal", String.valueOf(charges.totalMinor()));
        return new Shipment(id, "SETTLED", sender, recipient, parcels, route,
                new Charges(charges.currency(), charges.baseMinor(), charges.weightMinor(), charges.fragileMinor(),
                        charges.totalMinor(), true),
                handoff, settled, append("settle"));
    }

    private List<String> append(String stage) {
        List<String> next = new ArrayList<>(trail);
        next.add(stage);
        return List.copyOf(next);
    }

    private static Party party(SplittableRandom rnd) {
        String name = NAMES[rnd.nextInt(NAMES.length)];
        return new Party(" " + name + " ", name.toLowerCase().replaceAll("[^a-z]", "") + "@Example.COM",
                rnd.nextInt(1, 999) + " Main St", CITIES[rnd.nextInt(CITIES.length)],
                String.format("%05d", rnd.nextInt(0, 100_000)),
                new Geo(rnd.nextInt(-89_000, 89_000) / 1000.0, rnd.nextInt(-179_000, 179_000) / 1000.0));
    }

    private static Party canonical(Party p) {
        return new Party(p.name().strip(), p.email().toLowerCase(), p.street(), p.city(), p.zip(), p.geo());
    }

    private static String hub(Party p) {
        return "HUB-" + p.zip().substring(0, 2);
    }

    private static SplittableRandom random(String key) {
        long h = 1125899906842597L;
        for (int i = 0; i < key.length(); i++) h = 31 * h + key.charAt(i);
        return new SplittableRandom(h);
    }
}
