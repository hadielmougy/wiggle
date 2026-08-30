package com.wiggle.server.coord;

/** A workflow definition registered in a namespace, tracked by content-hash version (R23). */
public record CoordDefinition(String namespace, String name, int version, String hash, long registeredAt) {}
