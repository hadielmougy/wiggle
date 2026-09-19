package com.wiggle.server.coord;

/** A workflow definition registered in a namespace, tracked by its declared version and a hash
 *  of the submitted JSON (R23). */
public record CoordDefinition(String namespace, String name, int version, String hash, long registeredAt) {}
