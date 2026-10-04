package com.wiggle.console;

import com.wiggle.client.DirectConnection;
import com.wiggle.client.WiggleConnection;
import com.wiggle.core.Tls;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.auth.AuthCache;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Accounts and roles managed in the portal, on the auth shard: who may create them, who may sign in
 * with them, what a role lets them do, and what an account can change about itself.
 */
class ConsoleUsersTest {

    private static ServerConfig config() {
        return new ServerConfig(0, "users-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static HttpResponse<String> send(HttpClient http, String method, String url, String basic, String body)
            throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json");
        if (basic != null) b.header("Authorization", "Basic " + Base64.getEncoder().encodeToString(basic.getBytes()));
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String json(String... kv) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < kv.length; i += 2) {
            if (i > 0) sb.append(',');
            sb.append('"').append(kv[i]).append("\":\"").append(kv[i + 1]).append('"');
        }
        return sb.append('}').toString();
    }

    /** A portal over one server's accounts, with an optional built-in admin password. */
    private interface Body { void run(String base, HttpClient http) throws Exception; }

    private static ConsoleServer portal(WiggleServer server, String builtinPassword) {
        AuthCache cache = new AuthCache(server.accounts(), 30_000, System::currentTimeMillis).start(50);
        ConsoleAuth auth = new ConsoleAuth("admin", builtinPassword, "viewer", null, false, server.accounts(), cache);
        return new ConsoleServer(new EngineDashboardData(server.engine(), server.cluster()), auth, 0,
                Tls.Options.DISABLED).start();
    }

    private static void withPortal(String builtinPassword, Body body) throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             ConsoleServer console = portal(server, builtinPassword)) {
            body.run("http://localhost:" + console.port(), HttpClient.newHttpClient());
        }
    }

    private static String cookieOf(HttpResponse<String> login) {
        String set = login.headers().firstValue("Set-Cookie").orElseThrow();
        return set.substring(0, set.indexOf(';'));
    }

    private static HttpResponse<String> withCookie(HttpClient http, String url, String cookie) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).header("Cookie", cookie).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test @DisplayName("an admin creates accounts with a role; they sign in, and what their role allows holds")
    void createAndSignIn() throws Exception {
        withPortal("root-pass", (base, http) -> {
            assertEquals(200, send(http, "POST", base + "/api/users", "admin:root-pass",
                    json("user", "dana", "password", "dana-password", "role", "admin")).statusCode());
            assertEquals(200, send(http, "POST", base + "/api/users", "admin:root-pass",
                    json("user", "rey", "password", "rey-password", "role", "viewer")).statusCode());

            String list = send(http, "GET", base + "/api/users", "admin:root-pass", null).body();
            assertTrue(list.contains("\"dana\"") && list.contains("\"rey\""), list);
            assertTrue(list.contains("\"builtin\":true"), "the environment's own account is listed as built-in");

            assertEquals(200, send(http, "GET", base + "/api/instances", "dana:dana-password", null).statusCode());
            assertEquals(200, send(http, "GET", base + "/api/instances", "rey:rey-password", null).statusCode());
            assertEquals(403, send(http, "POST", base + "/api/schedules", "rey:rey-password",
                    json("workflow", "nope")).statusCode(), "a viewer may not write");
            assertEquals(403, send(http, "GET", base + "/api/users", "rey:rey-password", null).statusCode(),
                    "nor see who else can sign in");
            assertEquals(401, send(http, "GET", base + "/api/instances", "dana:wrong", null).statusCode());

            String me = send(http, "GET", base + "/api/auth", "dana:dana-password", null).body();
            assertTrue(me.contains("\"user\":\"dana\""), me);
            assertTrue(me.contains("\"role\":\"admin\""), me);
            assertTrue(me.contains("\"permissions\":[\"*\"]"), me);
            assertTrue(me.contains("\"canChangePassword\":true"), me);
        });
    }

    @Test @DisplayName("a session opened on one portal node is served by another, and a logout ends it on both")
    void sessionsAreShared() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             ConsoleServer one = portal(server, "root-pass");
             ConsoleServer two = portal(server, "root-pass")) {
            HttpClient http = HttpClient.newHttpClient();
            String a = "http://localhost:" + one.port(), b = "http://localhost:" + two.port();
            send(http, "POST", a + "/api/users", "admin:root-pass",
                    json("user", "dana", "password", "dana-password", "role", "viewer"));
            HttpResponse<String> login = send(http, "POST", a + "/api/login", null,
                    json("user", "dana", "password", "dana-password"));
            assertEquals(200, login.statusCode());
            String cookie = cookieOf(login);
            assertEquals(200, withCookie(http, b + "/api/instances", cookie).statusCode(),
                    "no sticky session: the other node knows it");

            http.send(HttpRequest.newBuilder(URI.create(a + "/logout")).header("Cookie", cookie).build(),
                    HttpResponse.BodyHandlers.ofString());
            Thread.sleep(300);
            assertEquals(401, withCookie(http, b + "/api/instances", cookie).statusCode(),
                    "the other node dropped it at its next poll");
        }
    }

    @Test @DisplayName("a role grants named permissions, scoped to a workflow where it says so")
    void scopedRoles() throws Exception {
        withPortal("root-pass", (base, http) -> {
            assertEquals(200, send(http, "POST", base + "/api/roles", "admin:root-pass",
                    "{\"name\":\"orders-ops\",\"permissions\":[\"read\",\"schedule.write:orders\"]}").statusCode());
            assertEquals(400, send(http, "POST", base + "/api/roles", "admin:root-pass",
                    "{\"name\":\"bad\",\"permissions\":[\"instance.delete\"]}").statusCode());
            assertEquals(200, send(http, "POST", base + "/api/users", "admin:root-pass",
                    "{\"user\":\"sam\",\"password\":\"sam-password\",\"roles\":[\"orders-ops\"]}").statusCode());

            String roles = send(http, "GET", base + "/api/roles", "admin:root-pass", null).body();
            assertTrue(roles.contains("\"orders-ops\"") && roles.contains("schedule.write:orders"), roles);

            assertEquals(403, send(http, "POST", base + "/api/schedules", "sam:sam-password",
                    "{\"workflow\":\"billing\",\"everyMillis\":60000}").statusCode(), "another workflow is out of scope");
            int inScope = send(http, "POST", base + "/api/schedules", "sam:sam-password",
                    "{\"workflow\":\"orders\",\"everyMillis\":60000}").statusCode();
            assertTrue(inScope != 403, "in scope, the engine decides (orders is not registered): " + inScope);
            assertEquals(403, send(http, "POST", base + "/api/instances/x/cancel", "sam:sam-password", null).statusCode(),
                    "no cancel permission at all");
            assertEquals(403, send(http, "GET", base + "/api/roles", "sam:sam-password", null).statusCode());

            String audit = send(http, "GET", base + "/api/audit", "admin:root-pass", null).body();
            Matcher m = Pattern.compile("\"action\":\"role.put\",\"target\":\"orders-ops\"").matcher(audit);
            assertTrue(m.find(), audit);
            assertTrue(audit.contains("\"actor\":\"admin\""), "who made the change is recorded: " + audit);
        });
    }

    @Test @DisplayName("first run: everything leads to the setup screen until the admin password is set, then never again")
    void firstRunSetup() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             ConsoleServer one = portal(server, null)) {
            HttpClient http = HttpClient.newHttpClient();
            String base = "http://localhost:" + one.port();
            HttpResponse<String> root = http.send(HttpRequest.newBuilder(URI.create(base + "/")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(302, root.statusCode());
            assertEquals("/setup", root.headers().firstValue("Location").orElseThrow());
            HttpResponse<String> api = send(http, "GET", base + "/api/instances", null, null);
            assertEquals(401, api.statusCode(), "no API answers before setup");
            assertTrue(send(http, "GET", base + "/api/auth", null, null).body().contains("\"setupRequired\":true"));
            assertTrue(send(http, "GET", base + "/setup", null, null).body().contains("value=\"admin\""),
                    "the setup screen names the admin account it creates");

            assertEquals(400, send(http, "POST", base + "/api/setup", null, "{\"password\":\"short\"}").statusCode());
            HttpResponse<String> set = send(http, "POST", base + "/api/setup", null, "{\"password\":\"first-run-pass\"}");
            assertEquals(200, set.statusCode(), set.body());
            assertEquals(200, withCookie(http, base + "/api/instances", cookieOf(set)).statusCode(),
                    "setting the password signs the admin in");

            assertEquals(409, send(http, "POST", base + "/api/setup", null, "{\"password\":\"take-over-pass\"}").statusCode(),
                    "once set up, the setup endpoint cannot be used to take the portal over");
            assertEquals(401, send(http, "GET", base + "/api/instances", "admin:take-over-pass", null).statusCode());
            assertEquals(200, send(http, "GET", base + "/api/instances", "admin:first-run-pass", null).statusCode());
            HttpResponse<String> setupAgain = http.send(HttpRequest.newBuilder(URI.create(base + "/setup")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals("/login", setupAgain.headers().firstValue("Location").orElseThrow());

            assertTrue(server.accounts().account("admin").orElseThrow().permissions().contains("*"),
                    "the admin is an account in the database, with the admin role");
            try (ConsoleServer two = portal(server, null)) {
                assertEquals(200, send(http, "GET", "http://localhost:" + two.port() + "/api/instances",
                        "admin:first-run-pass", null).statusCode(), "every portal node knows it");
            }
        }
    }

    @Test @DisplayName("a password in the environment skips setup: the built-in admin signs in directly")
    void builtinAdminSkipsSetup() throws Exception {
        withPortal("env-password", (base, http) -> {
            assertTrue(send(http, "GET", base + "/api/auth", null, null).body().contains("\"setupRequired\":false"));
            assertEquals(200, send(http, "GET", base + "/api/instances", "admin:env-password", null).statusCode());
            assertEquals(409, send(http, "POST", base + "/api/setup", null, "{\"password\":\"take-over-pass\"}").statusCode());
        });
    }

    @Test @DisplayName("an admin issues an API key once, lists credentials without it, and deletes them")
    void credentials() throws Exception {
        withPortal("root-pass", (base, http) -> {
            HttpResponse<String> created = send(http, "POST", base + "/api/credentials", "admin:root-pass",
                    "{\"id\":\"orders-worker\",\"kind\":\"api-key\",\"role\":\"viewer\"}");
            assertEquals(200, created.statusCode(), created.body());
            Matcher key = Pattern.compile("\"key\":\"(wgk_[^\"]+)\"").matcher(created.body());
            assertTrue(key.find(), created.body());
            assertEquals(200, send(http, "POST", base + "/api/credentials", "admin:root-pass",
                    "{\"id\":\"orders-cert\",\"kind\":\"mtls\",\"subject\":\"CN=orders\",\"role\":\"admin\"}").statusCode());

            String list = send(http, "GET", base + "/api/credentials", "admin:root-pass", null).body();
            assertTrue(list.contains("orders-worker") && list.contains("CN=orders"), list);
            assertFalse(list.contains(key.group(1)), "the key is never listed");
            assertFalse(list.contains("keyHash"), "nor its hash");

            assertEquals(200, send(http, "DELETE", base + "/api/credentials/orders-worker", "admin:root-pass", null).statusCode());
            assertFalse(send(http, "GET", base + "/api/credentials", "admin:root-pass", null).body().contains("orders-worker"));
        });
    }

    @Test @DisplayName("an account changes its own password; an admin resets someone else's or disables them")
    void passwords() throws Exception {
        withPortal("root-pass", (base, http) -> {
            send(http, "POST", base + "/api/users", "admin:root-pass",
                    json("user", "rey", "password", "rey-password", "role", "viewer"));

            assertEquals(400, send(http, "POST", base + "/api/password", "rey:rey-password",
                    json("current", "not-it", "password", "new-password")).statusCode(), "the current password is proved");
            assertEquals(200, send(http, "POST", base + "/api/password", "rey:rey-password",
                    json("current", "rey-password", "password", "new-password")).statusCode());
            assertEquals(401, send(http, "GET", base + "/api/instances", "rey:rey-password", null).statusCode(),
                    "the old password is gone");
            assertEquals(200, send(http, "GET", base + "/api/instances", "rey:new-password", null).statusCode());

            assertEquals(200, send(http, "POST", base + "/api/users/rey/password", "admin:root-pass",
                    json("password", "reset-password")).statusCode());
            Thread.sleep(300);
            assertEquals(200, send(http, "GET", base + "/api/instances", "rey:reset-password", null).statusCode());

            assertEquals(200, send(http, "POST", base + "/api/users/rey/disabled", "admin:root-pass",
                    "{\"disabled\":true}").statusCode());
            Thread.sleep(300);
            assertEquals(401, send(http, "GET", base + "/api/instances", "rey:reset-password", null).statusCode(),
                    "a disabled account cannot sign in");

            assertEquals(400, send(http, "POST", base + "/api/password", "admin:root-pass",
                    json("current", "root-pass", "password", "another-password")).statusCode(),
                    "a built-in account's password lives in the environment");
        });
    }

    @Test @DisplayName("the rules that keep the portal reachable: reserved names, weak passwords, the last manager")
    void refusals() throws Exception {
        withPortal("root-pass", (base, http) -> {
            assertTrue(send(http, "POST", base + "/api/users", "admin:root-pass",
                    json("user", "admin", "password", "admin-password", "role", "admin")).body().contains("built-in"),
                    "a built-in name cannot be taken over");
            assertEquals(400, send(http, "POST", base + "/api/users", "admin:root-pass",
                    json("user", "short", "password", "abc", "role", "admin")).statusCode(), "too short");
            assertEquals(400, send(http, "POST", base + "/api/users", "admin:root-pass",
                    json("user", "bad name", "password", "long-enough", "role", "admin")).statusCode(), "bad name");
            assertEquals(400, send(http, "POST", base + "/api/users", "admin:root-pass",
                    json("user", "dana", "password", "dana-password", "role", "root")).statusCode(), "unknown role");
            send(http, "POST", base + "/api/users", "admin:root-pass",
                    json("user", "dana", "password", "dana-password", "role", "admin"));
            assertEquals(400, send(http, "POST", base + "/api/users", "admin:root-pass",
                    json("user", "dana", "password", "other-password", "role", "admin")).statusCode(), "already exists");
            assertEquals(400, send(http, "DELETE", base + "/api/users/admin", "admin:root-pass", null).statusCode(),
                    "a built-in account is not deletable here");
            assertEquals(200, send(http, "DELETE", base + "/api/users/dana", "admin:root-pass", null).statusCode(),
                    "with a built-in admin behind it, deleting the only managed admin is fine");
        });

        withPortal(null, (base, http) -> {
            HttpResponse<String> set = send(http, "POST", base + "/api/setup", null, "{\"password\":\"admin-password\"}");
            assertEquals(200, set.statusCode(), set.body());
            assertEquals(400, send(http, "DELETE", base + "/api/users/admin", "admin:admin-password", null).statusCode(),
                    "the admin set up on first run is the only one that can manage users, so it stays");
            assertTrue(send(http, "DELETE", base + "/api/users/admin", "admin:admin-password", null).body()
                    .contains("no account that can manage users"));
            assertEquals(200, send(http, "POST", base + "/api/users", "admin:admin-password",
                    json("user", "sam", "password", "sam-password", "role", "admin")).statusCode());
            assertEquals(200, send(http, "DELETE", base + "/api/users/admin", "sam:sam-password", null).statusCode(),
                    "with a second admin, the first may go");
        });
    }
}
