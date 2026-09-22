package com.wiggle.console;

import com.wiggle.client.DirectConnection;
import com.wiggle.client.WiggleConnection;
import com.wiggle.core.Tls;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Console accounts an admin manages: who may create them, who may sign in with them, and what an
 * account can change about itself.
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

    /** A console wired to a user file, with an optional built-in admin password. */
    private interface Body { void run(String base, HttpClient http, ConsoleUsers users) throws Exception; }

    private static void withConsole(Path file, String builtinPassword, Body body) throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             DirectConnection conn = WiggleConnection.direct(server.baseUrl())) {
            ConsoleUsers users = new ConsoleUsers(file);
            ConsoleAuth auth = new ConsoleAuth("admin", builtinPassword, "viewer", null, false, users);
            try (ConsoleServer console = new ConsoleServer(new GrpcDashboardData(new ConsoleBackend.Direct(conn)),
                    auth, 0, Tls.Options.DISABLED).start()) {
                body.run("http://localhost:" + console.port(), HttpClient.newHttpClient(), users);
            }
        }
    }

    @Test @DisplayName("an admin creates accounts with a role; they sign in, and outlive the process")
    void createAndSignIn(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("users.json");
        withConsole(file, "root-pass", (base, http, users) -> {
            assertEquals(200, send(http, "POST", base + "/api/users", "admin:root-pass",
                    json("user", "dana", "password", "dana-password", "role", "admin")).statusCode());
            assertEquals(200, send(http, "POST", base + "/api/users", "admin:root-pass",
                    json("user", "rey", "password", "rey-password", "role", "viewer")).statusCode());

            String list = send(http, "GET", base + "/api/users", "admin:root-pass", null).body();
            assertTrue(list.contains("\"dana\"") && list.contains("\"rey\""), list);
            assertTrue(list.contains("\"builtin\":true"), "the environment's own account is listed as built-in");

            // The new admin can write; the new viewer can read but not write.
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
            assertTrue(me.contains("\"canChangePassword\":true"), me);
        });

        // A second console over the same file: the accounts are still there.
        withConsole(file, "root-pass", (base, http, users) ->
                assertEquals(200, send(http, "GET", base + "/api/instances", "dana:dana-password", null).statusCode()));
        assertFalse(Files.readString(file).contains("dana-password"), "no password is stored in the clear");
    }

    @Test @DisplayName("an account changes its own password; an admin resets someone else's")
    void passwords(@TempDir Path dir) throws Exception {
        withConsole(dir.resolve("users.json"), "root-pass", (base, http, users) -> {
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
            assertEquals(200, send(http, "GET", base + "/api/instances", "rey:reset-password", null).statusCode());

            assertEquals(400, send(http, "POST", base + "/api/password", "admin:root-pass",
                    json("current", "root-pass", "password", "another-password")).statusCode(),
                    "a built-in account's password lives in the environment");
        });
    }

    @Test @DisplayName("the rules that keep the console reachable: reserved names, weak passwords, the last admin")
    void refusals(@TempDir Path dir) throws Exception {
        withConsole(dir.resolve("users.json"), "root-pass", (base, http, users) -> {
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
            // With a built-in admin behind it, deleting the only managed admin is fine.
            assertEquals(200, send(http, "DELETE", base + "/api/users/dana", "admin:root-pass", null).statusCode());
        });

        // No built-in admin: the last managed admin is the only way in, so it stays.
        Path file = dir.resolve("only.json");
        withConsole(file, null, (base, http, users) -> {
            assertTrue(send(http, "GET", base + "/api/auth", null, null).body().contains("\"required\":false"),
                    "no password and no accounts: open mode");
            assertEquals(200, send(http, "POST", base + "/api/users", null,
                    json("user", "dana", "password", "dana-password", "role", "admin")).statusCode());
            assertEquals(401, send(http, "GET", base + "/api/instances", null, null).statusCode(),
                    "the first account turns authentication on");
            assertTrue(send(http, "DELETE", base + "/api/users/dana", "dana:dana-password", null).body()
                            .contains("only admin"), "and the console cannot be locked out");
            assertEquals(200, send(http, "POST", base + "/api/users", "dana:dana-password",
                    json("user", "sam", "password", "sam-password", "role", "admin")).statusCode());
            assertEquals(200, send(http, "DELETE", base + "/api/users/dana", "dana:dana-password", null).statusCode(),
                    "with a second admin, the first may go");
        });
    }
}
