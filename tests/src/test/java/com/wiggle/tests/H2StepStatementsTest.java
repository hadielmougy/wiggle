package com.wiggle.tests;

import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;

import java.util.ArrayList;
import java.util.List;

/** {@link StepStatementSequences} on H2: the lock reads the token apart, and a claim selects then updates each task. */
class H2StepStatementsTest extends StepStatementSequences {

    @Override protected JdbcStorage storage() {
        JdbcStorage storage = new JdbcStorage("jdbc:h2:mem:seq-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
                "sa", "", 4, new H2Dialect());
        storage.migrate();
        return storage;
    }

    @Override protected List<String> lockTask() {
        return List.of("SELECT wf_instance FOR UPDATE", "SELECT wf_token");
    }

    @Override protected List<String> claim(int tasks) {
        List<String> out = new ArrayList<>(List.of("SELECT wf_token"));
        for (int i = 0; i < tasks; i++) out.add("UPDATE wf_token");
        return out;
    }
}
