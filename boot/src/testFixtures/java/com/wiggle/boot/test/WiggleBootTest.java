package com.wiggle.boot.test;

import com.wiggle.boot.WiggleApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A test on one node of the Boot application, on H2 in memory, its API on a port from
 * {@code TestPorts}, reached through the {@link WiggleTestNode} bean. The node's timings are the
 * fast ones {@link TestNodeInitializer} sets, the process's own {@code WIGGLE_*} variables are not
 * seen, and test properties override both. Profiles add to it: {@code portal} and {@code probe}
 * serve those on ports of their own, {@code postgres} runs on {@code WIGGLE_TEST_PG_URL}.
 *
 * <p>Each test class has a node of its own, shared by its tests; a class whose tests each need a new
 * one adds {@code @DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)}. Tests that share a node keep
 * to queues of their own: a closed worker's long poll can still claim a token on its queue.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@SpringBootTest(classes = WiggleApplication.class, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ContextConfiguration(initializers = TestNodeInitializer.class)
@Import(WiggleTestNode.Config.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public @interface WiggleBootTest {
}
