// Created: 05.04.2021
package io.r2dbc.jdbc.util;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcOperations;

/**
 * Erzeugt und löscht die DB-Tabellen vor und nach jeder Test-Methode.
 *
 * @author Thomas Freese
 */
public class JanitorInvocationInterceptor implements InvocationInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(JanitorInvocationInterceptor.class);

    @Override
    public void interceptTestMethod(final Invocation<Void> invocation, final ReflectiveInvocationContext<Method> invocationContext, final ExtensionContext extensionContext)
            throws Throwable {
        final List<Object> arguments = invocationContext.getArguments();
        Object lastArgument = null;

        if (!arguments.isEmpty()) {
            lastArgument = arguments.get(arguments.size() - 1);
        }

        DbServerExtension serverExtension = null;

        if (lastArgument instanceof DbServerExtension se) {
            serverExtension = se;
        }

        if (serverExtension != null) {
            createTable(serverExtension.getJdbcOperations());
        }

        try {
            invocation.proceed();
        }
        finally {
            if (serverExtension != null) {
                dropTable(serverExtension.getJdbcOperations());
            }
        }
    }

    @Override
    public void interceptTestTemplateMethod(final Invocation<Void> invocation, final ReflectiveInvocationContext<Method> invocationContext, final ExtensionContext extensionContext)
            throws Throwable {
        interceptTestMethod(invocation, invocationContext, extensionContext);
    }

    private void createTable(final JdbcOperations jdbcOperations) {
        // LOGGER.debug("createTables");

        try {
            jdbcOperations.execute("CREATE TABLE test (test_value INTEGER)");
        }
        catch (Exception ex) {
            // Ignore
            LOGGER.error(ex.getMessage());
        }

        try {
            // jdbcOperations.execute("CREATE TABLE tbl_auto (id INTEGER IDENTITY, value INTEGER);");
            jdbcOperations.execute("CREATE TABLE test_auto (id INTEGER GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1) PRIMARY KEY, test_value INTEGER)");
        }
        catch (Exception ex) {
            // Ignore
            LOGGER.error(ex.getMessage());
        }
    }

    private void dropTable(final JdbcOperations jdbcOperations) {
        // LOGGER.debug("dropTables");

        try {
            jdbcOperations.execute("DROP TABLE test");
        }
        catch (Exception ex) {
            // Ignore
            LOGGER.error(ex.getMessage());
        }

        try {
            jdbcOperations.execute("DROP TABLE test_auto");
        }
        catch (Exception ex) {
            // Ignore
            LOGGER.error(ex.getMessage());
        }

        // switch (databaseType)
        // {
        // case DERBY:
        // jdbcOperations.execute("DROP TABLE test");
        // jdbcOperations.execute("DROP TABLE test_auto");
        // break;
        //
        // default:
        // jdbcOperations.execute("DROP TABLE test if exists");
        // jdbcOperations.execute("DROP TABLE test_auto if exists");
        // }
    }
}
