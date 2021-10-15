// Created: 05.04.2021
package io.r2dbc.jdbc.util;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.springframework.jdbc.core.JdbcOperations;

/**
 * Erzeugt und Löscht die DB-Tabellen vor und nach jeder Test-Methode.
 *
 * @author Thomas Freese
 */
public class JanitorInvocationInterceptor implements InvocationInterceptor
{
    /**
     * @param jdbcOperations {@link JdbcOperations}
     */
    void createTable(final JdbcOperations jdbcOperations)
    {
        jdbcOperations.execute("CREATE TABLE tbl (value INTEGER)");

        // jdbcOperations.execute("CREATE TABLE tbl_auto (id INTEGER IDENTITY, value INTEGER);");
        jdbcOperations.execute("CREATE TABLE tbl_auto (id INTEGER GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1) PRIMARY KEY, value INTEGER)");
    }

    /**
     * @param jdbcOperations {@link JdbcOperations}
     */
    void dropTable(final JdbcOperations jdbcOperations)
    {
        jdbcOperations.execute("DROP TABLE tbl");
        jdbcOperations.execute("DROP TABLE tbl_auto");

        // switch (databaseType)
        // {
        // case DERBY:
        // jdbcOperations.execute("DROP TABLE tbl");
        // jdbcOperations.execute("DROP TABLE tbl_auto");
        // break;
        //
        // default:
        // jdbcOperations.execute("DROP TABLE tbl if exists");
        // jdbcOperations.execute("DROP TABLE tbl_auto if exists");
        // }
    }

    /**
     * @see org.junit.jupiter.api.extension.InvocationInterceptor#interceptTestMethod(org.junit.jupiter.api.extension.InvocationInterceptor.Invocation,
     *      org.junit.jupiter.api.extension.ReflectiveInvocationContext, org.junit.jupiter.api.extension.ExtensionContext)
     */
    @Override
    public void interceptTestMethod(final Invocation<Void> invocation, final ReflectiveInvocationContext<Method> invocationContext,
                                    final ExtensionContext extensionContext)
        throws Throwable
    {
        List<Object> arguments = invocationContext.getArguments();
        Object lastArgument = null;

        if (!arguments.isEmpty())
        {
            lastArgument = arguments.get(arguments.size() - 1);
        }

        DBServerExtension server = null;

        if (lastArgument instanceof DBServerExtension)
        {
            server = (DBServerExtension) lastArgument;
        }

        if (server != null)
        {
            createTable(server.getJdbcOperations());
        }

        try
        {
            invocation.proceed();
        }
        finally
        {
            if (server != null)
            {
                dropTable(server.getJdbcOperations());
            }
        }
    }

    /**
     * @see org.junit.jupiter.api.extension.InvocationInterceptor#interceptTestTemplateMethod(org.junit.jupiter.api.extension.InvocationInterceptor.Invocation,
     *      org.junit.jupiter.api.extension.ReflectiveInvocationContext, org.junit.jupiter.api.extension.ExtensionContext)
     */
    @Override
    public void interceptTestTemplateMethod(final Invocation<Void> invocation, final ReflectiveInvocationContext<Method> invocationContext,
                                            final ExtensionContext extensionContext)
        throws Throwable
    {
        List<Object> arguments = invocationContext.getArguments();
        Object lastArgument = null;

        if (!arguments.isEmpty())
        {
            lastArgument = arguments.get(arguments.size() - 1);
        }

        DBServerExtension server = null;

        if (lastArgument instanceof DBServerExtension)
        {
            server = (DBServerExtension) lastArgument;
        }

        if (server != null)
        {
            createTable(server.getJdbcOperations());
        }

        try
        {
            invocation.proceed();
        }
        finally
        {
            if (server != null)
            {
                dropTable(server.getJdbcOperations());
            }
        }
    }
}
