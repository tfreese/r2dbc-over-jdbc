// Created: 31.03.2021
package io.r2dbc.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;

/**
 * @author Thomas Freese
 */
public class JdbcBatch implements Batch
{
    /**
     *
     */
    private final Connection connection;

    /**
     *
     */
    private final List<String> statements = new ArrayList<>();

    /**
     * Erstellt ein neues {@link JdbcBatch} Object.
     *
     * @param connection {@link Connection}
     */
    public JdbcBatch(final Connection connection)
    {
        super();

        this.connection = Objects.requireNonNull(connection, "connection required");
    }

    /**
     * @see io.r2dbc.spi.Batch#add(java.lang.String)
     */
    @Override
    public Batch add(final String sql)
    {
        this.statements.add(Objects.requireNonNull(sql, "sql required"));

        return this;
    }

    /**
     * @see io.r2dbc.spi.Batch#execute()
     */
    @Override
    public Flux<Result> execute()
    {
        // @formatter:off
        return Flux.fromIterable(this.statements).map(this.connection::createStatement).flatMap(Statement::execute);
        // @formatter:on
    }
}
