// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.util.Objects;
import java.util.function.BiFunction;

import org.reactivestreams.Publisher;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcResult implements Result
{
    /**
     *
     */
    private final Mono<RowMetadata> rowMetadata;

    /**
     *
     */
    private final Flux<Row> rows;

    /**
     *
     */
    private final Publisher<Integer> rowsUpdated;

    /**
     * Erstellt ein neues {@link JdbcResult} Object.
     *
     * @param rows {@link Flux}
     * @param rowMetadata {@link Mono}
     * @param rowsUpdated {@link Publisher}
     */
    public JdbcResult(final Flux<Row> rows, final Mono<RowMetadata> rowMetadata, final Publisher<Integer> rowsUpdated)
    {
        super();

        this.rows = Objects.requireNonNull(rows, "rows must not be null");
        this.rowMetadata = Objects.requireNonNull(rowMetadata, "rowMetadata must not be null");
        this.rowsUpdated = Objects.requireNonNull(rowsUpdated, "rowsUpdated must not be null");
    }

    /**
     * @see io.r2dbc.spi.Result#getRowsUpdated()
     */
    @Override
    public Publisher<Integer> getRowsUpdated()
    {
        return this.rowsUpdated;
    }

    /**
     * @see io.r2dbc.spi.Result#map(java.util.function.BiFunction)
     */
    @Override
    public <T> Flux<T> map(final BiFunction<Row, RowMetadata, ? extends T> function)
    {
        Objects.requireNonNull(function, "function must not be null");

        return this.rows.zipWith(this.rowMetadata.repeat()).map(tuple -> function.apply(tuple.getT1(), tuple.getT2()));
        // return this.rows.map(row -> function.apply(row, this.rowMetadata.block()));
    }
}
