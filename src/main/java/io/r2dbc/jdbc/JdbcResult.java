/**
 * Created: 14.06.2019
 */

package io.r2dbc.jdbc;

import java.util.Objects;
import java.util.function.BiFunction;
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
    private final Mono<JdbcRowMetadata> rowMetadata;

    /**
     *
     */
    private final Flux<JdbcRow> rows;

    /**
     *
     */
    private final Mono<Integer> rowsUpdated;

    /**
     * Erstellt ein neues {@link JdbcResult} Object.
     *
     * @param rows {@link Flux}
     * @param rowMetadata {@link Mono}
     * @param rowsUpdated {@link Mono}
     */
    public JdbcResult(final Flux<JdbcRow> rows, final Mono<JdbcRowMetadata> rowMetadata, final Mono<Integer> rowsUpdated)
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
    public Mono<Integer> getRowsUpdated()
    {
        return this.rowsUpdated;
    }

    /**
     * @see io.r2dbc.spi.Result#map(java.util.function.BiFunction)
     */
    @Override
    public <T> Flux<T> map(final BiFunction<Row, RowMetadata, ? extends T> f)
    {
        Objects.requireNonNull(f, "f must not be null");

        return this.rows.zipWith(this.rowMetadata.repeat()).map(tuple -> f.apply(tuple.getT1(), tuple.getT2()));
    }
}
