// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcResult implements Result {
    private final int[] affectedRows;
    private final Mono<RowMetadata> rowMetadata;
    private final Flux<JdbcRow> rows;
    private final Long rowsUpdated;

    public JdbcResult(final Flux<JdbcRow> rows, final Mono<RowMetadata> rowMetadata, final int[] affectedRows) {
        super();

        this.rows = Objects.requireNonNull(rows, "rows must not be null");
        this.rowMetadata = Objects.requireNonNull(rowMetadata, "rowMetadata must not be null");
        this.affectedRows = affectedRows;

        if (affectedRows != null) {
            rowsUpdated = IntStream.of(affectedRows).mapToLong(Long::valueOf).sum();
        }
        else {
            rowsUpdated = null;
        }
    }

    @Override
    public Result filter(final Predicate<Segment> filter) {
        Objects.requireNonNull(filter, "filter must not be null");

        if (rowsUpdated != null) {
            return new JdbcResult(Flux.empty(), rowMetadata, null);
        }

        Flux<JdbcRow> filteredSegments = this.rows.filter(filter);

        return new JdbcResult(filteredSegments, rowMetadata, affectedRows);
    }

    @Override
    public <T> Publisher<T> flatMap(final Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
        Objects.requireNonNull(mappingFunction, "mappingFunction must not be null");

        if (rowsUpdated != null) {
            return Flux.just((UpdateCount) () -> rowsUpdated).flatMap(segment -> {
                Publisher<? extends T> result = mappingFunction.apply(segment);

                if (result == null) {
                    return Mono.error(new IllegalStateException("The mapper returned a null Publisher"));
                }

                if (result instanceof Mono) {
                    return result;
                }

                return Flux.from(result);
            });
        }

        return this.rows.flatMap(segment -> {
            Publisher<? extends T> result = mappingFunction.apply(segment);

            if (result == null) {
                return Mono.error(new IllegalStateException("The mapper returned a null Publisher"));
            }

            if (result instanceof Mono) {
                return result;
            }

            return Flux.from(result);
        });
    }

    @Override
    public Mono<Long> getRowsUpdated() {
        if (rowsUpdated == null) {
            return Mono.empty();
        }

        return Mono.just(rowsUpdated);
    }

    @Override
    public <T> Flux<T> map(final BiFunction<Row, RowMetadata, ? extends T> function) {
        Objects.requireNonNull(function, "function must not be null");

        return this.rows.zipWith(this.rowMetadata.repeat()).map(tuple -> function.apply(tuple.getT1(), tuple.getT2()));
        // return this.rows.map(row -> function.apply(row, this.rowMetadata.block()));
    }
}
