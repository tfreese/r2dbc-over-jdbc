// Created: 25.03.2021
package io.r2dbc.jdbc.testKit;

import java.util.stream.IntStream;

import io.r2dbc.jdbc.JdbcConnectionFactory;
import io.r2dbc.jdbc.codecs.DefaultCodecs;
import io.r2dbc.jdbc.util.DbServerExtension;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.test.TestKit;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author Thomas Freese
 */
abstract class AbstractTestKit implements TestKit<Integer> {
    public static <T> Mono<T> close(final Connection connection) {
        return Mono.from(connection.close()).then(Mono.empty());
    }

    private ConnectionFactory connectionFactory;

    @Override
    public String doGetSql(final TestStatement statement) {
        return switch (statement) {
            case CREATE_TABLE_AUTOGENERATED_KEY -> "CREATE TABLE test (id INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY, test_value INTEGER)";
            case INSERT_VALUE_AUTOGENERATED_KEY -> "INSERT INTO test VALUES(100, 100)"; // Sollte komischerweise auch ohne den ID-Paramerter gehen?!

            default -> TestKit.super.doGetSql(statement);
        };
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        if (this.connectionFactory == null) {
            this.connectionFactory = new JdbcConnectionFactory(getServer().getDataSource(), new DefaultCodecs());
        }

        return this.connectionFactory;
    }

    @Override
    public Integer getIdentifier(final int index) {
        return index;
    }

    @Override
    public JdbcOperations getJdbcOperations() {
        return getServer().getJdbcOperations();
    }

    @Override
    public String getPlaceholder(final int index) {
        return "?";
    }

    @Override
    @Test
    public void prepareStatement() {
        // Der Original Testfall erwartet 10 RowsUpdated ... expectNextCount(10)
        Flux.usingWhen(getConnectionFactory().create(), connection -> {
            final Statement statement = connection.createStatement(expand(TestStatement.INSERT_VALUE_PLACEHOLDER, getPlaceholder(0)));

            IntStream.range(0, 10).forEach(i -> {
                TestKit.bind(statement, getIdentifier(0), i);

                if (i != 9) {
                    statement.add();
                }
            });

            return Flux.from(statement.execute()).flatMap(this::extractRowsUpdated);
        }, Connection::close).as(StepVerifier::create).expectNext(10L).as("values from insertions").verifyComplete();
    }

    abstract DbServerExtension getServer();
}
