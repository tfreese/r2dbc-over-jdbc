// Created: 14.06.2019
package io.r2dbc.jdbc;

import static io.r2dbc.jdbc.util.Awaits.awaitNone;
import static io.r2dbc.jdbc.util.Awaits.awaitQuery;
import static io.r2dbc.jdbc.util.Awaits.awaitUpdate;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.r2dbc.jdbc.util.DbServerExtension;
import io.r2dbc.jdbc.util.JanitorInvocationInterceptor;
import io.r2dbc.jdbc.util.MultiDatabaseExtension;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author Thomas Freese
 */
@ExtendWith(JanitorInvocationInterceptor.class)
final class ParameterizedExampleTest {
    @RegisterExtension
    static final MultiDatabaseExtension DATABASE_EXTENSION = new MultiDatabaseExtension();

    static Connection getConnection(final ConnectionFactory connectionFactory) {
        return Mono.from(connectionFactory.create()).block(DbServerExtension.getSqlTimeout());
    }

    static Stream<Arguments> getDatabases() {
        return DATABASE_EXTENSION.getServers().stream().map(server -> Arguments.of(server.getDatabaseType(), server));
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testBatch") // Ohne Parameter
    @MethodSource("getDatabases")
    void testBatch(final EmbeddedDatabaseType databaseType, final DbServerExtension server) {
        final ConnectionFactory connectionFactory = ConnectionFactories.get(
                ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build());

        final Connection connection = getConnection(connectionFactory);

        try {
            awaitUpdate(5L, connection.createStatement("INSERT INTO test VALUES (?)")
                    .bind(0, 1).add()
                    .bind(0, 2).add()
                    .bind(0, 3).add()
                    .bind(0, 4).add()
                    .bind(0, 5)
            );
        }
        finally {
            awaitNone(connection.close());
        }

        assertTrue(true);

        // Mono.from(connectionFactory.create())
        //         .flatMapMany(connection -> {
        //             final Statement statement = connection.createStatement("INSERT INTO test VALUES (?)");
        //
        //             IntStream.range(0, 10).forEach(i -> statement.bind(0, i).add());
        //
        //             return Flux.from(statement.execute())
        //                     .flatMap(TestKit::extractRowsUpdated)
        //                     .concatWith(TestKit.close(connection));
        //         })
        //         .as(StepVerifier::create)
        //         .expectNext(10).as("values from insertions")
        //         .verifyComplete();
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testBatchAutoIncrement") // Ohne Parameter
    @MethodSource("getDatabases")
    void testBatchAutoIncrement(final EmbeddedDatabaseType databaseType, final DbServerExtension server) {
        final ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
                .option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource())
                //.option(JdbcConnectionFactoryProvider.CODECS, new MyCodecs()) // optional
                .build();

        final ConnectionFactory connectionFactory = ConnectionFactories.get(options);

        Flux.usingWhen(connectionFactory.create(),
                        connection -> {
                            final Statement statement = connection.createStatement("INSERT INTO test_auto (test_value) VALUES (?)");

                            IntStream.range(0, 10).forEach(i -> {
                                statement.bind(0, i);

                                if (i != 9) {
                                    statement.add();
                                }
                            });

                            return Flux.from(statement.returnGeneratedValues().execute())
                                    .flatMap(result -> Flux.from(result.map((row, rowMetadata) -> row.get(0, Integer.class)))
                                            .collectList())
                                    ;
                        }, Connection::close)
                .as(StepVerifier::create)
                // .expectNext(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).as("values from insertions")
                // .expectNext(List.of(10)).as("values from insertions")
                .expectNext(EmbeddedDatabaseType.DERBY.equals(databaseType) ? List.of(10) : List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).as("values from insertions")
                .verifyComplete();
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testBatchWithCommit") // Ohne Parameter
    @MethodSource("getDatabases")
    void testBatchWithCommit(final EmbeddedDatabaseType databaseType, final DbServerExtension server) {
        final ConnectionFactory connectionFactory = ConnectionFactories.get(
                ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build());

        final Connection connection = getConnection(connectionFactory);

        try {
            awaitNone(connection.beginTransaction());
            awaitUpdate(1L, connection.createStatement("INSERT INTO test VALUES (100)"));
            awaitUpdate(3L, connection.createStatement("INSERT INTO test VALUES (?)").bind(0, 200).add().bind(0, 300).add().bind(0, 400));
            awaitQuery(List.of(100, 200, 300, 400), row -> row.get(0, Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitQuery(List.of(100, 200, 300, 400), row -> row.get("test_value", Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitNone(connection.commitTransaction());

            awaitQuery(List.of(100, 200, 300, 400), row -> row.get(0, Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitQuery(List.of(100, 200, 300, 400), row -> row.get("test_value", Integer.class), connection.createStatement("SELECT test_value FROM test"));

            awaitNone(connection.beginTransaction());
            awaitUpdate(2, connection.createStatement("DELETE FROM test where test_value < ?").bind(0, 255));
            awaitQuery(List.of(300, 400), row -> row.get(0, Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitQuery(List.of(300, 400), row -> row.get("test_value", Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitNone(connection.commitTransaction());

            awaitQuery(List.of(300, 400), row -> row.get(0, Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitQuery(List.of(300, 400), row -> row.get("test_value", Integer.class), connection.createStatement("SELECT test_value FROM test"));
        }
        finally {
            awaitNone(connection.close());
        }

        assertTrue(true);

        // server.getJdbcOperations().execute("INSERT INTO test VALUES (100)");
        //
        // Mono.from(connectionFactory.create())
        //         .flatMapMany(connection -> Mono.from(connection.beginTransaction())
        //                 .<Object>thenMany(Flux.from(connection.createStatement("INSERT INTO test VALUES (?)")
        //                                 .bind(0, 200)
        //                                 .add().bind(0, 300)
        //                                 .add().bind(0, 400)
        //                                 .execute())
        //                         .flatMap(TestKit::extractRowsUpdated))
        //                 .concatWith(Flux.from(connection.createStatement("SELECT test_value FROM test")
        //                                 .execute())
        //                         .flatMap(TestKit::extractColumns))
        //                 .concatWith(connection.commitTransaction())
        //
        //                 .concatWith(Flux.from(connection.createStatement("SELECT test_value FROM test")
        //                                 .execute())
        //                         .flatMap(TestKit::extractColumns))
        //
        //                 .concatWith(connection.beginTransaction())
        //                 .concatWith(Flux.from(connection.createStatement("DELETE FROM test where test_value < ?")
        //                                 .bind(0, 255)
        //                                 .execute())
        //                         .flatMap(TestKit::extractRowsUpdated))
        //                 .concatWith(connection.commitTransaction())
        //
        //                 .concatWith(Flux.from(connection.createStatement("SELECT test_value FROM test")
        //                                 .execute())
        //                         .flatMap(TestKit::extractColumns))
        //
        //                 .concatWith(TestKit.close(connection))
        //         )
        //         .as(StepVerifier::create)
        //         .expectNext(3).as("rows inserted")
        //         .expectNext(List.of(100, 200, 300, 400)).as("values from select before commit")
        //         .expectNext(List.of(100, 200, 300, 400)).as("values from select after commit")
        //         .expectNext(2).as("rows deleted")
        //         .expectNext(List.of(300, 400)).as("values from select after delete after commit")
        //         .verifyComplete()
        // ;
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testBatchWithRollback") // Ohne Parameter
    @MethodSource("getDatabases")
    void testBatchWithRollback(final EmbeddedDatabaseType databaseType, final DbServerExtension server) {
        final ConnectionFactory connectionFactory = ConnectionFactories.get(
                ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build());

        final Connection connection = getConnection(connectionFactory);

        try {
            awaitNone(connection.beginTransaction());
            awaitUpdate(1L, connection.createStatement("INSERT INTO test VALUES (100)"));
            awaitUpdate(3L, connection.createStatement("INSERT INTO test VALUES (?)").bind(0, 200).add().bind(0, 300).add().bind(0, 400));
            awaitQuery(List.of(100, 200, 300, 400), row -> row.get(0, Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitQuery(List.of(100, 200, 300, 400), row -> row.get("test_value", Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitNone(connection.commitTransaction());

            awaitQuery(List.of(100, 200, 300, 400), row -> row.get(0, Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitQuery(List.of(100, 200, 300, 400), row -> row.get("test_value", Integer.class), connection.createStatement("SELECT test_value FROM test"));

            awaitNone(connection.beginTransaction());
            awaitUpdate(2, connection.createStatement("DELETE FROM test where test_value < ?").bind(0, 255));
            awaitQuery(List.of(300, 400), row -> row.get(0, Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitQuery(List.of(300, 400), row -> row.get("test_value", Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitNone(connection.rollbackTransaction());

            awaitQuery(List.of(100, 200, 300, 400), row -> row.get(0, Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitQuery(List.of(100, 200, 300, 400), row -> row.get("test_value", Integer.class), connection.createStatement("SELECT test_value FROM test"));
        }
        finally {
            awaitNone(connection.close());
        }

        assertTrue(true);
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testDeleteBatch") // Ohne Parameter
    @MethodSource("getDatabases")
    void testDeleteBatch(final EmbeddedDatabaseType databaseType, final DbServerExtension server) {
        final ConnectionFactory connectionFactory = ConnectionFactories.get(
                ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build());

        final Connection connection = getConnection(connectionFactory);

        try {
            awaitUpdate(List.of(5L), connection.createStatement("INSERT INTO test VALUES (100), (200), (300), (400), (500)"));
            awaitUpdate(2, connection.createStatement("DELETE FROM test where test_value = ?").bind(0, 300).add().bind(0, 400));
            awaitQuery(List.of(100, 200, 500), row -> row.get(0, Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitQuery(List.of(100, 200, 500), row -> row.get("test_value", Integer.class), connection.createStatement("SELECT test_value FROM test"));
        }
        finally {
            awaitNone(connection.close());
        }

        assertTrue(true);

        // server.getJdbcOperations().execute("INSERT INTO test VALUES (100, 200, 300, 400, 500)");
        //
        // Mono.from(connectionFactory.create())
        //         .flatMapMany(connection -> Mono.from(connection.beginTransaction())
        //                 .<Object>thenMany(Flux.from(connection.createStatement("DELETE FROM test where test_value < ?")
        //                                 .bind(0, 300)
        //                                 .add().bind(0, 400)
        //                                 .execute())
        //                         .flatMap(TestKit::extractRowsUpdated))
        //                 .concatWith(connection.commitTransaction())
        //
        //                 .concatWith(Flux.from(connection.createStatement("SELECT test_value FROM test")
        //                                 .execute())
        //                         .flatMap(TestKit::extractColumns))
        //
        //                 .concatWith(TestKit.close(connection))
        //         )
        //         .as(StepVerifier::create)
        //         .expectNext(3).as("rows deleted")
        //         .expectNext(List.of(400, 500)).as("values from select after delete after commit")
        //         .verifyComplete()
        // ;
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testInsert") // Ohne Parameter
    @MethodSource("getDatabases")
    void testInsert(final EmbeddedDatabaseType databaseType, final DbServerExtension server) {
        final ConnectionFactory connectionFactory = ConnectionFactories.get(
                ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build());

        final Connection connection = getConnection(connectionFactory);

        try {
            // @formatter:off
            awaitUpdate(1, connection.createStatement("INSERT INTO test VALUES (?)")
                    .bind(0, 1)
                    );
            // @formatter:on
        }
        finally {
            awaitNone(connection.close());
        }

        assertTrue(true);

        // Mono.from(connectionFactory.create())
        //         .flatMapMany(connection -> {
        //             final Statement statement = connection.createStatement("INSERT INTO test VALUES (?)")
        //                     .bind(0, 2)
        //                     .add()
        //                     .add()
        //                     .add();
        //
        //             return Flux.from(statement.execute())
        //                     .flatMap(TestKit::extractRowsUpdated)
        //                     .concatWith(TestKit.close(connection));
        //         })
        //         .as(StepVerifier::create)
        //         .expectNext(1).as("value from insertion")
        //         .verifyComplete();
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testSelectWithConverter") // Ohne Parameter
    @MethodSource("getDatabases")
    void testSelectWithConverter(final EmbeddedDatabaseType databaseType, final DbServerExtension server) {
        final ConnectionFactory connectionFactory = ConnectionFactories.get(
                ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build());

        server.getJdbcOperations().execute("INSERT INTO test VALUES (100), (200)");

        Flux.usingWhen(connectionFactory.create(),
                        connection -> Mono.from(connection
                                        .beginTransaction())
                                .<Object>thenMany(Flux.from(connection.createStatement("SELECT test_value FROM test")
                                                .execute())
                                        .flatMap(result -> Flux.from(result.map((row, rowMetadata) -> row.get(0, Integer.class)))
                                                .collectList()))

                                .concatWith(Flux.from(connection.createStatement("SELECT test_value FROM test")
                                                .execute())
                                        .flatMap(result -> Flux.from(result.map((row, rowMetadata) -> row.get(0, String.class)))
                                                .collectList()))

                                .concatWith(Flux.from(connection.createStatement("SELECT test_value FROM test")
                                                .execute())
                                        .flatMap(result -> Flux.from(result.map((row, rowMetadata) -> row.get(0, Double.class)))
                                                .collectList())), Connection::close)
                .as(StepVerifier::create)
                .expectNext(List.of(100, 200)).as("value from Integer select")
                .expectNext(List.of("100", "200")).as("values from String select")
                .expectNext(List.of(100D, 200D)).as("values from Double select")
                .verifyComplete()
        ;
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testUpdate") // Ohne Parameter
    @MethodSource("getDatabases")
    void testUpdate(final EmbeddedDatabaseType databaseType, final DbServerExtension server) {
        final ConnectionFactory connectionFactory = ConnectionFactories.get(
                ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build());

        final Connection connection = getConnection(connectionFactory);

        try {
            awaitNone(connection.beginTransaction());
            awaitUpdate(1L, connection.createStatement("INSERT INTO test VALUES (100)"));
            awaitUpdate(3L, connection.createStatement("INSERT INTO test VALUES (?)").bind(0, 200).add().bind(0, 300).add().bind(0, 400));
            awaitQuery(List.of(100, 200, 300, 400), row -> row.get(0, Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitQuery(List.of(100, 200, 300, 400), row -> row.get("test_value", Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitNone(connection.commitTransaction());

            awaitQuery(List.of(100, 200, 300, 400), row -> row.get(0, Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitQuery(List.of(100, 200, 300, 400), row -> row.get("test_value", Integer.class), connection.createStatement("SELECT test_value FROM test"));

            // @formatter:off
            awaitNone(connection.beginTransaction());
            awaitUpdate(4, connection.createStatement("UPDATE test set test_value = ? where test_value = ?")
                    .bind(0, 199).bind(1, 100)
                    .add().bind(0, 299).bind(1, 200)
                    .add().bind(0, 399).bind(1, 300)
                    .add().bind(0, 499).bind(1, 400)
                    );
            awaitQuery(List.of(199, 299, 399, 499), row -> row.get(0, Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitQuery(List.of(199, 299, 399, 499), row -> row.get("test_value", Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitNone(connection.commitTransaction());
            // @formatter:on

            awaitQuery(List.of(199, 299, 399, 499), row -> row.get(0, Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitQuery(List.of(199, 299, 399, 499), row -> row.get("test_value", Integer.class), connection.createStatement("SELECT test_value FROM test"));
        }
        finally {
            awaitNone(connection.close());
        }

        assertTrue(true);

        // server.getJdbcOperations().execute("INSERT INTO test VALUES (100)");
        //
        // Mono.from(connectionFactory.create())
        //         .flatMapMany(connection -> Mono.from(connection.beginTransaction())
        //                 .<Object>thenMany(Flux.from(connection.createStatement("INSERT INTO test VALUES (?)")
        //                                 .bind(0, 200)
        //                                 .add().bind(0, 300)
        //                                 .add().bind(0, 400)
        //                                 .execute())
        //                         .flatMap(TestKit::extractRowsUpdated))
        //                 .concatWith(Flux.from(connection.createStatement("SELECT test_value FROM test")
        //                                 .execute())
        //                         .flatMap(TestKit::extractColumns))
        //                 .concatWith(connection.commitTransaction())
        //
        //                 .concatWith(Flux.from(connection.createStatement("SELECT test_value FROM test")
        //                                 .execute())
        //                         .flatMap(TestKit::extractColumns))
        //
        //                 .concatWith(connection.beginTransaction())
        //                 .concatWith(Flux.from(connection.createStatement("UPDATE test set test_value = ? where test_value = ?")
        //                                 .bind(0, 199).bind(1, 100)
        //                                 .add().bind(0, 299).bind(1, 200)
        //                                 .add().bind(0, 399).bind(1, 300)
        //                                 .add().bind(0, 499).bind(1, 400)
        //                                 .execute())
        //                         .flatMap(TestKit::extractRowsUpdated))
        //                 .concatWith(connection.commitTransaction())
        //
        //                 .concatWith(Flux.from(connection.createStatement("SELECT test_value FROM test")
        //                                 .execute())
        //                         .flatMap(TestKit::extractColumns))
        //
        //                 .concatWith(TestKit.close(connection))
        //         )
        //         .as(StepVerifier::create)
        //         .expectNext(3).as("rows inserted")
        //         .expectNext(List.of(100, 200, 300, 400)).as("values from select before commit")
        //         .expectNext(List.of(100, 200, 300, 400)).as("values from select after commit")
        //         .expectNext(4).as("rows updated")
        //         .expectNext(List.of(199, 299, 399, 499)).as("values from select after update after commit")
        //         .verifyComplete()
        // ;
    }
}
