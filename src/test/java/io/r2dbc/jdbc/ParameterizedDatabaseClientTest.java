// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.util.stream.Stream;

import io.r2dbc.client.R2dbc;
import io.r2dbc.jdbc.util.DbServerExtension;
import io.r2dbc.jdbc.util.JanitorInvocationInterceptor;
import io.r2dbc.jdbc.util.MultiDatabaseExtension;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * @author Thomas Freese
 */
// @ExtendWith(DatabaseExtension.class) // funktioniert nicht mit statischem Zugriff auf die Server, werden sonst doppelt erzeugt !
@ExtendWith(JanitorInvocationInterceptor.class)
@Disabled("Not finished")
final class ParameterizedDatabaseClientTest
{
    @RegisterExtension
    static final MultiDatabaseExtension DATABASE_EXTENSION = new MultiDatabaseExtension();

    static Stream<Arguments> getDatabases()
    {
        return DATABASE_EXTENSION.getServers().stream().map(server -> Arguments.of(server.getDatabaseType(), server));
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testInsert") // Ohne Parameter
    // @ArgumentsSource(DatabaseExtension.class)
    @MethodSource("getDatabases")
    void testInsert(final EmbeddedDatabaseType databaseType, final DbServerExtension server)
    {
        ConnectionFactoryOptions connectionFactoryOptions =
                ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build();

        DatabaseClient databaseClient = DatabaseClient.builder()
                .connectionFactory(ConnectionFactories.get(connectionFactoryOptions))
                .namedParameters(false)
                .build();

        // @formatter:off
        databaseClient.sql("INSERT INTO test VALUES (?)")
                .bind(0, 200)
                .map(row -> row.get(0, Long.class))
                .one()
                .as(StepVerifier::create)
                .expectNext(1L).as("value from insertion")
                .verifyComplete()
           ;
       // @formatter:on
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testInsertBatch") // Ohne Parameter
    @MethodSource("getDatabases")
    void testInsertBatch(final EmbeddedDatabaseType databaseType, final DbServerExtension server)
    {
        ConnectionFactoryOptions connectionFactoryOptions =
                ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build();
        R2dbc r2dbc = new R2dbc(ConnectionFactories.get(connectionFactoryOptions));

        // @formatter:off
       r2dbc.inTransaction(handle -> handle.execute("INSERT INTO test VALUES (?)", 100)
               .concatWith(handle.execute("INSERT INTO test VALUES (?)", 200))
               .concatWith(handle.execute("INSERT INTO test VALUES (?)", 300))
               )
           .as(StepVerifier::create)
           .expectNext(1).as("value from insertion")
           .expectNext(1).as("value from insertion")
           .expectNext(1).as("value from insertion")
           .verifyComplete()
           ;
       // @formatter:on
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testInsertWithSelect") // Ohne Parameter
    @MethodSource("getDatabases")
    void testInsertWithSelect(final EmbeddedDatabaseType databaseType, final DbServerExtension server)
    {
        ConnectionFactoryOptions connectionFactoryOptions =
                ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build();
        R2dbc r2dbc = new R2dbc(ConnectionFactories.get(connectionFactoryOptions));

        // @formatter:off
        r2dbc.inTransaction(handle -> handle.execute("INSERT INTO test VALUES (?)", 100))
            .concatWith(r2dbc.inTransaction(handle -> handle.select("SELECT test_value FROM test")
                .mapResult(result -> Flux.from(result.map((row, rowMetadata) -> row.get("test_value", Integer.class)))))
                )
            .as(StepVerifier::create)
            .expectNext(1).as("value from insertion")
            .expectNext(100).as("value from select")
            .verifyComplete()
            ;
        // @formatter:on
    }
}
