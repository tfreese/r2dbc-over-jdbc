// Created: 14.06.2019
package io.r2dbc.jdbc;

import static io.r2dbc.jdbc.util.Awaits.awaitNone;
import static io.r2dbc.jdbc.util.Awaits.awaitQuery;
import static io.r2dbc.jdbc.util.Awaits.awaitUpdate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.JDBCType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.jdbc.codecs.DefaultCodecs;
import io.r2dbc.jdbc.util.DbServerExtension;
import io.r2dbc.jdbc.util.JanitorInvocationInterceptor;
import io.r2dbc.jdbc.util.MultiDatabaseExtension;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.RowMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
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
final class ParameterizedRowTest {
    @RegisterExtension
    static final MultiDatabaseExtension DATABASE_EXTENSION = new MultiDatabaseExtension();

    static Stream<Arguments> getDatabases() {
        return DATABASE_EXTENSION.getServers().stream().map(server -> Arguments.of(server.getDatabaseType(), server));
    }

    private final Codecs codecs = new DefaultCodecs();

    @Test
    void testConstructorNoValues() {
        final JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("TEST-NAME-1", 0, Object.class, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        final JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        assertThatNullPointerException().isThrownBy(() -> new JdbcRow(null, new HashMap<>(), this.codecs)).withMessage("rowMetadata required");
        assertThatNullPointerException().isThrownBy(() -> new JdbcRow(rowMetadata, null, this.codecs)).withMessage("values required");
    }

    @Test
    void testGetByIndex() {
        final JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("TEST-NAME-1", 0, Object.class, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        final JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        final Object value = new Object();

        final Map<Integer, Object> values = new HashMap<>();
        values.put(0, value);

        assertThat(new JdbcRow(rowMetadata, values, this.codecs).get(0, Object.class)).isSameAs(value);
    }

    @Test
    void testGetByName() {
        final JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("TEST-NAME-2", 0, Object.class, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        final JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        final Object value = new Object();

        final Map<Integer, Object> values = new HashMap<>();
        values.put(0, value);

        assertThat(new JdbcRow(rowMetadata, values, this.codecs).get("test-name-2", Object.class)).isSameAs(value);
    }

    @Test
    void testGetInvalidIdentifier() {
        final ColumnMetadata columnMetadata = new JdbcColumnMetadata("", 0, Object.class, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        final RowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        assertThat(new JdbcRow(rowMetadata, new HashMap<>(), this.codecs).get(3, Object.class)).isNull();
    }

    @Test
    void testGetNoIdentifier() {
        final JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("", 0, Object.class, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        final JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        assertThatIllegalArgumentException().isThrownBy(() -> new JdbcRow(rowMetadata, new HashMap<>(), this.codecs).get(null, Object.class)).withMessage("name is null");
    }

    @Test
    void testGetNull() {
        final JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("TEST-NAME-3", 0, Object.class, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        final JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        final Map<Integer, Object> values = new HashMap<>();
        values.put(0, null);

        assertThat(new JdbcRow(rowMetadata, values, this.codecs).get("test-name-3", Object.class)).isNull();
    }

    @Test
    void testGetWrongIdentifierType() {
        final String identifier = "-";

        final JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("", 0, Object.class, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        final JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> new JdbcRow(rowMetadata, new HashMap<>(), this.codecs).get(identifier, Object.class))
                .withMessage("No MetaData for Name: %s", identifier);
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testSelectWithAliases") // Ohne Parameter
    @MethodSource("getDatabases")
    void testSelectWithAliases(final EmbeddedDatabaseType databaseType, final DbServerExtension server) {
        final ConnectionFactory connectionFactory = ConnectionFactories.get(
                ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build());

        server.getJdbcOperations().execute("INSERT INTO test VALUES (100)");

        Flux.usingWhen(connectionFactory.create(),
                        connection -> Flux.from(connection.createStatement("SELECT test_value as ALIASED_VALUE FROM test").execute())
                                .flatMap(result -> Flux.from(result.map((row, rowMetadata) -> row.get("ALIASED_VALUE", Integer.class)))
                                        .collectList()
                                ), Connection::close)
                .as(StepVerifier::create)
                .expectNext(List.of(100))
                .verifyComplete();
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testSelectWithoutAliases") // Ohne Parameter
    @MethodSource("getDatabases")
    void testSelectWithoutAliases(final EmbeddedDatabaseType databaseType, final DbServerExtension server) {
        final ConnectionFactory connectionFactory = ConnectionFactories.get(
                ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build());

        // server.getJdbcOperations().execute("INSERT INTO test VALUES (100)");
        //
        // Mono.from(this.connectionFactory.create())
        //         .flatMapMany(connection -> Flux.from(connection.createStatement("SELECT test_value FROM test").execute())
        //                 .flatMap(TestKit::extractColumns)
        //                 .concatWith(TestKit.close(connection)))
        //         .as(StepVerifier::create)
        //         .expectNext(List.of(100))
        //         .verifyComplete();

        final Connection connection = Mono.from(connectionFactory.create()).block(DbServerExtension.getSqlTimeout());

        try {
            // awaitExecution(connection.createStatement("CREATE TABLE test ( test_value INTEGER )"));

            awaitUpdate(1, connection.createStatement("INSERT INTO test VALUES (100)"));

            awaitQuery(List.of(100), row -> row.get(0, Integer.class), connection.createStatement("SELECT test_value FROM test"));
            awaitQuery(List.of(100), row -> row.get("test_value", Integer.class), connection.createStatement("SELECT test_value FROM test"));
        }
        finally {
            awaitNone(connection.close());
        }

        assertTrue(true);
    }
}
