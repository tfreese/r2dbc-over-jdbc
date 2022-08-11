// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.r2dbc.jdbc.codecs.BlobCodec;
import io.r2dbc.jdbc.codecs.ClobCodec;
import io.r2dbc.jdbc.util.DbServerExtension;
import io.r2dbc.jdbc.util.MultiDatabaseExtension;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author Thomas Freese
 */
class ParameterizedLobTest
{
    /**
     *
     */
    static final byte[] ALL_BYTES = new byte[-(-128) + 127];

    /**
     *
     */
    @RegisterExtension
    static final MultiDatabaseExtension DATABASE_EXTENSION = new MultiDatabaseExtension();

    static
    {
        Hooks.onOperatorDebug();

        for (int i = -128; i < 127; i++)
        {
            ALL_BYTES[-(-128) + i] = (byte) i;
        }
    }

    /**
     * @param connectionFactory {@link ConnectionFactory}
     *
     * @return {@link Connection}
     */
    static Connection getConnection(final ConnectionFactory connectionFactory)
    {
        return Mono.from(connectionFactory.create()).block(DbServerExtension.getSqlTimeout());
    }

    /**
     * @return {@link Stream}
     */
    static Stream<Arguments> getDatabases()
    {
        return DATABASE_EXTENSION.getServers().stream().map(server -> Arguments.of(server.getDatabaseType(), server));
    }

    /**
     * @param connection {@link Connection}
     * @param columnType String
     */
    void createTable(final Connection connection, final String columnType)
    {
        // "DROP TABLE IF EXISTS lob_test"

        // @formatter:off
        Flux.from(connection.createStatement("DROP TABLE lob_test")
                .execute()
                )
                .flatMap(Result::getRowsUpdated)
                .onErrorResume(e -> Mono.empty())
                .thenMany(
                        Flux.from(connection.createStatement("CREATE TABLE lob_test (my_col " + columnType + ")")
                                .execute()
                                )
                                .flatMap(Result::getRowsUpdated)
                        )
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();
        // @formatter:on
    }

    /**
     * @param databaseType {@link EmbeddedDatabaseType}
     * @param server {@link DbServerExtension}
     */
    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testBigBlob") // Ohne Parameter
    @MethodSource("getDatabases")
    void testBigBlob(final EmbeddedDatabaseType databaseType, final DbServerExtension server)
    {
        ConnectionFactory connectionFactory =
                ConnectionFactories.get(ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build());

        createTable(getConnection(connectionFactory), "BLOB");

        int i = 50 + new Random().nextInt(1);

        // Connection connection = getConnection();
        //
        // try
        // {
        // awaitNone(connection.beginTransaction());
        // awaitUpdate(1,
        // connection.createStatement("INSERT INTO lob_test values(?)").bind(0, Blob.from(Flux.range(0, i).map(it -> ByteBuffer.wrap(ALL_BYTES)))));
        // awaitNone(connection.commitTransaction());
        // }
        // finally
        // {
        // awaitNone(connection.close());
        // }

        Connection connection = getConnection(connectionFactory);

        // @formatter:off
        Flux.from(connection.createStatement("INSERT INTO lob_test values(?)")
                .bind(0, Blob.from(Flux.range(0, i).map(it -> ByteBuffer.wrap(ALL_BYTES))))
                //.bind(0, R2dbcUtils.byteArrayToBlob(ALL_BYTES))
                .execute()
                )
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                //.expectNext(1)
                .expectNextCount(1).as("blobs inserted")
                .verifyComplete();


        connection = getConnection(connectionFactory);

        Flux.from(connection.createStatement("SELECT my_col FROM lob_test")
                .execute()
                )
                .flatMap(result -> result.map((row, rowMetadata) -> row.get("my_col", Blob.class)))
                .flatMap(Blob::stream)
                //.doOnNext(it -> System.out.println(it.remaining()))
                .map(Buffer::remaining)
                .collect(Collectors.summingInt(value -> value))
                .as(StepVerifier::create)
                .expectNext(i * ALL_BYTES.length)
                .verifyComplete();
        // @formatter:on
    }

    /**
     * @param databaseType {@link EmbeddedDatabaseType}
     * @param server {@link DbServerExtension}
     */
    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testBigClob") // Ohne Parameter
    @MethodSource("getDatabases")
    void testBigClob(final EmbeddedDatabaseType databaseType, final DbServerExtension server)
    {
        ConnectionFactory connectionFactory =
                ConnectionFactories.get(ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build());

        createTable(getConnection(connectionFactory), "CLOB");

        int i = 50 + new Random().nextInt(100);

        String TEST_STRING = "foo你好bar";

        // Connection connection = getConnection(connectionFactory);
        //
        // try
        // {
        // awaitNone(connection.beginTransaction());
        // awaitUpdate(1, connection.createStatement("INSERT INTO lob_test values(?)").bind(0, Clob.from(Flux.range(0, i).map(it -> TEST_STRING))));
        // awaitNone(connection.commitTransaction());
        // }
        // finally
        // {
        // awaitNone(connection.close());
        // }

        Connection connection = getConnection(connectionFactory);

        // @formatter:off
        Flux.from(connection.createStatement("INSERT INTO lob_test values(?)")
                .bind(0, Clob.from(Flux.range(0, i).map(it -> TEST_STRING)))
                .execute()
                )
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                //.expectNext(1)
                .expectNextCount(1).as("clobs inserted")
                .verifyComplete();

        connection = getConnection(connectionFactory);

        Flux.from(connection.createStatement("SELECT my_col FROM lob_test")
                .execute()
                )
                .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Clob.class)))
                .flatMap(Clob::stream)
                //.doOnNext(it -> System.out.println(it.toString()))
                .map(CharSequence::length)
                .collect(Collectors.summingInt(value -> value))
                .as(StepVerifier::create)
                .expectNext(i * TEST_STRING.length())
                .verifyComplete();
        // @formatter:on
    }

    /**
     * @param databaseType {@link EmbeddedDatabaseType}
     * @param server {@link DbServerExtension}
     */
    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testNullBlob") // Ohne Parameter
    @MethodSource("getDatabases")
    void testNullBlob(final EmbeddedDatabaseType databaseType, final DbServerExtension server)
    {
        ConnectionFactory connectionFactory =
                ConnectionFactories.get(ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build());

        createTable(getConnection(connectionFactory), "BLOB");

        Connection connection = getConnection(connectionFactory);

        // @formatter:off
        Flux.from(connection.createStatement("INSERT INTO lob_test values(?)")
                .bindNull(0, Blob.class)
                .execute()
                )
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNextCount(1).as("null blobs inserted")
                .verifyComplete();
        // @formatter:on

        connection = getConnection(connectionFactory);

        // @formatter:off
        Flux.from(connection.createStatement("SELECT my_col FROM lob_test")
                .execute()
                )
                .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Blob.class)))
                .as(StepVerifier::create)
                .consumeNextWith(actual -> Assertions.assertThat(actual).isSameAs(BlobCodec.NULL_BLOB))
                .verifyComplete();
        // @formatter:on
    }

    /**
     * @param databaseType {@link EmbeddedDatabaseType}
     * @param server {@link DbServerExtension}
     */
    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testNullClob") // Ohne Parameter
    @MethodSource("getDatabases")
    void testNullClob(final EmbeddedDatabaseType databaseType, final DbServerExtension server)
    {
        ConnectionFactory connectionFactory =
                ConnectionFactories.get(ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, server.getDataSource()).build());

        createTable(getConnection(connectionFactory), "CLOB");

        Connection connection = getConnection(connectionFactory);

        // @formatter:off
        Flux.from(connection.createStatement("INSERT INTO lob_test values(?)")
                .bindNull(0, Clob.class)
                .execute()
                )
                .flatMap(Result::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNextCount(1).as("null clobs inserted")
                .verifyComplete();
        // @formatter:on

        connection = getConnection(connectionFactory);

        // @formatter:off
        Flux.from(connection.createStatement("SELECT my_col FROM lob_test")
                .execute()
                )
                .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Clob.class)))
                .as(StepVerifier::create)
                .consumeNextWith(actual -> Assertions.assertThat(actual).isSameAs(ClobCodec.NULL_CLOB))
                .verifyComplete();
        // @formatter:on
    }
}
