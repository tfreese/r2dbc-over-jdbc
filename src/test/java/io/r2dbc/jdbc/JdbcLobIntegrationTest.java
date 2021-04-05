// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.r2dbc.jdbc.codecs.BlobCodec;
import io.r2dbc.jdbc.codecs.ClobCodec;
import io.r2dbc.jdbc.util.DBServerExtension;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.test.TestKit;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author Thomas Freese
 */
class JdbcLobIntegrationTest
{
    /**
    *
    */
    static byte[] ALL_BYTES = new byte[-(-128) + 127];

    /**
    *
    */
    private static ConnectionFactory connectionFactory;

    /**
    *
    */
    @RegisterExtension
    static final DBServerExtension SERVER = new DBServerExtension();

    static
    {
        Hooks.onOperatorDebug();

        for (int i = -128; i < 127; i++)
        {
            ALL_BYTES[-(-128) + i] = (byte) i;
        }
    }

    /**
    *
    */
    @BeforeAll
    static void beforeAll()
    {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, SERVER.getDataSource()).build();

        connectionFactory = ConnectionFactories.get(options);
    }

    /**
     * @return {@link Connection}
     */
    protected static Connection getConnection()
    {
        return Mono.from(getConnectionFactory().create()).block(DBServerExtension.getSqlTimeout());
    }

    /**
     * @return {@link ConnectionFactory}
     */
    protected static ConnectionFactory getConnectionFactory()
    {
        return connectionFactory;
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
                .expectNext(0)
                .verifyComplete();
        // @formatter:on
    }

    /**
    *
    */
    @Test
    void testBigBlob()
    {
        createTable(getConnection(), "BLOB");

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

        Connection connection = getConnection();

        // @formatter:off
        Flux.from(connection.createStatement("INSERT INTO lob_test values(?)")
                .bind(0, Blob.from(Flux.range(0, i).map(it -> ByteBuffer.wrap(ALL_BYTES))))
                //.bind(0, R2dbcUtils.byteArrayToBlob(ALL_BYTES))
                .execute()
                )
                .flatMap(Result::getRowsUpdated)
                .concatWith(TestKit.close(connection))
                .as(StepVerifier::create)
                //.expectNext(1)
                .expectNextCount(1).as("blobs inserted")
                .verifyComplete();


        connection = getConnection();

        Flux.from(connection.createStatement("SELECT my_col FROM lob_test")
                .execute()
                )
                .flatMap(result -> result.map((row, rowMetadata) -> row.get("my_col", Blob.class)))
                .flatMap(Blob::stream)
                .doOnNext(it -> System.out.println(it.remaining()))
                .map(Buffer::remaining)
                .collect(Collectors.summingInt(value -> value))
                .concatWith(TestKit.close(connection))
                .as(StepVerifier::create)
                .expectNext(i * ALL_BYTES.length)
                .verifyComplete();
        // @formatter:on
    }

    /**
    *
    */
    @Test
    void testBigClob()
    {
        createTable(getConnection(), "CLOB");

        int i = 50 + new Random().nextInt(100);

        String TEST_STRING = "foo你好bar";

        // Connection connection = Mono.from(getConnectionFactory().create()).block(DBServerExtension.getSqlTimeout());
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

        Connection connection = getConnection();

        // @formatter:off
        Flux.from(connection.createStatement("INSERT INTO lob_test values(?)")
                .bind(0, Clob.from(Flux.range(0, i).map(it -> TEST_STRING)))
                .execute()
                )
                .flatMap(Result::getRowsUpdated)
                .concatWith(TestKit.close(connection))
                .as(StepVerifier::create)
                //.expectNext(1)
                .expectNextCount(1).as("clobs inserted")
                .verifyComplete();

        connection = getConnection();

        Flux.from(connection.createStatement("SELECT my_col FROM lob_test")
                .execute()
                )
                .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Clob.class)))
                .flatMap(Clob::stream)
                .doOnNext(it -> System.out.println(it.toString()))
                .map(CharSequence::length)
                .collect(Collectors.summingInt(value -> value))
                .concatWith(TestKit.close(connection))
                .as(StepVerifier::create)
                .expectNext(i * TEST_STRING.length())
                .verifyComplete();
        // @formatter:on
    }

    /**
     *
     */
    @Test
    void testNullBlob()
    {
        createTable(getConnection(), "BLOB");

        Connection connection = getConnection();

        // @formatter:off
        Flux.from(connection.createStatement("INSERT INTO lob_test values(?)")
                .bindNull(0, Blob.class)
                .execute()
                )
                .flatMap(Result::getRowsUpdated)
                .concatWith(TestKit.close(connection))
                .as(StepVerifier::create)
                .expectNextCount(1).as("null blobs inserted")
                .verifyComplete();
        // @formatter:on

        connection = getConnection();

        // @formatter:off
        Flux.from(connection.createStatement("SELECT my_col FROM lob_test")
                .execute()
                )
                .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Blob.class)))
                .concatWith(TestKit.close(connection))
                .as(StepVerifier::create)
                .consumeNextWith(actual -> Assertions.assertThat(actual).isSameAs(BlobCodec.NULL_BLOB))
                .verifyComplete();
        // @formatter:on
    }

    /**
     *
     */
    @Test
    void testNullClob()
    {
        createTable(getConnection(), "CLOB");

        Connection connection = getConnection();

        // @formatter:off
        Flux.from(connection.createStatement("INSERT INTO lob_test values(?)")
                .bindNull(0, Clob.class)
                .execute()
                )
                .flatMap(Result::getRowsUpdated)
                .concatWith(TestKit.close(connection))
                .as(StepVerifier::create)
                .expectNextCount(1).as("null clobs inserted")
                .verifyComplete();
        // @formatter:on

        connection = getConnection();

        // @formatter:off
        Flux.from(connection.createStatement("SELECT my_col FROM lob_test")
                .execute()
                )
                .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Clob.class)))
                .concatWith(TestKit.close(connection))
                .as(StepVerifier::create)
                .consumeNextWith(actual -> Assertions.assertThat(actual).isSameAs(ClobCodec.NULL_CLOB))
                .verifyComplete();
        // @formatter:on
    }
}
