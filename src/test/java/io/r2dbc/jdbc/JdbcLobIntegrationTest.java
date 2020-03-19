package io.r2dbc.jdbc;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import io.r2dbc.jdbc.converter.Converters;
import io.r2dbc.jdbc.util.AbstractIntegrationTestSupport;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.test.TestKit;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/***
 * Integration tests for{@link Converters} testing all known codecs with pre-defined values and {@code null} values.
 */
class JdbcLobIntegrationTest extends AbstractIntegrationTestSupport
{
    /**
    *
    */
    static byte[] ALL_BYTES = new byte[-(-128) + 127];

    static
    {
        Hooks.onOperatorDebug();

        for (int i = -128; i < 127; i++)
        {
            ALL_BYTES[-(-128) + i] = (byte) i;
        }
    }

    /**
     * @param connection {@link JdbcConnection}
     * @param columnType String
     */
    void createTable(final JdbcConnection connection, final String columnType)
    {
        // @formatter:off
        connection.createStatement("DROP TABLE IF EXISTS lob_test")
            .execute()
            .flatMap(Result::getRowsUpdated)
            .onErrorResume(e -> Mono.empty())
            .thenMany(connection.createStatement("CREATE TABLE lob_test (my_col " + columnType + ")")
                    .execute()
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
    @Disabled
    void testBigBlob()
    {
        createTable(getConnection(), "BLOB");
        // getJdbcOperations().execute("DROP TABLE IF EXISTS lob_test");
        // getJdbcOperations().execute("CREATE TABLE lob_test (my_col BLOB)");

        int i = 50 + new Random().nextInt(1);

        // @formatter:off
        Flux.from(getConnection().createStatement("INSERT INTO lob_test values(?)")
                .bind(0, Blob.from(Flux.range(0, i).map(it -> ByteBuffer.wrap(ALL_BYTES))))
                .execute()
                )
                .flatMap(Result::getRowsUpdated)
                .concatWith(TestKit.close(getConnection()))
                .as(StepVerifier::create)
                //.expectNext(1)
                .expectNextCount(1).as("blobs inserted")
                .verifyComplete();

        getConnection().createStatement("SELECT my_col FROM lob_test")
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row.get("my_col", Blob.class))
                    )
            .flatMap(Blob::stream)
            .doOnNext(it -> System.out.println(it.remaining()))
            .map(Buffer::remaining)
            .collect(Collectors.summingInt(value -> value))
            .concatWith(TestKit.close(getConnection()))
            .as(StepVerifier::create)
            .expectNext(i * ALL_BYTES.length)
            .verifyComplete();
        // @formatter:on
    }

    /**
    *
    */
    @Test
    @Disabled
    void testBigClob()
    {
        createTable(getConnection(), "CLOB");
        // getJdbcOperations().execute("DROP TABLE IF EXISTS lob_test");
        // getJdbcOperations().execute("CREATE TABLE lob_test (my_col CLOB)");

        int i = 50 + new Random().nextInt(100);

        String TEST_STRING = "foo你好bar";

        // @formatter:off
        Flux.from(getConnection().createStatement("INSERT INTO lob_test values(?)")
                .bind(0, Clob.from(Flux.range(0, i).map(it -> TEST_STRING)))
                .execute())
                .flatMap(Result::getRowsUpdated)
                .concatWith(TestKit.close(getConnection()))
                .as(StepVerifier::create)
                //.expectNext(1)
                .expectNextCount(1).as("clobs inserted")
                .verifyComplete();

        getConnection().createStatement("SELECT my_col FROM lob_test")
                .execute()
                .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Clob.class)))
                .flatMap(Clob::stream)
                .doOnNext(it -> System.out.println(it.toString()))
                .map(CharSequence::length)
                .collect(Collectors.summingInt(value -> value))
                .concatWith(TestKit.close(getConnection()))
                .as(StepVerifier::create)
                .expectNext(i * TEST_STRING.length())
                .verifyComplete();
        // @formatter:on
    }

    // /**
    // *
    // */
    // @Test
    // void testNullBlob()
    // {
    // createTable(getConnection(), "IMAGE");
    //
    // Flux.from(getConnection().createStatement("INSERT INTO lob_test values($1)").bindNull("$1", Blob.class).execute()).flatMap(Result::getRowsUpdated)
    // .as(StepVerifier::create).expectNext(1).verifyComplete();
    //
    // getConnection().createStatement("SELECT my_col FROM lob_test").execute()
    // .flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable(row.get("my_col", Blob.class)))).as(StepVerifier::create)
    // .consumeNextWith(actual -> assertThat(actual).isEqualTo(Optional.empty())).verifyComplete();
    // }

    // /**
    // *
    // */
    // @Test
    // void testNullClob()
    // {
    // createTable(getConnection(), "NTEXT");
    //
    // Flux.from(getConnection().createStatement("INSERT INTO lob_test values($1)").bindNull("$1", Clob.class).execute()).flatMap(Result::getRowsUpdated)
    // .as(StepVerifier::create).expectNext(1).verifyComplete();
    //
    // getConnection().createStatement("SELECT my_col FROM lob_test").execute()
    // .flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable(row.get("my_col", Clob.class)))).as(StepVerifier::create)
    // .consumeNextWith(actual -> assertThat(actual).isEqualTo(Optional.empty())).verifyComplete();
    // }

    // /**
    // *
    // */
    // @Test
    // void testSmallBlob()
    // {
    // createTable(getConnection(), "IMAGE");
    //
    // Flux.from(getConnection().createStatement("INSERT INTO lob_test values($1)").bind("$1", Blob.from(Mono.just("foo".getBytes()).map(ByteBuffer::wrap)))
    // .execute()).flatMap(Result::getRowsUpdated).as(StepVerifier::create).expectNext(1).verifyComplete();
    //
    // getConnection().createStatement("SELECT my_col FROM lob_test").execute().flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Blob.class)))
    // .flatMap(Blob::stream).as(StepVerifier::create).consumeNextWith(actual -> assertThat(actual).isEqualTo(ByteBuffer.wrap("foo".getBytes())))
    // .verifyComplete();
    // }

    // /**
    // *
    // */
    // @Test
    // void testSmallClob()
    // {
    // createTable(getConnection(), "NTEXT");
    //
    // Flux.from(getConnection().createStatement("INSERT INTO lob_test values($1)").bind("$1", Clob.from(Mono.just("foo你好"))).execute())
    // .flatMap(Result::getRowsUpdated).as(StepVerifier::create).expectNext(1).verifyComplete();
    //
    // getConnection().createStatement("SELECT my_col FROM lob_test").execute().flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Clob.class)))
    // .flatMap(Clob::stream).as(StepVerifier::create).consumeNextWith(actual -> assertThat(actual).isEqualTo("foo你好")).verifyComplete();
    // }
}
