// Created: 14.06.2019
package io.r2dbc.jdbc;

import static io.r2dbc.jdbc.util.Awaits.awaitNone;
import static io.r2dbc.jdbc.util.Awaits.awaitQuery;
import static io.r2dbc.jdbc.util.Awaits.awaitUpdate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.sql.JDBCType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcOperations;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.jdbc.codecs.DefaultCodecs;
import io.r2dbc.jdbc.util.DBServerExtension;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.test.TestKit;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author Thomas Freese
 */
final class JdbcRowTest
{
    /**
     *
     */
    @RegisterExtension
    static final DBServerExtension SERVER = new DBServerExtension();

    /**
     *
     */
    private final Codecs codecs = new DefaultCodecs();

    /**
     *
     */
    private final ConnectionFactory connectionFactory =
            ConnectionFactories.get(ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, SERVER.getDataSource()).build());

    /**
    *
    */
    @AfterEach
    void afterEach()
    {
        getJdbcOperations().execute("DROP TABLE tbl");
    }

    /**
    *
    */
    @BeforeEach
    void beforeEach()
    {
        getJdbcOperations().execute("CREATE TABLE tbl ( value INTEGER )");
    }

    /**
     * @return {@link JdbcOperations}
     */
    private JdbcOperations getJdbcOperations()
    {
        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();

        if (jdbcOperations == null)
        {
            throw new IllegalStateException("JdbcOperations not yet initialized");
        }

        return jdbcOperations;
    }

    /**
     *
     */
    @Test
    void testConstructorNoValues()
    {
        JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("TEST-NAME-1", 0, Object.class, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        assertThatNullPointerException().isThrownBy(() -> new JdbcRow(null, new HashMap<>(), this.codecs)).withMessage("rowMetadata required");
        assertThatNullPointerException().isThrownBy(() -> new JdbcRow(rowMetadata, null, this.codecs)).withMessage("values required");
    }

    /**
     *
     */
    @Test
    void testGetByIndex()
    {
        JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("TEST-NAME-1", 0, Object.class, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        Object value = new Object();

        Map<Integer, Object> values = new HashMap<>();
        values.put(0, value);

        assertThat(new JdbcRow(rowMetadata, values, this.codecs).get(0, Object.class)).isSameAs(value);
    }

    /**
     *
     */
    @Test
    void testGetByName()
    {
        JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("TEST-NAME-2", 0, Object.class, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        Object value = new Object();

        Map<Integer, Object> values = new HashMap<>();
        values.put(0, value);

        assertThat(new JdbcRow(rowMetadata, values, this.codecs).get("test-name-2", Object.class)).isSameAs(value);
    }

    /**
     *
     */
    @Test
    void testGetInvalidIdentifier()
    {
        ColumnMetadata columnMetadata = new JdbcColumnMetadata("", 0, Object.class, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        RowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        assertThat(new JdbcRow(rowMetadata, new HashMap<>(), this.codecs).get(3, Object.class)).isNull();
    }

    /**
     *
     */
    @Test
    void testGetNoIdentifier()
    {
        JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("", 0, Object.class, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        assertThatIllegalArgumentException().isThrownBy(() -> new JdbcRow(rowMetadata, new HashMap<>(), this.codecs).get(null, Object.class))
                .withMessage("name is null");
    }

    /**
     *
     */
    @Test
    void testGetNull()
    {
        JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("TEST-NAME-3", 0, Object.class, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        Map<Integer, Object> values = new HashMap<>();
        values.put(0, null);

        assertThat(new JdbcRow(rowMetadata, values, this.codecs).get("test-name-3", Object.class)).isNull();
    }

    /**
     *
     */
    @Test
    void testGetWrongIdentifierType()
    {
        String identifier = "-";

        JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("", 0, Object.class, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        assertThatExceptionOfType(NoSuchElementException.class)
                .isThrownBy(() -> new JdbcRow(rowMetadata, new HashMap<>(), this.codecs).get(identifier, Object.class))
                .withMessage("No MetaData for Name: %s", identifier);
    }

    /**
     *
     */
    @Test
    void testSelectWithAliases()
    {
        getJdbcOperations().execute("INSERT INTO tbl VALUES (100)");

        // @formatter:off
        Mono.from(this.connectionFactory.create())
            .flatMapMany(connection -> Flux.from(connection .createStatement("SELECT value as ALIASED_VALUE FROM tbl").execute())
                .flatMap(result -> Flux.from(result.map((row, rowMetadata) ->
                            row.get("ALIASED_VALUE", Integer.class)
                            )
                        )
                        .collectList())
                .concatWith(TestKit.close(connection)))
            .as(StepVerifier::create)
            .expectNext(List.of(100))
            .verifyComplete();
        // @formatter:on
    }

    /**
    *
    */
    @Test
    void testSelectWithoutAliases()
    {
        // getJdbcOperations().execute("INSERT INTO tbl VALUES (100)");
        //
        // @formatter:off
//        Mono.from(this.connectionFactory.create())
//            .flatMapMany(connection -> Flux.from(connection.createStatement("SELECT value FROM tbl").execute())
//                .flatMap(TestKit::extractColumns)
//                .concatWith(TestKit.close(connection)))
//        .as(StepVerifier::create)
//        .expectNext(List.of(100))
//        .verifyComplete();
        // @formatter:on

        Connection connection = Mono.from(this.connectionFactory.create()).block(DBServerExtension.getSqlTimeout());

        try
        {
            // awaitExecution(connection.createStatement("CREATE TABLE tbl ( value INTEGER )"));

            awaitUpdate(1, connection.createStatement("INSERT INTO tbl VALUES (100)"));

            awaitQuery(List.of(100), row -> row.get(0, Integer.class), connection.createStatement("SELECT value FROM tbl"));
            awaitQuery(List.of(100), row -> row.get("value", Integer.class), connection.createStatement("SELECT value FROM tbl"));
        }
        finally
        {
            awaitNone(connection.close());
        }
    }
}
