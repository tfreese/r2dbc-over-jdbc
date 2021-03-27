// Created: 14.06.2019
package io.r2dbc.jdbc;

import static io.r2dbc.jdbc.util.Awaits.awaitNone;
import static io.r2dbc.jdbc.util.Awaits.awaitQuery;
import static io.r2dbc.jdbc.util.Awaits.awaitUpdate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.sql.JDBCType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcOperations;

import io.r2dbc.jdbc.util.DBServerExtension;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Nullability;
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
    private final ConnectionFactory connectionFactory =
            ConnectionFactories.get(ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, SERVER.getDataSource()).build());

    /**
     *
     */
    @AfterEach
    void afterEach()
    {
        getJdbcOperations().execute("DROP TABLE test");
    }

    /**
     *
     */
    @BeforeEach
    void beforeEach()
    {
        getJdbcOperations().execute("CREATE TABLE test ( value INTEGER )");
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
        JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("TEST-NAME-1", 0, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        assertThatNullPointerException().isThrownBy(() -> new JdbcRow(null, new HashMap<>())).withMessage("rowMetadata required");
        assertThatNullPointerException().isThrownBy(() -> new JdbcRow(rowMetadata, null)).withMessage("values required");
    }

    /**
     *
     */
    @Test
    void testGetByIndex()
    {
        JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("TEST-NAME-1", 0, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        Object value = new Object();

        Map<Object, Object> values = new HashMap<>();
        values.put("TEST-NAME-1", value);
        values.put(0, value);

        assertThat(new JdbcRow(rowMetadata, values).get(0, Object.class)).isSameAs(value);
    }

    /**
     *
     */
    @Test
    void testGetByName()
    {
        JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("TEST-NAME-2", 0, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        Object value = new Object();

        Map<Object, Object> values = new HashMap<>();
        values.put("TEST-NAME-2", value);

        assertThat(new JdbcRow(rowMetadata, values).get("test-name-2", Object.class)).isSameAs(value);
    }

    /**
     *
     */
    @Test
    void testGetInvalidIdentifier()
    {
        JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("", 0, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        assertThatIllegalArgumentException().isThrownBy(() -> new JdbcRow(rowMetadata, new HashMap<>()).get(3, Object.class))
                .withMessage("Column identifier '3' does not exist");
    }

    /**
     *
     */
    @Test
    void testGetNoIdentifier()
    {
        JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("", 0, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        assertThatNullPointerException().isThrownBy(() -> new JdbcRow(rowMetadata, new HashMap<>()).get(null, Object.class))
                .withMessage("identifier must not be null");
    }

    /**
     *
     */
    @Test
    void testGetNull()
    {
        JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("TEST-NAME-3", 0, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        Map<Object, Object> values = new HashMap<>();
        values.put("TEST-NAME-3", null);

        assertThat(new JdbcRow(rowMetadata, values).get("test-name-3", Object.class)).isNull();
    }

    /**
     *
     */
    @Test
    void testGetWrongIdentifierType()
    {
        String identifier = "-";

        JdbcColumnMetadata columnMetadata = new JdbcColumnMetadata("", 0, JDBCType.OTHER, Nullability.UNKNOWN, 0, 0);
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(List.of(columnMetadata));

        assertThatIllegalArgumentException().isThrownBy(() -> new JdbcRow(rowMetadata, new HashMap<>()).get(identifier, Object.class))
                .withMessage("Column identifier '%s' does not exist", identifier);
    }

    /**
     *
     */
    @Test
    void testSelectWithAliases()
    {
        getJdbcOperations().execute("INSERT INTO test VALUES (100)");

        // @formatter:off
        Mono.from(this.connectionFactory.create())
            .flatMapMany(connection -> Flux.from(connection .createStatement("SELECT value as ALIASED_VALUE FROM test").execute())
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
        // getJdbcOperations().execute("INSERT INTO test VALUES (100)");
        //
        // @formatter:off
//        Mono.from(this.connectionFactory.create())
//            .flatMapMany(connection -> Flux.from(connection.createStatement("SELECT value FROM test").execute())
//                .flatMap(TestKit::extractColumns)
//                .concatWith(TestKit.close(connection)))
//        .as(StepVerifier::create)
//        .expectNext(List.of(100))
//        .verifyComplete();
        // @formatter:on

        Connection connection = Mono.from(this.connectionFactory.create()).block(DBServerExtension.getSqlTimeout());

        try
        {
            // awaitExecution(connection.createStatement("CREATE TABLE test ( value INTEGER )"));

            awaitUpdate(1, connection.createStatement("INSERT INTO test VALUES (100)"));

            awaitQuery(List.of(100), row -> row.get(0, Integer.class), connection.createStatement("SELECT value FROM test"));
            awaitQuery(List.of(100), row -> row.get("value", Integer.class), connection.createStatement("SELECT value FROM test"));
        }
        finally
        {
            awaitNone(connection.close());
        }
    }
}
