package io.r2dbc.jdbc;

import io.r2dbc.jdbc.util.HsqldbServerExtension;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.test.Example;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * @author Thomas Freese
 */
final class JdbcRowTest
{
    /**
     *
     */
    @RegisterExtension
    static final HsqldbServerExtension SERVER = new HsqldbServerExtension();

    /**
     *
     */
    private final ConnectionFactory connectionFactory =
            ConnectionFactories.get(ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, SERVER.getDataSource()).build());

    /**
     *
     */
    @Test
    void constructorNoValues()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcRow(null)).withMessage("values must not be null");
    }

    /**
     *
     */
    @BeforeEach
    void createTable()
    {
        getJdbcOperations().execute("CREATE TABLE test ( value INTEGER )");
    }

    /**
     *
     */
    @AfterEach
    void dropTable()
    {
        getJdbcOperations().execute("DROP TABLE test");
    }

    /**
     *
     */
    @Test
    void getByIndex()
    {
        Object value = new Object();

        Map<Object, Object> values = new HashMap<>();
        values.put("TEST-NAME-1", value);
        values.put(0, value);

        assertThat(new JdbcRow(values).get(0, Object.class)).isSameAs(value);
    }

    /**
     *
     */
    @Test
    void getByName()
    {
        Object value = new Object();

        Map<Object, Object> values = new HashMap<>();
        values.put("TEST-NAME-2", value);

        assertThat(new JdbcRow(values).get("test-name-2", Object.class)).isSameAs(value);
    }

    /**
     *
     */
    @Test
    void getInvalidIdentifier()
    {
        assertThatIllegalArgumentException().isThrownBy(() -> new JdbcRow(new HashMap<>()).get(3, Object.class))
                .withMessage("Column identifier '3' does not exist");
    }

    /**
     *
     */
    @Test
    void getNoIdentifier()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcRow(new HashMap<>()).get(null, Object.class)).withMessage("identifier must not be null");
    }

    /**
     *
     */
    @Test
    void getNull()
    {
        Map<Object, Object> values = new HashMap<>();
        values.put("TEST-NAME-3", null);

        assertThat(new JdbcRow(values).get("test-name-3", Object.class)).isNull();
    }

    /**
     *
     */
    @Test
    void getWrongIdentifierType()
    {
        Object identifier = new Object();

        assertThatIllegalArgumentException().isThrownBy(() -> new JdbcRow(new HashMap<>()).get(identifier, Object.class))
                .withMessage("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String column name.", identifier);
    }

    /**
     *
     */
    @Test
    void selectWithAliases()
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
                .concatWith(Example.close(connection)))
            .as(StepVerifier::create)
            .expectNext(Collections.singletonList(100))
            .verifyComplete();
        // @formatter:on
    }

    /**
     *
     */
    @Test
    void selectWithoutAliases()
    {
        getJdbcOperations().execute("INSERT INTO test VALUES (100)");

        // @formatter:off
        Mono.from(this.connectionFactory.create())
            .flatMapMany(connection -> Flux.from(connection.createStatement("SELECT value FROM test").execute())
                .flatMap(Example::extractColumns)
                .concatWith(Example.close(connection)))
        .as(StepVerifier::create)
        .expectNext(Collections.singletonList(100))
        .verifyComplete();
        // @formatter:on
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
}
