/**
 * Created: 22.06.2019
 */

package io.r2dbc.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcOperations;
import io.r2dbc.client.R2dbc;
import io.r2dbc.jdbc.util.HsqldbServerExtension;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * @author Thomas Freese
 */
final class JdbcR2dbcClientTest
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
    private final R2dbc r2dbc = new R2dbc(this.connectionFactory);

    /**
     *
     */
    @BeforeEach
    void createTable()
    {
        getJdbcOperations().execute("CREATE TABLE tbl ( value INTEGER )");
        // getJdbcOperations().execute("CREATE TABLE tbl_auto ( id INTEGER IDENTITY, value INTEGER);");
        getJdbcOperations().execute("CREATE TABLE tbl_auto ( id INTEGER GENERATED BY DEFAULT AS IDENTITY(START WITH 1, INCREMENT BY 1), value INTEGER);");
        // getJdbcOperations().execute("CREATE TABLE tbl_auto ( id INTEGER AUTO_INCREMENT, value INTEGER);");
    }

    /**
     *
     */
    @AfterEach
    void dropTable()
    {
        getJdbcOperations().execute("DROP TABLE tbl");
        getJdbcOperations().execute("DROP TABLE tbl_auto");
    }

    /**
     * @return {@link JdbcOperations}
     */
    public JdbcOperations getJdbcOperations()
    {
        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();

        if (jdbcOperations == null)
        {
            throw new IllegalStateException("JdbcOperations not yet initialized.");
        }

        return jdbcOperations;
    }

    /**
    *
    */
    @Test
    void insert()
    {
       // @formatter:off
       this.r2dbc.inTransaction(handle -> handle.execute("INSERT INTO tbl VALUES (?)", 200))
           .as(StepVerifier::create)
           .expectNext(1).as("value from insertion")
           .verifyComplete()
           ;
       // @formatter:on
    }

    /**
    *
    */
    @Test
    void insertBatch()
    {
       // @formatter:off
       this.r2dbc.inTransaction(handle -> handle.execute("INSERT INTO tbl VALUES (?)", 100)
               .concatWith(handle.execute("INSERT INTO tbl VALUES (?)", 200))
               .concatWith(handle.execute("INSERT INTO tbl VALUES (?)", 300))
               )
           .as(StepVerifier::create)
           .expectNext(1).as("value from insertion")
           .expectNext(1).as("value from insertion")
           .expectNext(1).as("value from insertion")
           .verifyComplete()
           ;
       // @formatter:on
    }

    /**
    *
    */
    @Test
    void insertWithSelect()
    {
        // @formatter:off
        this.r2dbc.inTransaction(handle -> handle.execute("INSERT INTO tbl VALUES (?)", 100))
            .concatWith(this.r2dbc.inTransaction(handle -> handle.select("SELECT value FROM tbl")
                .mapResult(result -> Flux.from(result.map((row, rowMetadata) -> row.get("value", Integer.class)))))
                )
            .as(StepVerifier::create)
            .expectNext(1).as("value from insertion")
            .expectNext(100).as("value from select")
            .verifyComplete()
//            .subscribe(System.out::println)
            ;
        // @formatter:o
    }
}