/*
 * Copyright 2018 the original author or authors. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package io.r2dbc.jdbc;

import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcOperations;
import io.r2dbc.jdbc.util.HsqldbServerExtension;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.test.Example;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author Thomas Freese
 */
final class JdbcExampleTest
{
    /**
     * @author Thomas Freese
     */
    @Nested
    @Disabled
    final class NamedParameterStyle implements Example<Integer>
    {
        /**
         * @see io.r2dbc.spi.test.Example#getConnectionFactory()
         */
        @Override
        public ConnectionFactory getConnectionFactory()
        {
            return JdbcExampleTest.this.connectionFactory;
        }

        /**
         * @see io.r2dbc.spi.test.Example#getIdentifier(int)
         */
        @Override
        public Integer getIdentifier(final int index)
        {
            // return getPlaceholder(index);
            return index + 1;
        }

        /**
         * @see io.r2dbc.spi.test.Example#getJdbcOperations()
         */
        @Override
        public JdbcOperations getJdbcOperations()
        {

            return JdbcExampleTest.this.getJdbcOperations();
        }

        /**
         * @see io.r2dbc.spi.test.Example#getPlaceholder(int)
         */
        @Override
        public String getPlaceholder(final int index)
        {
            return "?";
        }
    }

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
    @BeforeEach
    void createTable()
    {
        getJdbcOperations().execute("CREATE TABLE test ( value INTEGER )");
        // getJdbcOperations().execute("CREATE TABLE test_auto ( id INTEGER IDENTITY, value INTEGER);");
        getJdbcOperations().execute("CREATE TABLE test_auto ( id INTEGER GENERATED BY DEFAULT AS IDENTITY(START WITH 1, INCREMENT BY 1), value INTEGER);");
        // getJdbcOperations().execute("CREATE TABLE test_auto ( id INTEGER AUTO_INCREMENT PRIMARY KEY, value INTEGER);");
    }

    /**
    *
    */
    @AfterEach
    void dropTable()
    {
        getJdbcOperations().execute("DROP TABLE test");
        getJdbcOperations().execute("DROP TABLE test_auto");
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
    void prepareStatementInsert()
    {
       // @formatter:off
       Mono.from(this.connectionFactory.create())
           .flatMapMany(connection -> {
               Statement statement = connection.createStatement("INSERT INTO test VALUES (?)")
                       .bind(1, 2)
                       .add();

               return Flux.from(statement.execute())
                       .concatWith(Example.close(connection))
                       .flatMap(result -> Flux.from(result.map((row, rowMetadata) -> row.get(0, Integer.class)))
                               .collectList());
           })
           .as(StepVerifier::create)
           .expectNext(List.of(1))
           .as("value from insertion")
           .verifyComplete();
       // @formatter:on
    }

    /**
     *
     */
    @Test
    void prepareStatementInsertBatch()
    {
        // @formatter:off
        Mono.from(this.connectionFactory.create())
            .flatMapMany(connection -> {
                Statement statement = connection.createStatement("INSERT INTO test VALUES (?)");

                IntStream.range(0, 10).forEach(i -> statement.bind(1, i).add());

                return Flux.from(statement.execute())
                        .concatWith(Example.close(connection))
                        .flatMap(result -> Flux.from(result.map((row, rowMetadata) -> row.get(0, Integer.class)))
                                .collectList());
            })
            .as(StepVerifier::create)
            .expectNext(List.of(1, 1, 1, 1, 1, 1, 1, 1, 1, 1))
            .as("values from insertions")
            .verifyComplete();
        // @formatter:on
    }

    /**
    *
    */
    @Test
    void prepareStatementInsertBatchAutoIncrement()
    {
        // @formatter:off
        Mono.from(this.connectionFactory.create())
            .flatMapMany(connection -> {
                Statement statement = connection.createStatement("INSERT INTO test_auto (value) VALUES (?)");

                IntStream.range(0, 10).forEach(i -> statement.bind(1, i).add());

                return Flux.from(statement.returnGeneratedValues().execute())
                        .concatWith(Example.close(connection))
                        .flatMap(result -> Flux.from(result.map((row, rowMetadata) -> row.get(0, Integer.class)))
                                .collectList());
            })
            .as(StepVerifier::create)
            .expectNext(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
            .as("values from insertions")
            .verifyComplete();
        // @formatter:on
    }

    /**
    *
    */
    @Test
    @Disabled
    void prepareStatementInsertBatchExpectNextCount()
    {
        Mono.from(this.connectionFactory.create()).flatMapMany(connection -> {
            Statement statement = connection.createStatement("INSERT INTO test VALUES (?)");

            IntStream.range(0, 10).forEach(i -> statement.bind(1, i).add());

            return Flux.from(statement.execute()).concatWith(Example.close(connection));
        }).as(StepVerifier::create).expectNextCount(10).as("values from insertions").verifyComplete();
    }
}