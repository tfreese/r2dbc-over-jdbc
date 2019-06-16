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
    final class JdbcStyle implements Example<Integer>
    {
        /**
         * @see io.r2dbc.spi.test.Example#batch()
         */
        @Override
        @Test
        @Disabled
        public void batch()
        {
            Example.super.batch();
        }

        /**
         * @see io.r2dbc.spi.test.Example#blobInsert()
         */
        @Override
        @Test
        @Disabled
        public void blobInsert()
        {
            Example.super.blobInsert();
        }

        /**
         * @see io.r2dbc.spi.test.Example#blobSelect()
         */
        @Override
        @Test
        @Disabled
        public void blobSelect()
        {
            Example.super.blobSelect();
        }

        /**
         * @see io.r2dbc.spi.test.Example#blobType()
         */
        @Override
        @Test
        @Disabled
        public String blobType()
        {
            return Example.super.blobType();
        }

        /**
         * @see io.r2dbc.spi.test.Example#clobInsert()
         */
        @Override
        @Test
        @Disabled
        public void clobInsert()
        {
            Example.super.clobInsert();
        }

        /**
         * @see io.r2dbc.spi.test.Example#clobSelect()
         */
        @Override
        @Test
        @Disabled
        public void clobSelect()
        {
            Example.super.clobSelect();
        }

        /**
         * @see io.r2dbc.spi.test.Example#clobType()
         */
        @Override
        @Test
        @Disabled
        public String clobType()
        {
            return Example.super.clobType();
        }

        /**
         * @see io.r2dbc.spi.test.Example#compoundStatement()
         */
        @Override
        @Test
        @Disabled
        public void compoundStatement()
        {
            Example.super.compoundStatement();
        }

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
            return index;
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

        /**
         * @see io.r2dbc.spi.test.Example#prepareStatement()
         */
        @Override
        @Test
        @Disabled
        public void prepareStatement()
        {
            Example.super.prepareStatement();
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
    void prepareStatementDelete()
    {
        getJdbcOperations().execute("INSERT INTO tbl VALUES (100)");

        // @formatter:off
        Mono.from(this.connectionFactory.create())
            .flatMapMany(connection -> Mono.from(connection.beginTransaction())

                    .<Object>thenMany(Flux.from(connection.createStatement("INSERT INTO tbl VALUES (?)")
                                .bind(0, 200)
                                .add().bind(0, 300)
                                .add().bind(0, 400)
                                .execute())
                            .flatMap(Example::extractRowsUpdated))

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(connection.commitTransaction())

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(connection.beginTransaction())

                    .concatWith(Flux.from(connection.createStatement("DELETE from tbl where value < ?")
                                .bind(0, 255)
                                .execute())
                            .flatMap(Example::extractRowsUpdated))

                    .concatWith(connection.commitTransaction())

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(Example.close(connection))
            )
            .as(StepVerifier::create)
            .expectNext(3).as("rows inserted")
            .expectNext(List.of(100, 200, 300, 400)).as("values from select before commit")
            .expectNext(List.of(100, 200, 300, 400)).as("values from select after commit")
            .expectNext(2).as("rows deleted")
            .expectNext(List.of(300, 400)).as("values from select after delete after commit")
            .verifyComplete()
            ;
       // @formatter:on
    }

    /**
    *
    */
    @Test
    void prepareStatementDeleteBatch()
    {
        getJdbcOperations().execute("INSERT INTO tbl VALUES (100, 200, 300, 400, 500)");

        // @formatter:off
        Mono.from(this.connectionFactory.create())
            .flatMapMany(connection -> Mono.from(connection.beginTransaction())

                    .<Object>thenMany(Flux.from(connection.createStatement("DELETE from tbl where value < ?")
                                .bind(0, 300)
                                .add().bind(0, 400)
                                .execute())
                            .flatMap(Example::extractRowsUpdated))

                    .concatWith(connection.commitTransaction())

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(Example.close(connection))
            )
            .as(StepVerifier::create)
            .expectNext(3).as("rows deleted")
            .expectNext(List.of(400, 500)).as("values from select after delete after commit")
            .verifyComplete()
            ;
       // @formatter:on
    }

    /**
    *
    */
    @Test
    void prepareStatementDeleteSimple()
    {
        getJdbcOperations().execute("INSERT INTO tbl VALUES (100, 200, 300, 400)");

        // @formatter:off
        Mono.from(this.connectionFactory.create())
            .flatMapMany(connection -> Mono.from(connection.beginTransaction())

                    .<Object>thenMany(Flux.from(connection.createStatement("DELETE from tbl where value < ?")
                                .bind(0, 255)
                                .execute())
                            .flatMap(Example::extractRowsUpdated))

                    .concatWith(connection.commitTransaction())

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(Example.close(connection))
            )
            .as(StepVerifier::create)
            .expectNext(2).as("rows deleted")
            .expectNext(List.of(300, 400)).as("values from select after delete after commit")
            .verifyComplete()
            ;
       // @formatter:on
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
               Statement statement = connection.createStatement("INSERT INTO tbl VALUES (?)")
                       .bind(0, 2)
                       .add()
                       .add()
                       .add()
                       ;

               return Flux.from(statement.execute())
                       .concatWith(Example.close(connection))
                       .flatMap(result -> Flux.from(result.map((row, rowMetadata) -> row.get(0, Integer.class)))
                               .collectList());
           })
           .as(StepVerifier::create)
           .expectNext(List.of(1)).as("value from insertion")
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
                Statement statement = connection.createStatement("INSERT INTO tbl VALUES (?)");

                IntStream.range(0, 10).forEach(i -> statement.bind(0, i).add());

                return Flux.from(statement.execute())
                        .concatWith(Example.close(connection))
                        .flatMap(result -> Flux.from(result.map((row, rowMetadata) -> row.get(0, Integer.class)))
                                .collectList());
            })
            .as(StepVerifier::create)
            .expectNext(List.of(1, 1, 1, 1, 1, 1, 1, 1, 1, 1)).as("values from insertions")
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
                Statement statement = connection.createStatement("INSERT INTO tbl_auto (value) VALUES (?)");

                IntStream.range(0, 10).forEach(i -> statement.bind(0, i).add());

                return Flux.from(statement.returnGeneratedValues().execute())
                        .concatWith(Example.close(connection))
                        .flatMap(result -> Flux.from(result.map((row, rowMetadata) -> row.get(0, Integer.class)))
                                .collectList());
            })
            .as(StepVerifier::create)
            .expectNext(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).as("values from insertions")
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
            Statement statement = connection.createStatement("INSERT INTO tbl VALUES (?)");

            IntStream.range(0, 10).forEach(i -> statement.bind(0, i).add());

            return Flux.from(statement.execute()).concatWith(Example.close(connection));
        }).as(StepVerifier::create).expectNextCount(10).as("values from insertions").verifyComplete();
    }

    /**
    *
    */
    @Test
    void prepareStatementInsertBatchWithCommit()
    {
        getJdbcOperations().execute("INSERT INTO tbl VALUES (100)");

        // @formatter:off
        Mono.from(this.connectionFactory.create())
            .flatMapMany(connection -> Mono.from(connection.beginTransaction())

                    .<Object>thenMany(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(Flux.from(connection.createStatement("INSERT INTO tbl VALUES (?)")
                                .bind(0, 200)
                                .execute())
                            .flatMap(Example::extractRowsUpdated))

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(connection.commitTransaction())

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(Example.close(connection))

            )
            .as(StepVerifier::create)
            .expectNext(List.of(100)).as("value from select")
            .expectNext(1).as("rows inserted")
            .expectNext(List.of(100, 200)).as("values from select before commit")
            .expectNext(List.of(100, 200)).as("values from select after commit")
            .verifyComplete()
            ;
       // @formatter:on
    }

    /**
    *
    */
    @Test
    void prepareStatementInsertBatchWithRollback()
    {
        getJdbcOperations().execute("INSERT INTO tbl VALUES (100)");

        // @formatter:off
        Mono.from(this.connectionFactory.create())
            .flatMapMany(connection -> Mono.from(connection.beginTransaction())

                    .<Object>thenMany(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(Flux.from(connection.createStatement("INSERT INTO tbl VALUES (?)")
                                .bind(0, 200)
                                .execute())
                            .flatMap(Example::extractRowsUpdated))

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(connection.rollbackTransaction())

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(Example.close(connection))
            )
            .as(StepVerifier::create)
            .expectNext(List.of(100)).as("value from select")
            .expectNext(1).as("rows inserted")
            .expectNext(List.of(100, 200)).as("values from select before rollback")
            .expectNext(List.of(100)).as("value from select after rollback")
            .verifyComplete()
            ;
       // @formatter:on
    }

    /**
    *
    */
    @Test
    void prepareStatementSelect()
    {
        getJdbcOperations().execute("INSERT INTO tbl VALUES (100)");

        // @formatter:off
        Mono.from(this.connectionFactory.create())
            .flatMapMany(connection -> Mono.from(connection.beginTransaction())

                    .<Object>thenMany(Flux.from(connection.createStatement("INSERT INTO tbl VALUES (?)")
                                .bind(0, 200)
                                .add().bind(0, 300)
                                .add().bind(0, 400)
                                .execute())
                            .flatMap(Example::extractRowsUpdated))

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(connection.commitTransaction())

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl where value < ?")
                                .bind(0, 250)
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl where value > ?")
                                .bind(0, 250)
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(Example.close(connection))
            )
            .as(StepVerifier::create)
            .expectNext(3).as("rows inserted")
            .expectNext(List.of(100, 200, 300, 400)).as("values from select before commit")
            .expectNext(List.of(100, 200, 300, 400)).as("values from select after commit")
            .expectNext(List.of(100, 200)).as("values from select where value < ?")
            .expectNext(List.of(300, 400)).as("values from select where value > ?")
            .verifyComplete()
            ;
       // @formatter:on
    }

    /**
    *
    */
    @Test
    void prepareStatementUpdate()
    {
        getJdbcOperations().execute("INSERT INTO tbl VALUES (100)");

        // @formatter:off
        Mono.from(this.connectionFactory.create())
            .flatMapMany(connection -> Mono.from(connection.beginTransaction())

                    .<Object>thenMany(Flux.from(connection.createStatement("INSERT INTO tbl VALUES (?)")
                                .bind(0, 200)
                                .add().bind(0, 300)
                                .add().bind(0, 400)
                                .execute())
                            .flatMap(Example::extractRowsUpdated))

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(connection.commitTransaction())

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(connection.beginTransaction())

                    .concatWith(Flux.from(connection.createStatement("UPDATE tbl set value = ? where value = ?")
                                .bind(0, 199).bind(1, 100)
                                .add().bind(0, 299).bind(1, 200)
                                .add().bind(0, 399).bind(1, 300)
                                .add().bind(0, 499).bind(1, 400)
                                .execute())
                            .flatMap(result -> Flux.from(result.map((row, rowMetadata) -> row.get(0, Integer.class)))).collectList())

                    .concatWith(connection.commitTransaction())

                    .concatWith(Flux.from(connection.createStatement("SELECT value FROM tbl")
                                .execute())
                            .flatMap(Example::extractColumns))

                    .concatWith(Example.close(connection))
            )
            .as(StepVerifier::create)
            .expectNext(3).as("rows inserted")
            .expectNext(List.of(100, 200, 300, 400)).as("values from select before commit")
            .expectNext(List.of(100, 200, 300, 400)).as("values from select after commit")
            //.expectNext(4).as("rows updated")
            .expectNext(List.of(1, 1, 1, 1)).as("values from update")
            .expectNext(List.of(199, 299, 399, 499)).as("values from select after update after commit")
            .verifyComplete()
            ;
       // @formatter:on
    }
}
