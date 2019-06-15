/*
 * Copyright 2018 the original author or authors. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package io.r2dbc.jdbc;

import static io.r2dbc.spi.IsolationLevel.READ_UNCOMMITTED;
import static io.r2dbc.spi.IsolationLevel.REPEATABLE_READ;
import static io.r2dbc.spi.IsolationLevel.SERIALIZABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransactionRollbackException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcRollbackException;
import reactor.test.StepVerifier;

/**
 * @author Thomas Freese
 */
final class JdbcConnectionTest
{
    /**
     *
     */
    private final Connection connection = mock(Connection.class, RETURNS_SMART_NULLS);

    /**
     *
     */
    @Test
    void beginTransaction()
    {
        new JdbcConnection(this.connection).beginTransaction().as(StepVerifier::create).verifyComplete();
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void beginTransactionErrorResponse() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(true);
        doThrow(new SQLNonTransientConnectionException("Unable to disable autocommits", "some state", 999)).when(this.connection)
                .setAutoCommit(ArgumentMatchers.anyBoolean());

        new JdbcConnection(this.connection).beginTransaction().as(StepVerifier::create)
                .verifyErrorMatches(R2dbcNonTransientResourceException.class::isInstance);
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void beginTransactionInTransaction() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(true);
        verifyNoMoreInteractions(this.connection);

        new JdbcConnection(this.connection).beginTransaction().as(StepVerifier::create).verifyComplete();
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void close() throws SQLException
    {
        when(this.connection.isClosed()).thenReturn(false);

        new JdbcConnection(this.connection).close().as(StepVerifier::create).verifyComplete();
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void closeWhenClosed() throws SQLException
    {
        when(this.connection.isClosed()).thenReturn(true);
        verifyNoMoreInteractions(this.connection);

        new JdbcConnection(this.connection).close().as(StepVerifier::create).verifyComplete();
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void commitTransaction() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(false);

        new JdbcConnection(this.connection).commitTransaction().as(StepVerifier::create).verifyComplete();
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void commitTransactionErrorResponse() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(false);
        doThrow(new SQLTransactionRollbackException("can't commit", "some state", 999)).when(this.connection).commit();

        new JdbcConnection(this.connection).commitTransaction().as(StepVerifier::create).verifyErrorMatches(R2dbcRollbackException.class::isInstance);
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void commitTransactionNonOpen() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(true);
        verifyNoMoreInteractions(this.connection);

        new JdbcConnection(this.connection).commitTransaction().as(StepVerifier::create).verifyComplete();
    }

    /**
     *
     */
    @Test
    void constructorNoClient()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcConnection(null)).withMessage("connection must not be null");
    }

    /**
     *
     */
    @Test
    void createBatch()
    {
        JdbcConnection jdbcConnection = new JdbcConnection(this.connection);

        // assertThat(new JdbcConnection(connection).createBatch()).isInstanceOf(JdbcBatch.class);
        assertThatThrownBy(jdbcConnection::createBatch).isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     *
     */
    @Test
    void createSavepoint()
    {
        // new JdbcConnection(this.connection).createSavepoint("test").as(StepVerifier::create)
        // .verifyErrorMatches(JdbcR2dbcNonTransientException.class::isInstance);
        new JdbcConnection(this.connection).createSavepoint("test").as(StepVerifier::create).verifyComplete();
    }

    /**
     *
     */
    @Test
    void createSavepointNoName()
    {
        // new
        // JdbcConnection(this.connection).createSavepoint(null).as(StepVerifier::create).verifyErrorMatches(JdbcR2dbcNonTransientException.class::isInstance);
        new JdbcConnection(this.connection).createSavepoint(null).as(StepVerifier::create).verifyErrorMatches(NullPointerException.class::isInstance);
    }

    /**
     *
     */
    @Test
    void createStatement()
    {
        assertThat(new JdbcConnection(this.connection).createStatement("test-query-?")).isInstanceOf(AbstractJdbcStatement.class);
    }

    /**
     *
     */
    @Test
    void releaseSavepoint()
    {
        // new JdbcConnection(this.connection).releaseSavepoint("test").as(StepVerifier::create)
        // .verifyErrorMatches(JdbcR2dbcNonTransientException.class::isInstance);
        new JdbcConnection(this.connection).releaseSavepoint("test").as(StepVerifier::create).verifyComplete();
    }

    /**
     *
     */
    @Test
    void releaseSavepointNoName()
    {
        // new JdbcConnection(this.connection).releaseSavepoint(null).as(StepVerifier::create)
        // .verifyErrorMatches(JdbcR2dbcNonTransientException.class::isInstance);
        new JdbcConnection(this.connection).releaseSavepoint(null).as(StepVerifier::create).verifyErrorMatches(NullPointerException.class::isInstance);
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void rollbackTransaction() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(false);

        new JdbcConnection(this.connection).rollbackTransaction().as(StepVerifier::create).verifyComplete();
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void rollbackTransactionErrorResponse() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(false);
        doThrow(new SQLTransactionRollbackException("can't commit", "some state", 999)).when(this.connection).rollback();

        new JdbcConnection(this.connection).rollbackTransaction().as(StepVerifier::create).verifyErrorMatches(R2dbcRollbackException.class::isInstance);
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void rollbackTransactionNonOpen() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(true);
        verifyNoMoreInteractions(this.connection);

        new JdbcConnection(this.connection).rollbackTransaction().as(StepVerifier::create).verifyComplete();
    }

    /**
     *
     */
    @Test
    void rollbackTransactionToSavepoint()
    {
        // new JdbcConnection(this.connection).rollbackTransactionToSavepoint("test").as(StepVerifier::create)
        // .verifyErrorMatches(JdbcR2dbcNonTransientException.class::isInstance);
        new JdbcConnection(this.connection).rollbackTransactionToSavepoint("test").as(StepVerifier::create).verifyComplete();
    }

    /**
     *
     */
    @Test
    void rollbackTransactionToSavepointNoName()
    {
        // new JdbcConnection(this.connection).rollbackTransactionToSavepoint(null).as(StepVerifier::create)
        // .verifyErrorMatches(JdbcR2dbcNonTransientException.class::isInstance);
        new JdbcConnection(this.connection).rollbackTransactionToSavepoint(null).as(StepVerifier::create)
                .verifyErrorMatches(NullPointerException.class::isInstance);
    }

    /**
     *
     */
    @Test
    void setTransactionIsolationLevelNoIsolationLevel()
    {
        // .expectError(NullPointerException.class)
        // .expectNextCount(1).verifyComplete();
        new JdbcConnection(this.connection).setTransactionIsolationLevel(null).as(StepVerifier::create)
                .verifyErrorMatches(NullPointerException.class::isInstance);
    }

    /**
     *
     */
    @Test
    void setTransactionIsolationLevelReadUncommitted()
    {
        // when(this.connection.getAutoCommit()).thenReturn(false);
        // when(this.client.execute("SET LOCK_MODE 0")).thenReturn(Mono.empty());

        new JdbcConnection(this.connection).setTransactionIsolationLevel(READ_UNCOMMITTED).as(StepVerifier::create).verifyComplete();
    }

    /**
     *
     */
    @Test
    void setTransactionIsolationLevelRepeatableRead()
    {
        // when(this.connection.getAutoCommit()).thenReturn(false);
        // when(this.client.execute("SET LOCK_MODE 1")).thenReturn(Mono.empty());

        new JdbcConnection(this.connection).setTransactionIsolationLevel(REPEATABLE_READ).as(StepVerifier::create).verifyComplete();
    }

    /**
     *
     */
    @Test
    void setTransactionIsolationLevelSerializable()
    {
        // when(this.connection.getAutoCommit()).thenReturn(false);
        // when(this.client.execute("SET LOCK_MODE 1")).thenReturn(Mono.empty());

        new JdbcConnection(this.connection).setTransactionIsolationLevel(SERIALIZABLE).as(StepVerifier::create).verifyComplete();
    }
}
