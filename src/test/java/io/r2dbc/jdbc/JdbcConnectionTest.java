// Created: 14.06.2019
package io.r2dbc.jdbc;

import static io.r2dbc.spi.IsolationLevel.READ_UNCOMMITTED;
import static io.r2dbc.spi.IsolationLevel.REPEATABLE_READ;
import static io.r2dbc.spi.IsolationLevel.SERIALIZABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
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

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.jdbc.codecs.DefaultCodecs;
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
    private final Codecs codecs = new DefaultCodecs();

    /**
     *
     */
    private final Connection connection = mock(Connection.class, RETURNS_SMART_NULLS);

    /**
     *
     */
    @Test
    void testBeginTransaction()
    {
        new JdbcConnection(this.connection, this.codecs).beginTransaction().as(StepVerifier::create).verifyComplete();
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @SuppressWarnings("resource")
    @Test
    void testBeginTransactionErrorResponse() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(true);
        doThrow(new SQLNonTransientConnectionException("Unable to disable autocommits", "some state", 999)).when(this.connection)
                .setAutoCommit(ArgumentMatchers.anyBoolean());

        new JdbcConnection(this.connection, this.codecs).beginTransaction().as(StepVerifier::create)
                .verifyErrorMatches(R2dbcNonTransientResourceException.class::isInstance);
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void testBeginTransactionInTransaction() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(true);
        verifyNoMoreInteractions(this.connection);

        new JdbcConnection(this.connection, this.codecs).beginTransaction().as(StepVerifier::create).verifyComplete();
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void testClose() throws SQLException
    {
        when(this.connection.isClosed()).thenReturn(false);

        new JdbcConnection(this.connection, this.codecs).close().as(StepVerifier::create).verifyComplete();
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void testCloseWhenClosed() throws SQLException
    {
        when(this.connection.isClosed()).thenReturn(true);
        verifyNoMoreInteractions(this.connection);

        new JdbcConnection(this.connection, this.codecs).close().as(StepVerifier::create).verifyComplete();
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void testCommitTransaction() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(false);

        new JdbcConnection(this.connection, this.codecs).commitTransaction().as(StepVerifier::create).verifyComplete();
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @SuppressWarnings("resource")
    @Test
    void testCommitTransactionErrorResponse() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(false);
        doThrow(new SQLTransactionRollbackException("can't commit", "some state", 999)).when(this.connection).commit();

        new JdbcConnection(this.connection, this.codecs).commitTransaction().as(StepVerifier::create)
                .verifyErrorMatches(R2dbcRollbackException.class::isInstance);
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void testCommitTransactionNonOpen() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(true);
        verifyNoMoreInteractions(this.connection);

        new JdbcConnection(this.connection, this.codecs).commitTransaction().as(StepVerifier::create).verifyComplete();
    }

    /**
     *
     */
    @Test
    void testConstructorNoClient()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcConnection(null, null)).withMessage("connection must not be null");
    }

    /**
     *
     */
    @Test
    void testCreateBatch()
    {
        assertThat(new JdbcConnection(this.connection, this.codecs).createBatch()).isInstanceOf(JdbcBatch.class);
        // assertThatThrownBy(jdbcConnection::createBatch).isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     *
     */
    @Test
    void testCreateSavepoint()
    {
        new JdbcConnection(this.connection, this.codecs).createSavepoint("test").as(StepVerifier::create).verifyComplete();
    }

    /**
     *
     */
    @Test
    void testCreateSavepointNoName()
    {
        // new JdbcConnection(this.connection).createSavepoint(null).as(StepVerifier::create).verifyErrorMatches(IllegalArgumentException.class::isInstance);

        assertThatIllegalArgumentException().isThrownBy(() -> new JdbcConnection(this.connection, this.codecs).createSavepoint(null));
    }

    /**
     *
     */
    @Test
    void testCreateStatement()
    {
        assertThat(new JdbcConnection(this.connection, this.codecs).createStatement("select-query-?")).isInstanceOf(JdbcStatement.class);
        assertThat(new JdbcConnection(this.connection, this.codecs).createStatement("insert-query-?")).isInstanceOf(JdbcStatement.class);
        assertThat(new JdbcConnection(this.connection, this.codecs).createStatement("update-query-?")).isInstanceOf(JdbcStatement.class);
        assertThat(new JdbcConnection(this.connection, this.codecs).createStatement("delete-query-?")).isInstanceOf(JdbcStatement.class);

        // assertThatThrownBy(() -> new JdbcConnection(this.connection,codecs).createStatement("some-query-?")).isInstanceOf(R2dbcBadGrammarException.class);
    }

    /**
     *
     */
    @Test
    void testReleaseSavepoint()
    {
        // new JdbcConnection(this.connection).releaseSavepoint("test").as(StepVerifier::create)
        // .verifyErrorMatches(JdbcR2dbcNonTransientException.class::isInstance);
        new JdbcConnection(this.connection, this.codecs).releaseSavepoint("test").as(StepVerifier::create).verifyComplete();
    }

    /**
     *
     */
    @Test
    void testReleaseSavepointNoName()
    {
        // new JdbcConnection(this.connection,codecs).releaseSavepoint(null).as(StepVerifier::create)
        // .verifyErrorMatches(JdbcR2dbcNonTransientException.class::isInstance);
        // new
        // JdbcConnection(this.connection,codecs).releaseSavepoint(null).as(StepVerifier::create).verifyErrorMatches(IllegalArgumentException.class::isInstance);

        assertThatIllegalArgumentException().isThrownBy(() -> new JdbcConnection(this.connection, this.codecs).releaseSavepoint(null));
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void testRollbackTransaction() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(false);

        new JdbcConnection(this.connection, this.codecs).rollbackTransaction().as(StepVerifier::create).verifyComplete();
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @SuppressWarnings("resource")
    @Test
    void testRollbackTransactionErrorResponse() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(false);
        doThrow(new SQLTransactionRollbackException("can't commit", "some state", 999)).when(this.connection).rollback();

        new JdbcConnection(this.connection, this.codecs).rollbackTransaction().as(StepVerifier::create)
                .verifyErrorMatches(R2dbcRollbackException.class::isInstance);
    }

    /**
     * @throws SQLException Falls was schief geht.
     */
    @Test
    void testRollbackTransactionNonOpen() throws SQLException
    {
        when(this.connection.getAutoCommit()).thenReturn(true);
        verifyNoMoreInteractions(this.connection);

        new JdbcConnection(this.connection, this.codecs).rollbackTransaction().as(StepVerifier::create).verifyComplete();
    }

    /**
     *
     */
    @Test
    void testRollbackTransactionToSavepoint()
    {
        // new JdbcConnection(this.connection).rollbackTransactionToSavepoint("test").as(StepVerifier::create)
        // .verifyErrorMatches(JdbcR2dbcNonTransientException.class::isInstance);
        new JdbcConnection(this.connection, this.codecs).rollbackTransactionToSavepoint("test").as(StepVerifier::create).verifyComplete();
    }

    /**
     *
     */
    @Test
    void testRollbackTransactionToSavepointNoName()
    {
        // new JdbcConnection(this.connection).rollbackTransactionToSavepoint(null).as(StepVerifier::create)
        // .verifyErrorMatches(JdbcR2dbcNonTransientException.class::isInstance);
        new JdbcConnection(this.connection, this.codecs).rollbackTransactionToSavepoint(null).as(StepVerifier::create)
                .verifyErrorMatches(NullPointerException.class::isInstance);
    }

    /**
     *
     */
    @Test
    void testSetTransactionIsolationLevelNoIsolationLevel()
    {
        // .expectError(NullPointerException.class)
        // .expectNextCount(1).verifyComplete();
        new JdbcConnection(this.connection, this.codecs).setTransactionIsolationLevel(null).as(StepVerifier::create)
                .verifyErrorMatches(NullPointerException.class::isInstance);
    }

    /**
     *
     */
    @Test
    void testSetTransactionIsolationLevelReadUncommitted()
    {
        // when(this.connection.getAutoCommit()).thenReturn(false);
        // when(this.client.execute("SET LOCK_MODE 0")).thenReturn(Mono.empty());

        new JdbcConnection(this.connection, this.codecs).setTransactionIsolationLevel(READ_UNCOMMITTED).as(StepVerifier::create).verifyComplete();
    }

    /**
     *
     */
    @Test
    void testSetTransactionIsolationLevelRepeatableRead()
    {
        // when(this.connection.getAutoCommit()).thenReturn(false);
        // when(this.client.execute("SET LOCK_MODE 1")).thenReturn(Mono.empty());

        new JdbcConnection(this.connection, this.codecs).setTransactionIsolationLevel(REPEATABLE_READ).as(StepVerifier::create).verifyComplete();
    }

    /**
     *
     */
    @Test
    void testSetTransactionIsolationLevelSerializable()
    {
        // when(this.connection.getAutoCommit()).thenReturn(false);
        // when(this.client.execute("SET LOCK_MODE 1")).thenReturn(Mono.empty());

        new JdbcConnection(this.connection, this.codecs).setTransactionIsolationLevel(SERIALIZABLE).as(StepVerifier::create).verifyComplete();
    }
}
