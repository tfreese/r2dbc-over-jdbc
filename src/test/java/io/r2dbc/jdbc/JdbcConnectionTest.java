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

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.jdbc.codecs.DefaultCodecs;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcRollbackException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import reactor.test.StepVerifier;

/**
 * @author Thomas Freese
 */
final class JdbcConnectionTest {
    private final Codecs codecs = new DefaultCodecs();
    private final Connection connection = mock(Connection.class, RETURNS_SMART_NULLS);

    @Test
    void testBeginTransaction() {
        new JdbcConnection(connection, codecs).beginTransaction().as(StepVerifier::create).verifyComplete();
    }

    @Test
    void testBeginTransactionErrorResponse() throws SQLException {
        when(connection.getAutoCommit()).thenReturn(true);
        doThrow(new SQLNonTransientConnectionException("Unable to disable autocommits", "some state", 999)).when(connection).setAutoCommit(ArgumentMatchers.anyBoolean());

        new JdbcConnection(connection, codecs).beginTransaction().as(StepVerifier::create).verifyErrorMatches(R2dbcNonTransientResourceException.class::isInstance);
    }

    @Test
    void testBeginTransactionInTransaction() throws SQLException {
        when(connection.getAutoCommit()).thenReturn(true);
        verifyNoMoreInteractions(connection);

        new JdbcConnection(connection, codecs).beginTransaction().as(StepVerifier::create).verifyComplete();
    }

    @Test
    void testClose() throws SQLException {
        when(connection.isClosed()).thenReturn(false);

        new JdbcConnection(connection, codecs).close().as(StepVerifier::create).verifyComplete();
    }

    @Test
    void testCloseWhenClosed() throws SQLException {
        when(connection.isClosed()).thenReturn(true);
        verifyNoMoreInteractions(connection);

        new JdbcConnection(connection, codecs).close().as(StepVerifier::create).verifyComplete();
    }

    @Test
    void testCommitTransaction() throws SQLException {
        when(connection.getAutoCommit()).thenReturn(false);

        new JdbcConnection(connection, codecs).commitTransaction().as(StepVerifier::create).verifyComplete();
    }

    @Test
    void testCommitTransactionErrorResponse() throws SQLException {
        when(connection.getAutoCommit()).thenReturn(false);
        doThrow(new SQLTransactionRollbackException("can't commit", "some state", 999)).when(connection).commit();

        new JdbcConnection(connection, codecs).commitTransaction().as(StepVerifier::create).verifyErrorMatches(R2dbcRollbackException.class::isInstance);
    }

    @Test
    void testCommitTransactionNonOpen() throws SQLException {
        when(connection.getAutoCommit()).thenReturn(true);
        verifyNoMoreInteractions(connection);

        new JdbcConnection(connection, codecs).commitTransaction().as(StepVerifier::create).verifyComplete();
    }

    @Test
    void testConstructorNoClient() {
        assertThatNullPointerException().isThrownBy(() -> new JdbcConnection(null, null)).withMessage("connection must not be null");
    }

    @Test
    void testCreateBatch() {
        assertThat(new JdbcConnection(connection, codecs).createBatch()).isInstanceOf(JdbcBatch.class);
        // assertThatThrownBy(jdbcConnection::createBatch).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testCreateSavepoint() {
        new JdbcConnection(connection, codecs).createSavepoint("test").as(StepVerifier::create).verifyComplete();
    }

    @Test
    void testCreateSavepointNoName() {
        // new JdbcConnection(this.connection).createSavepoint(null).as(StepVerifier::create).verifyErrorMatches(IllegalArgumentException.class::isInstance);

        assertThatIllegalArgumentException().isThrownBy(() -> new JdbcConnection(connection, codecs).createSavepoint(null));
    }

    @Test
    void testCreateStatement() {
        assertThat(new JdbcConnection(connection, codecs).createStatement("select-query-?")).isInstanceOf(JdbcStatement.class);
        assertThat(new JdbcConnection(connection, codecs).createStatement("insert-query-?")).isInstanceOf(JdbcStatement.class);
        assertThat(new JdbcConnection(connection, codecs).createStatement("update-query-?")).isInstanceOf(JdbcStatement.class);
        assertThat(new JdbcConnection(connection, codecs).createStatement("delete-query-?")).isInstanceOf(JdbcStatement.class);

        // assertThatThrownBy(() -> new JdbcConnection(this.connection,codecs).createStatement("some-query-?")).isInstanceOf(R2dbcBadGrammarException.class);
    }

    @Test
    void testReleaseSavepoint() {
        // new JdbcConnection(connection).releaseSavepoint("test").as(StepVerifier::create)
        // .verifyErrorMatches(JdbcR2dbcNonTransientException.class::isInstance);
        new JdbcConnection(connection, codecs).releaseSavepoint("test").as(StepVerifier::create).verifyComplete();
    }

    @Test
    void testReleaseSavepointNoName() {
        // new JdbcConnection(connection,codecs).releaseSavepoint(null).as(StepVerifier::create)
        // .verifyErrorMatches(JdbcR2dbcNonTransientException.class::isInstance);
        // new
        // JdbcConnection(connection,codecs).releaseSavepoint(null).as(StepVerifier::create).verifyErrorMatches(IllegalArgumentException.class::isInstance);

        assertThatIllegalArgumentException().isThrownBy(() -> new JdbcConnection(connection, codecs).releaseSavepoint(null));
    }

    @Test
    void testRollbackTransaction() throws SQLException {
        when(connection.getAutoCommit()).thenReturn(false);

        new JdbcConnection(connection, codecs).rollbackTransaction().as(StepVerifier::create).verifyComplete();
    }

    @Test
    void testRollbackTransactionErrorResponse() throws SQLException {
        when(connection.getAutoCommit()).thenReturn(false);
        doThrow(new SQLTransactionRollbackException("can't commit", "some state", 999)).when(connection).rollback();

        new JdbcConnection(connection, codecs).rollbackTransaction().as(StepVerifier::create).verifyErrorMatches(R2dbcRollbackException.class::isInstance);
    }

    @Test
    void testRollbackTransactionNonOpen() throws SQLException {
        when(connection.getAutoCommit()).thenReturn(true);
        verifyNoMoreInteractions(connection);

        new JdbcConnection(connection, codecs).rollbackTransaction().as(StepVerifier::create).verifyComplete();
    }

    @Test
    void testRollbackTransactionToSavepoint() {
        // new JdbcConnection(connection).rollbackTransactionToSavepoint("test").as(StepVerifier::create)
        // .verifyErrorMatches(JdbcR2dbcNonTransientException.class::isInstance);
        new JdbcConnection(connection, codecs).rollbackTransactionToSavepoint("test").as(StepVerifier::create).verifyComplete();
    }

    @Test
    void testRollbackTransactionToSavepointNoName() {
        // new JdbcConnection(connection).rollbackTransactionToSavepoint(null).as(StepVerifier::create)
        // .verifyErrorMatches(JdbcR2dbcNonTransientException.class::isInstance);
        new JdbcConnection(connection, codecs).rollbackTransactionToSavepoint(null).as(StepVerifier::create).verifyErrorMatches(NullPointerException.class::isInstance);
    }

    @Test
    void testSetTransactionIsolationLevelNoIsolationLevel() {
        // .expectError(NullPointerException.class)
        // .expectNextCount(1).verifyComplete();
        new JdbcConnection(connection, codecs).setTransactionIsolationLevel(null).as(StepVerifier::create).verifyErrorMatches(NullPointerException.class::isInstance);
    }

    @Test
    void testSetTransactionIsolationLevelReadUncommitted() {
        // when(connection.getAutoCommit()).thenReturn(false);
        // when(client.execute("SET LOCK_MODE 0")).thenReturn(Mono.empty());

        new JdbcConnection(connection, codecs).setTransactionIsolationLevel(READ_UNCOMMITTED).as(StepVerifier::create).verifyComplete();
    }

    @Test
    void testSetTransactionIsolationLevelRepeatableRead() {
        // when(connection.getAutoCommit()).thenReturn(false);
        // when(client.execute("SET LOCK_MODE 1")).thenReturn(Mono.empty());

        new JdbcConnection(connection, codecs).setTransactionIsolationLevel(REPEATABLE_READ).as(StepVerifier::create).verifyComplete();
    }

    @Test
    void testSetTransactionIsolationLevelSerializable() {
        // when(connection.getAutoCommit()).thenReturn(false);
        // when(client.execute("SET LOCK_MODE 1")).thenReturn(Mono.empty());

        new JdbcConnection(connection, codecs).setTransactionIsolationLevel(SERIALIZABLE).as(StepVerifier::create).verifyComplete();
    }
}
