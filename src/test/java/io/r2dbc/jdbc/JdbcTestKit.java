// Created: 25.03.2021
package io.r2dbc.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcOperations;

import io.r2dbc.jdbc.util.DBServerExtension;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.test.TestKit;

/**
 * @author Thomas Freese
 */
final class JdbcTestKit implements TestKit<Integer>
{
    /**
     *
     */
    private static ConnectionFactory connectionFactory;

    /**
    *
    */
    @RegisterExtension
    static final DBServerExtension SERVER = new DBServerExtension();

    // /**
    // * @see io.r2dbc.spi.test.TestKit#blobInsert()
    // */
    // @Override
    // @Test
    // public void blobInsert()
    // {
    // // Example.super.blobInsert();
    //
//            // @formatter:off
//            Mono.from(getConnectionFactory().create())
//                .flatMapMany(connection -> Flux.from(connection
//
//                    .createStatement(String.format("INSERT INTO blob_test VALUES (%s)", getPlaceholder(0)))
//                    .bind(getIdentifier(0), Blob.from(Mono.just(StandardCharsets.UTF_8.encode("test-value"))))
//                    .execute())
//
//                    .concatWith(TestKit.close(connection)))
//                .as(StepVerifier::create)
//                .expectNextCount(1).as("rows inserted")
//                .verifyComplete()
//                ;
//            // @formatter:on
    // }

    // /**
    // * @see io.r2dbc.spi.test.TestKit#clobInsert()
    // */
    // @Override
    // @Test
    // public void clobInsert()
    // {
    // // Example.super.clobInsert();
    //
//            // @formatter:off
//            Mono.from(getConnectionFactory().create())
//                .flatMapMany(connection -> Flux.from(connection
//
//                    .createStatement(String.format("INSERT INTO clob_test VALUES (%s)", getPlaceholder(0)))
//                    .bind(getIdentifier(0), Clob.from(Mono.just("test-value")))
//                    .execute())
//
//                    .concatWith(TestKit.close(connection)))
//                .as(StepVerifier::create)
//                .expectNextCount(1).as("rows inserted")
//                .verifyComplete()
//                ;
//         // @formatter:on
    // }

    /**
     * @see io.r2dbc.spi.test.TestKit#autoCommitByDefault()
     */
    @Override
    @Test
    public void autoCommitByDefault()
    {
        TestKit.super.autoCommitByDefault();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#batch()
     */
    @Override
    @Test
    public void batch()
    {
        TestKit.super.batch();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#bindFails()
     */
    @Override
    @Test
    public void bindFails()
    {
        TestKit.super.bindFails();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#bindNull()
     */
    @Override
    @Test
    public void bindNull()
    {
        TestKit.super.bindNull();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#bindNullFails()
     */
    @Override
    @Test
    public void bindNullFails()
    {
        TestKit.super.bindNullFails();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#blobInsert()
     */
    @Override
    @Test
    public void blobInsert()
    {
        TestKit.super.blobInsert();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#blobSelect()
     */
    @Override
    @Test
    public void blobSelect()
    {
        TestKit.super.blobSelect();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#changeAutoCommitCommitsTransaction()
     */
    @Override
    @Test
    public void changeAutoCommitCommitsTransaction()
    {
        TestKit.super.changeAutoCommitCommitsTransaction();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#clobInsert()
     */
    @Override
    @Test
    public void clobInsert()
    {
        TestKit.super.clobInsert();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#clobSelect()
     */
    @Override
    @Test
    public void clobSelect()
    {
        TestKit.super.clobSelect();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#columnMetadata()
     */
    @Override
    @Test
    public void columnMetadata()
    {
        TestKit.super.columnMetadata();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#compoundStatement()
     */
    @Override
    @Test
    public void compoundStatement()
    {
        TestKit.super.compoundStatement();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#createStatementFails()
     */
    @Override
    @Test
    public void createStatementFails()
    {
        TestKit.super.createStatementFails();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#duplicateColumnNames()
     */
    @Override
    @Test
    public void duplicateColumnNames()
    {
        TestKit.super.duplicateColumnNames();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#getConnectionFactory()
     */
    @Override
    public ConnectionFactory getConnectionFactory()
    {
        if (connectionFactory == null)
        {
            connectionFactory = new JdbcConnectionFactory(SERVER.getDataSource());
        }

        return connectionFactory;
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#getCreateTableWithAutogeneratedKey()
     */
    @Override
    public String getCreateTableWithAutogeneratedKey()
    {
        return "CREATE TABLE tbl_auto ( id INTEGER GENERATED BY DEFAULT AS IDENTITY(START WITH 1, INCREMENT BY 1), value INTEGER);";
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#getIdentifier(int)
     */
    @Override
    public Integer getIdentifier(final int index)
    {
        return index;
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#getJdbcOperations()
     */
    @Override
    public JdbcOperations getJdbcOperations()
    {
        return SERVER.getJdbcOperations();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#getPlaceholder(int)
     */
    @Override
    public String getPlaceholder(final int index)
    {
        return "?";
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#prepareStatement()
     */
    @Override
    @Test
    public void prepareStatement()
    {
        // TODO Auto-generated method stub
        TestKit.super.prepareStatement();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#prepareStatementWithIncompleteBatchFails()
     */
    @Override
    @Test
    public void prepareStatementWithIncompleteBatchFails()
    {
        TestKit.super.prepareStatementWithIncompleteBatchFails();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#prepareStatementWithIncompleteBindingFails()
     */
    @Override
    @Test
    public void prepareStatementWithIncompleteBindingFails()
    {
        TestKit.super.prepareStatementWithIncompleteBindingFails();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#returnGeneratedValues()
     */
    @Override
    @Test
    public void returnGeneratedValues()
    {
        TestKit.super.returnGeneratedValues();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#returnGeneratedValuesFails()
     */
    @Override
    @Test
    public void returnGeneratedValuesFails()
    {
        // TODO Auto-generated method stub
        TestKit.super.returnGeneratedValuesFails();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#sameAutoCommitLeavesTransactionUnchanged()
     */
    @Override
    @Test
    public void sameAutoCommitLeavesTransactionUnchanged()
    {
        TestKit.super.sameAutoCommitLeavesTransactionUnchanged();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#savePoint()
     */
    @Override
    @Test
    public void savePoint()
    {
        TestKit.super.savePoint();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#savePointStartsTransaction()
     */
    @Override
    @Test
    public void savePointStartsTransaction()
    {
        TestKit.super.savePointStartsTransaction();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#transactionCommit()
     */
    @Override
    @Test
    public void transactionCommit()
    {
        TestKit.super.transactionCommit();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#transactionRollback()
     */
    @Override
    @Test
    public void transactionRollback()
    {
        TestKit.super.transactionRollback();
    }

    /**
     * @see io.r2dbc.spi.test.TestKit#validate()
     */
    @Override
    @Test
    public void validate()
    {
        TestKit.super.validate();
    }
}