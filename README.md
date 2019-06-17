# r2dbc-over-jdbc - Reactive Relational Database Connectivity over JDBC

An experimental and research Project ... but it works !


## Type-Mapping
For custom SQL-Types use
`io.r2dbc.jdbc.codec.Codecs#registerCodec`.


## Limitations
* No support for `io.r2dbc.spi.Batch` like: `insert ...; select ... `
* No support for Compound Statements  like: `select ...; select ...`
