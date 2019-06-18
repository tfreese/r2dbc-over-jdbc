# r2dbc-over-jdbc - Reactive Relational Database Connectivity over JDBC

An experimental and research Project ... but it works !


## Type-Mapping
Adapt the `io.r2dbc.jdbc.codec.decoder.Decoder` and `io.r2dbc.jdbc.codec.encoder.Encoder`
to your underlying Database.

For custom SQL-Types and Java-Classes use
* `io.r2dbc.jdbc.codec.Codecs#registerDecoder` to map ResultSet to JavaObject 
* `io.r2dbc.jdbc.codec.Codecs#registerEncoder` to map JavaObject to ResultSet  


## Limitations
* No support for `io.r2dbc.spi.Batch` like: `insert ...; select ... `
* No support for Compound Statements  like: `select ...; select ...`
