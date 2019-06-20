# r2dbc-over-jdbc - Reactive Relational Database Connectivity over JDBC

An experimental and research Project ... but it works !


## Type-Mapping
**Adapt the Decoder, Converter and Encoder to match your underlying Database !**

* `io.r2dbc.jdbc.codec.decoder.Decoder`
	Decodes a Value from the java.sql.ResultSet to a Java-Object, based on the Column SQL-Type.

* `io.r2dbc.jdbc.codec.converter.Converter`
	Convert an Object to the required Type for Method io.r2dbc.spi.Row.get(Object, Class<T>).

* `io.r2dbc.jdbc.codec.encoder.Encoder`
	Encodes a Java-Object to a SQL-Value for java.sql.PreparedStatement, based on the Column SQL-Type.
	
For custom Decoder, Converter and Encoder use
* `io.r2dbc.jdbc.codec.Codecs#registerDecoder`
* `io.r2dbc.jdbc.codec.Codecs#registerConverter`
* `io.r2dbc.jdbc.codec.Codecs#registerEncoder` 


## Limitations
* No support for `io.r2dbc.spi.Batch` like: `insert ...; select ... `
* No support for Compound Statements  like: `select ...; select ...`
