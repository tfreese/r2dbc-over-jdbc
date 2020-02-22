# r2dbc-over-jdbc - Reactive Relational Database Connectivity over JDBC


**This is just an experimental and research Project and not designed for production use ... but it works !**


## Type-Mapping
Adapt the SqlDecoder, SqlEncoder and Converter to match your underlying Database !

* `io.r2dbc.jdbc.codec.decoder.SqlDecoder`
	Decodes a Value from the java.sql.ResultSet to a Java-Object, based on the Column SQL-Type.

* `io.r2dbc.jdbc.codec.encoder.SqlEncoder`
	Encodes a Java-Object to a SQL-Value for java.sql.PreparedStatement, based on the Column SQL-Type.
	
* `io.r2dbc.jdbc.codec.converter.Converter`
	Convert an Object to the required Type for Method io.r2dbc.spi.Row.get(Object, Class<T>).
		
	
For custom SqlDecoder, Converter and SqlEncoder use
* `io.r2dbc.jdbc.codec.Codecs#registerDecoder`


## Limitations
* No support for `io.r2dbc.spi.Batch` like: `insert ...; select ... `
* No support for named parameters
