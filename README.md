# r2dbc-over-jdbc - Reactive Relational Database Connectivity over JDBC


**This is just an experimental and research Project and not designed for production use ... but it works !**


## Type-Mapping
Adapt the SqlMapper and ObjectTransformer to match your underlying Database !

* `io.r2dbc.jdbc.converter.sql.SqlMapper<T`
	Mapping for a SQL-Type from the java.sql.ResultSet to an Java-Object and vice versa.

* `io.r2dbc.jdbc.converter.transformer.ObjectTransformer<T>`
	Transform an Object to the required Type for Method io.r2dbc.spi.Row.get(Object, Class<T>).
		
	
For custom SqlConverter and ObjectTransformer use
* `io.r2dbc.jdbc.converter.Converters#register`


## Limitations
* No support for `io.r2dbc.spi.Batch` like: `insert ...; select ... `
* No support for named parameters
