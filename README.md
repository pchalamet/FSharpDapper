# FSharpDapper

This is a simple library to help dealing with Dapper in F#.

It maps F# collection, records and anonymous types to Dapper. Discriminated unions are not supported at this time.

Implementation for Mssql (database connection) and Prometheus (performance monitoring) are provided.

# Plain Old Record Object
In order to read/write data, you will need to define a record first:

```F#
[<Table("StatusEx"); CLIMutable>]
type DbStatusEx =
    { [<Key>] Name: string
      Status: int
      Comment: string option }
```

# Create connection

Create a connection using `DapperConnection.Create` - you can use FSharpDapper.Mssql.

# Usage

```F#
use conn = "...." |> DapperConnection.Create
let status = {| Name = "toto" |} |> Select<DbStatusEx> conn
````

# SQL Api

Following operations are available:

Operation | Description
----------|------------
`Execute` | Run provided sql query using the parameter and returns the result from the query (an int)
`QueryScalar<T>` | Run provided sql query using the parameter and returns the single result of `T`
`Query<T>` | Run provided sql query using the parameter and returns a list of result of `T`
`Select<'Table>` | Run select query using the conditions and returns a list of result of `'Table`
`SelectAll<'Table>` | Select all results from table `'Table`
`Insert<'Table>` | Insert values into table `'Table`
`Update<'Table>` | Update table `Table` with values
`Delete<'Table>` | Delete table `'Table` using the conditions
`Upsert<'Table>` | Upsert values into table `'Table'`

`Insert`, `Update`, `Delete` and `Upsert` support either a single value or a list. A value is either a record or an anonymous record.

# Transaction Api

`UseTransaction` is starting a new transation on the given connection.
Note the transaction must be disposed.

