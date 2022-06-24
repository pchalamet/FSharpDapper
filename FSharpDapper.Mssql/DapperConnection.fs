module FSharpDapper.Mssql
open System.Transactions
open System
open Dapper

type DapperConnection(connectionString) =
    let tx = lazy (new TransactionScope())
    let connection = lazy (new Microsoft.Data.SqlClient.SqlConnection(connectionString))
with
    interface IDapperConnection with
        member _.ExecuteScalar<'T>(sql, prms) =
            connection.Value.ExecuteScalar<'T>(sql, prms)

        member _.Query<'T>(sql, prms) =
            connection.Value.Query<'T>(sql, prms) |> List.ofSeq

        member _.Execute(sql, prms) =
            connection.Value.Execute(sql, prms)

        member _.TransactionScope() =
            new DapperTransactionScope(tx.Force()) :> IDapperTransactionScope

    interface IDisposable with
        member _.Dispose() =
            if connection.IsValueCreated then connection.Value.Dispose()
            if tx.IsValueCreated then tx.Value.Dispose()

    static member Create(connectionString) =
        new DapperConnection(connectionString) :> IDapperConnection
