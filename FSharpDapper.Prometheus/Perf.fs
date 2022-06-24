module FSharpDapper.Perf
open System
open Prometheus
open FSharpDapper

let sqlDurations = Metrics.CreateHistogram("sql_processing_duration_seconds",
                                           "Histogram of sql processing durations.",
                                           HistogramConfiguration(LabelNames = [| "method" |]))


type DapperPrometheus(connection: IDapperConnection) =
    interface IDapperConnection with
        member _.ExecuteScalar<'T>(sql, prms) =
            use _ = sqlDurations.WithLabels([| "execute-scalar" |]).NewTimer()
            connection.ExecuteScalar<'T>(sql, prms)

        member _.Query<'T>(sql, prms) =
            use _ = sqlDurations.WithLabels([| "query" |]).NewTimer()
            connection.Query<'T>(sql, prms) |> List.ofSeq

        member _.Execute(sql, prms) =
            use _ = sqlDurations.WithLabels([| "execute" |]).NewTimer()
            connection.Execute(sql, prms)

        member _.TransactionScope() =
            connection.TransactionScope()

    interface IDisposable with
        member _.Dispose() =
            connection.Dispose()

    static member Create(connection) =
        new DapperPrometheus(connection) :> IDapperConnection
