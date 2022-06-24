module FSharpDapper.Tests
open NUnit.Framework
open FsUnit
open System
open Moq
open TestHelpers

[<Table("Status"); CLIMutable>]
type DbStatus =
    { [<Key>] Name: string
      Status: int }

[<Table("StatusEx"); CLIMutable>]
type DbStatusEx =
    { [<Key>] Name: string
      Status: int
      Comment: string option }

[<Table("Monitoring"); CLIMutable>]
type DbMonitoring =
    { [<Key>] Name: string
      [<Key>] Count: int
      Status: int }


[<Test>]
let CheckCount() =
    FSharpDapper.getCount null |> should equal 0
    FSharpDapper.getCount "toto" |> should equal 1

    FSharpDapper.getCount [| |] |> should equal 0
    FSharpDapper.getCount [] |> should equal 0
    FSharpDapper.getCount Seq.empty |> should equal 0

    FSharpDapper.getCount [|"toto"; "tutu" |] |> should equal 2
    FSharpDapper.getCount ["toto"; "tutu" ] |> should equal 2
    FSharpDapper.getCount (["toto"; "tutu" ] |> Seq.ofList) |> should equal 2

[<Test>]
let GenSQLExecuteScalar () =
    let prm = {| A = 42 |}
    let conn = Mock<FSharpDapper.IDapperConnection>(MockBehavior.Strict)
    conn.Setup(fun dc -> dc.ExecuteScalar<int>("select * from Status", prm))
        .Returns(1) |> ignore

    let res = QueryScalar<int> conn.Object "select * from Status" prm
    res |> should equal 1

    conn.Verify()

[<Test>]
let GenSQLQuery () =
    let prm = {| A = 42 |}
    let conn = Mock<FSharpDapper.IDapperConnection>(MockBehavior.Strict)
    conn.Setup(fun dc -> dc.Query<int>("select * from Status", prm))
        .Returns([1]) |> ignore

    let res = Query<int> conn.Object "select * from Status" prm
    res |> List.ofSeq |> should equal [1]

    conn.Verify()

[<Test>]
let GenSQLSelect () =
    let prm = {| Name = "tagada" |}
    let conn = Mock<FSharpDapper.IDapperConnection>(MockBehavior.Strict)
    conn.Setup(fun dc -> dc.Query<DbStatus>("SELECT [Name],[Status] FROM [Status] WHERE [Name]=@Name", prm))
        .Returns( [ { Name = "tagada"; Status = 42 } ] ) |> ignore

    let res = Select<DbStatus> conn.Object prm
    res |> List.ofSeq |> should equal [{ DbStatus.Name = "tagada"; DbStatus.Status = 42 }]

    conn.Verify()

[<Test>]
let GenSQLSelectAll () =
    let conn = Mock<FSharpDapper.IDapperConnection>(MockBehavior.Strict)
    conn.Setup(fun dc -> dc.Query<DbStatus>("SELECT [Name],[Status] FROM [Status] WHERE 1=1", It.Is(areSame {| |})))
        .Returns( [ { DbStatus.Name = "tagada"; DbStatus.Status = 42 } ] ) |> ignore

    let res = SelectAll<DbStatus> conn.Object
    res |> List.ofSeq |> should equal [{ DbStatus.Name = "tagada"; DbStatus.Status = 42 }]

    conn.Verify()

[<Test>]
let GenSQLInsert () =
    let prm = {| Name = "tagada"; Status = 42 |}
    let conn = Mock<FSharpDapper.IDapperConnection>(MockBehavior.Strict)
    conn.Setup(fun dc -> dc.Execute("INSERT INTO [Status] ([Name],[Status]) VALUES (@Name,@Status)", prm)).Returns(1) |> ignore

    Insert<DbStatus> conn.Object prm

    conn.Verify()

[<Test>]
let GenSQLUpdate () =
    let prm = {| Name = "tagada"; Status = 42 |}
    let conn = Mock<FSharpDapper.IDapperConnection>(MockBehavior.Strict)
    conn.Setup(fun dc -> dc.Execute("UPDATE [Status] SET [Status]=@Status WHERE [Name]=@Name", prm)).Returns(1) |> ignore
    Update<DbStatus> conn.Object prm
    conn.Verify()

[<Test>]
let GenSQLUpdateError () =
    let prm = {| Name = "tagada"; Status = 42 |}
    let conn = Mock<FSharpDapper.IDapperConnection>(MockBehavior.Strict)
    conn.Setup(fun dc -> dc.Execute("UPDATE [Status] SET [Status]=@Status WHERE [Name]=@Name", prm)).Returns(0) |> ignore
    (fun () -> Update<DbStatus> conn.Object prm) |> should throw typeof<Exception> 
    conn.Verify()

[<Test>]
let GenSQLDelete () =
    let prm = {| Name = "tagada"; Status = 42 |}
    let conn = Mock<FSharpDapper.IDapperConnection>(MockBehavior.Strict)
    conn.Setup(fun dc -> dc.Execute("DELETE [Status] WHERE [Name]=@Name AND [Status]=@Status", prm)).Returns(1) |> ignore

    let res = Delete<DbStatus> conn.Object prm
    res |> should equal 1

    conn.Verify()

[<Test>]
let GenSQLUpsertOK () =
    let prm = {| Name = "tagada"; Status = 42 |}

    let conn = Mock<FSharpDapper.IDapperConnection>(MockBehavior.Strict)

    let seq = MockSequence()
    conn.InSequence(seq).Setup(fun dc -> dc.Execute("MERGE INTO [Status] as TARGET USING (VALUES(@Name,@Status)) AS SOURCE ([Name],[Status]) ON SOURCE.[Name]=TARGET.[Name] WHEN MATCHED THEN UPDATE SET [Status]=SOURCE.[Status] WHEN NOT MATCHED THEN INSERT ([Name],[Status]) VALUES (SOURCE.[Name],SOURCE.[Status]);", prm)).Returns(1) |> ignore

    Upsert<DbStatus> conn.Object prm

    conn.Verify()

[<Test>]
let GenSQLUpsertOKWithPartialTable () =
    let prm = {| Name = "tagada"; Status = 42 |}

    let conn = Mock<FSharpDapper.IDapperConnection>(MockBehavior.Strict)

    let seq = MockSequence()
    conn.InSequence(seq).Setup(fun dc -> dc.Execute("MERGE INTO [StatusEx] as TARGET USING (VALUES(@Name,@Status)) AS SOURCE ([Name],[Status]) ON SOURCE.[Name]=TARGET.[Name] WHEN MATCHED THEN UPDATE SET [Status]=SOURCE.[Status] WHEN NOT MATCHED THEN INSERT ([Name],[Status]) VALUES (SOURCE.[Name],SOURCE.[Status]);", prm)).Returns(1) |> ignore

    Upsert<DbStatusEx> conn.Object prm

    conn.Verify()



[<Test>]
let GenSQLUpsertKO () =
    let prm = {| Name = "tagada"; Status = 42 |}

    let conn = Mock<FSharpDapper.IDapperConnection>(MockBehavior.Strict)

    let seq = MockSequence()
    conn.InSequence(seq).Setup(fun dc -> dc.Execute("MERGE INTO [Status] as TARGET USING (VALUES(@Name,@Status)) AS SOURCE ([Name],[Status]) ON SOURCE.[Name]=TARGET.[Name] WHEN MATCHED THEN UPDATE SET [Status]=SOURCE.[Status] WHEN NOT MATCHED THEN INSERT ([Name],[Status]) VALUES (SOURCE.[Name],SOURCE.[Status]);", prm)).Returns(0) |> ignore

    (fun () -> Upsert<DbStatus> conn.Object prm) |> should throw (typeof<Exception>)

    conn.Verify()


[<Test>]
let GenSQLUpsertMultiKey () =
    let prm = {| Name = "tagada"; Count = 2; Status = 42 |}
    let conn = Mock<FSharpDapper.IDapperConnection>(MockBehavior.Strict)

    let seq = MockSequence()
    conn.InSequence(seq).Setup(fun dc -> dc.Execute("MERGE INTO [Monitoring] as TARGET USING (VALUES(@Count,@Name,@Status)) AS SOURCE ([Count],[Name],[Status]) ON SOURCE.[Name]=TARGET.[Name] AND SOURCE.[Count]=TARGET.[Count] WHEN MATCHED THEN UPDATE SET [Status]=SOURCE.[Status] WHEN NOT MATCHED THEN INSERT ([Count],[Name],[Status]) VALUES (SOURCE.[Count],SOURCE.[Name],SOURCE.[Status]);", prm)).Returns(1) |> ignore

    Upsert<DbMonitoring> conn.Object prm

    conn.Verify()


[<Test>]
let GenSQLSelectMultiKey () =
    let prm = {| Name = "tagada"; Count = 2; Status = 42 |}
    let dbMonitorings = [ { Name = "tagada"; Count = 2; Status = 42 } ]
    let conn = Mock<FSharpDapper.IDapperConnection>(MockBehavior.Strict)
    conn.Setup(fun dc -> dc.Query("SELECT [Name],[Count],[Status] FROM [Monitoring] WHERE [Count]=@Count AND [Name]=@Name AND [Status]=@Status", prm)).Returns(dbMonitorings) |> ignore

    let res = Select<DbMonitoring> conn.Object prm
    res |> should equal dbMonitorings

    conn.Verify()

[<Test>]
let GenSQLUseTransaction () =
    let prm = {| Name = "tagada"; Status = 42 |}

    let seq = MockSequence()
    let tx = Mock<FSharpDapper.IDapperTransactionScope>(MockBehavior.Strict)
    let conn = Mock<FSharpDapper.IDapperConnection>(MockBehavior.Strict)
    conn.InSequence(seq).Setup(fun dc -> dc.TransactionScope()).Returns(tx.Object) |> ignore
    conn.InSequence(seq).Setup(fun dc -> dc.Execute("MERGE INTO [Status] as TARGET USING (VALUES(@Name,@Status)) AS SOURCE ([Name],[Status]) ON SOURCE.[Name]=TARGET.[Name] WHEN MATCHED THEN UPDATE SET [Status]=SOURCE.[Status] WHEN NOT MATCHED THEN INSERT ([Name],[Status]) VALUES (SOURCE.[Name],SOURCE.[Status]);", prm)).Returns(1) |> ignore
    tx.InSequence(seq).Setup(fun t -> t.Complete()) |> ignore
    tx.InSequence(seq).Setup(fun t -> t.Dispose()) |> ignore

    let runInTx() =
        use tx = conn.Object |> UseTransactionScope
        Upsert<DbStatus> conn.Object prm
        tx.Complete()

    runInTx()

    conn.Verify()
    tx.Verify()
