module FSharpDapper
open System
open Dapper
open System.Collections.Generic
open System.Transactions

type OptionHandler<'T>() =
    inherit SqlMapper.TypeHandler<option<'T>>()

    override __.Parse value =
        if isNull value || value = box System.DBNull.Value then None
        else Some (value :?> 'T)

    override __.SetValue(param, value) = 
        param.Value <- value |> Option.map box |> Option.defaultValue null    


let Init() =
    SqlMapper.AddTypeHandler (OptionHandler<string>())
    SqlMapper.AddTypeHandler (OptionHandler<int>())
    SqlMapper.AddTypeHandler (OptionHandler<float>())
    SqlMapper.AddTypeHandler (OptionHandler<Guid>())
    SqlMapper.AddTypeHandler (OptionHandler<DateTime>())
    SqlMapper.AddTypeHandler (OptionHandler<bool>())


[<AttributeUsage(AttributeTargets.Class)>]
type TableAttribute(name: string) =
    inherit Attribute()

    member public _.Name = name

[<AttributeUsage(AttributeTargets.Property)>]
type KeyAttribute() =
    inherit Attribute()

let getUnderlyingtype (t: Type) =
    match t.GetInterfaces() |> Seq.tryFind (fun it -> it.IsGenericType && it.GetGenericTypeDefinition() = typedefof<IEnumerable<_>>) with
    | Some itf -> itf.GenericTypeArguments.[0]
    | _ -> t

let getCount (o: obj) =
    match o with
    | null -> 0
    | :? IEnumerable<obj> as e -> System.Linq.Enumerable.Count(e)
    | _ -> 1

let getMembers (t: Type) =
    let underlyingType = t |> getUnderlyingtype
    underlyingType.GetProperties() |> List.ofArray |> List.map (fun x -> x.Name)

let getValues (t: Type) =
    let underlyingType = t |> getUnderlyingtype
    underlyingType.GetProperties() |> List.ofArray |> List.map (fun x -> sprintf "@%s" x.Name)

let getKeys (t: Type) =
    let isKey (prop: System.Reflection.PropertyInfo) =
        let isId = prop.Name = "Id"
        let isAttrKey = prop.GetCustomAttributes(typeof<KeyAttribute>, false).Length > 0
        let res = isId || isAttrKey
        res

    let propIds = t.GetProperties() |> List.ofArray |> List.filter isKey
    propIds |> List.map (fun p -> p.Name)

let getTable (t : Type) =     
    match t.GetCustomAttributes(typeof<TableAttribute>, false) |> Seq.tryHead with
    | Some attr -> (attr :?> TableAttribute).Name
    | _ -> t.Name

let join (sep: string) (items: string list) =
    String.Join(sep, items)



type IDapperTransactionScope =
    inherit IDisposable
    abstract member Complete: unit -> unit

type IDapperConnection =
    inherit IDisposable
    abstract member ExecuteScalar: sql:string * prms: obj -> 'T
    abstract member Query: sql:string * prms: obj -> 'T list
    abstract member Execute: sql:string * prms: obj -> int
    abstract member TransactionScope: unit -> IDapperTransactionScope

type DapperTransactionScope(ts: TransactionScope) =
    interface IDapperTransactionScope with
        member _.Complete() = ts.Complete()

    interface IDisposable with
        member _.Dispose() = ts.Dispose()


let private escapeName = sprintf "[%s]"

let private parameterName = sprintf "@%s"

let UseTransactionScope (connection: IDapperConnection) =
    connection.TransactionScope()

let Execute (connection: IDapperConnection) (sql: string) (prms: obj) =
    connection.Execute(sql, prms)

let QueryScalar<'T> (connection: IDapperConnection) (sql: string) (prms: obj) =
    connection.ExecuteScalar<'T>(sql, prms)

let Query<'T> (connection: IDapperConnection) (sql: string) (prms: obj) =
    connection.Query<'T>(sql, prms) |> List.ofSeq

let Select<'Table> (connection: IDapperConnection) (conditions: obj) =
    // SELECT Id, Toto FROM Table [WHERE Tutu=@Tutu]
    let projection = typeof<'Table> |> getMembers |> List.map escapeName |> join ","
    let from = typeof<'Table> |> getTable |> escapeName

    let where =  match conditions.GetType() |> getMembers |> List.map (fun x -> sprintf "%s=@%s" (escapeName x) x) with
                 | [] -> "1=1"
                 | conditions -> conditions |> join " AND "
    
    let sql = sprintf "SELECT %s FROM %s WHERE %s" projection from where
    connection.Query<'Table>(sql, conditions) |> List.ofSeq

let SelectAll<'Table> (connection: IDapperConnection) = Select<'Table> connection {| |}

let Insert<'Table> (connection: IDapperConnection) (entity: obj) =
    // INSERT INTO Table (Id, Toto) VALUES (@Id, @Toto)
    let into = typeof<'Table> |> getTable |> escapeName
    let projection = entity.GetType() |> getMembers |> List.map escapeName |> join ","
    let values = entity.GetType() |> getValues |> join ","

    let sql = sprintf "INSERT INTO %s (%s) VALUES (%s)" into projection values
    let count = getCount entity
    if count <> connection.Execute(sql, entity) then failwith "Failed to Insert"

let Update<'Table> (connection: IDapperConnection) (entity: obj) =
    // UPDATE Table set @Toto=@Toto, @Tutu=@Tutu
    let from = typeof<'Table> |> getTable |> escapeName
    let ids = typeof<'Table> |> getKeys
    let assign = entity.GetType() |> getMembers
                                  |> List.except ids
                                  |> List.map (fun x -> sprintf "%s=@%s" (escapeName x) x) |> join ","
    let where = ids |> List.map (fun x -> sprintf "%s=@%s" (escapeName x) x) |> join " AND "

    let sql = sprintf "UPDATE %s SET %s WHERE %s" from assign where
    let count = getCount entity
    if count <> connection.Execute(sql, entity) then failwith "Failed to Update"

let Delete<'Table> (connection: IDapperConnection) (conditions: obj) =
    // DELETE Table WHERE @Tutu=@Tutu
    let from = typeof<'Table> |> getTable |> escapeName
    let where =  match conditions.GetType() |> getMembers |> List.map (fun x -> sprintf "%s=@%s" (escapeName x) x) with
                 | [] -> "1=1"
                 | conditions -> conditions |> join " AND "

    let sql = sprintf "DELETE %s WHERE %s" from where
    connection.Execute(sql, conditions)

let Upsert<'Table> (connection: IDapperConnection) (entity: obj) =
    // MERGE INTO <Table> as TARGET
    // USING ( VALUES(@Name, @Status) ) AS SOURCE ([Name], [Status]) ON SOURCE.[Name] = TARGET.[Name]
    // WHEN MATCHED THEN UPDATE 
    //     SET [Status]=SOURCE.[Status]
    // WHEN NOT MATCHED THEN 
    //     INSERT ([Name],[Status]) VALUES (SOURCE.[Name], SOURCE.[Status]);

    let entityType = entity.GetType() |> getUnderlyingtype
    let table = typeof<'Table> |> getTable |> escapeName
    let values = entityType |> getMembers |> List.map parameterName |> join ","
    let projection = entityType |> getMembers |> List.map escapeName |> join ","

    let ids = typeof<'Table> |> getKeys
    let assignMatched = entityType |> getMembers
                                   |> List.except ids
                                   |> List.map (fun x -> sprintf "%s=SOURCE.%s" (escapeName x) (escapeName x)) |> join ","
    let assignUnmatched = entityType |> getMembers
                                     |> List.map (fun x -> sprintf "SOURCE.%s" (escapeName x)) |> join ","
    let where = ids |> List.map (fun x -> sprintf "SOURCE.%s=TARGET.%s" (escapeName x) (escapeName x)) |> join " AND "
    let count = getCount entity

    let sql = $"MERGE INTO {table} as TARGET USING (VALUES({values})) AS SOURCE ({projection}) ON {where} WHEN MATCHED THEN UPDATE SET {assignMatched} WHEN NOT MATCHED THEN INSERT ({projection}) VALUES ({assignUnmatched});"
    if count <> connection.Execute(sql, entity) then failwith "Failed to Upsert"
