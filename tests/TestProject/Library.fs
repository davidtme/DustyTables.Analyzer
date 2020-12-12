module TestProject

open DustyTables
open Microsoft.Data.SqlClient
open System.Collections.Generic
open System.Data
open System.Text.RegularExpressions

// get the connection from the environment
type User =
    { Id : int
      Username : string
      }

//type TempTableLoader(fieldCount, items: obj seq) =
//    let enumerator = items.GetEnumerator()

//    interface IDataReader with
//        member this.FieldCount: int = fieldCount
//        member this.Read(): bool = enumerator.MoveNext()
//        member this.GetValue(i: int): obj =
//            let row : obj[] = unbox enumerator.Current
//            row.[i]
//        member this.Dispose(): unit = ()

//        member __.Close(): unit = invalidOp "NotImplementedException"
//        member __.Depth: int = invalidOp "NotImplementedException"
//        member __.GetBoolean(_: int): bool = invalidOp "NotImplementedException"
//        member __.GetByte(_ : int): byte = invalidOp "NotImplementedException"
//        member __.GetBytes(_ : int, _ : int64, _ : byte [], _ : int, _ : int): int64 = invalidOp "NotImplementedException"
//        member __.GetChar(_ : int): char = invalidOp "NotImplementedException"
//        member __.GetChars(_ : int, _ : int64, _ : char [], _ : int, _ : int): int64 = invalidOp "NotImplementedException"
//        member __.GetData(_ : int): IDataReader = invalidOp "NotImplementedException"
//        member __.GetDataTypeName(_ : int): string = invalidOp "NotImplementedException"
//        member __.GetDateTime(_ : int): System.DateTime = invalidOp "NotImplementedException"
//        member __.GetDecimal(_ : int): decimal = invalidOp "NotImplementedException"
//        member __.GetDouble(_ : int): float = invalidOp "NotImplementedException"
//        member __.GetFieldType(_ : int): System.Type = invalidOp "NotImplementedException"
//        member __.GetFloat(_ : int): float32 = invalidOp "NotImplementedException"
//        member __.GetGuid(_ : int): System.Guid = invalidOp "NotImplementedException"
//        member __.GetInt16(_ : int): int16 = invalidOp "NotImplementedException"
//        member __.GetInt32(_ : int): int = invalidOp "NotImplementedException"
//        member __.GetInt64(_ : int): int64 = invalidOp "NotImplementedException"
//        member __.GetName(_ : int): string = invalidOp "NotImplementedException"
//        member __.GetOrdinal(_ : string): int = invalidOp "NotImplementedException"
//        member __.GetSchemaTable(): DataTable = invalidOp "NotImplementedException"
//        member __.GetString(_ : int): string = invalidOp "NotImplementedException"
//        member __.GetValues(_ : obj []): int = invalidOp "NotImplementedException"
//        member __.IsClosed: bool = invalidOp "NotImplementedException"
//        member __.IsDBNull(_ : int): bool = invalidOp "NotImplementedException"
//        member __.Item with get (_ : int): obj = invalidOp "NotImplementedException"
//        member __.Item with get (_ : string): obj = invalidOp "NotImplementedException"
//        member __.NextResult(): bool = invalidOp "NotImplementedException"
//        member __.RecordsAffected: int = invalidOp "NotImplementedException"

//let colReg = 
//    [ "bigint"
//      "binary"
//      "bit"
//      "char"
//      "datetimeoffset"
//      "datetime2"
//      "datetime"
//      "date"
//      "decimal"
//      "float"
//      "image"
//      "int"
//      "nchar"
//      "ntext"
//      "nvarchar"
//      "real"
//      "timestamp"
//      "varbinary" ]
//    |> String.concat "|"
//    |> fun x -> Regex(@"[\[]{0,1}([a-z0-9\-_]+)[\]]{0,1} (?:"+x+")", RegexOptions.IgnoreCase)

//[<RequireQualifiedAccess>]
//module Sql =
//    type TempTable = 
//        { Name : string 
//          Columns : Map<string, int> }

//    let createTempTable table (props : Sql.SqlProps) = 
//        let connection = Sql.getConnection props
//        if not (connection.State.HasFlag ConnectionState.Open) then connection.Open()

//        use command = new SqlCommand(table, connection)
//        command.ExecuteNonQuery() |> ignore

//        let name = Regex("(#[a-z0-9\\-_]+)", RegexOptions.IgnoreCase).Match(table).Groups.[1].Value

//        let columns = 
//            colReg.Matches(table)
//            |> Seq.cast
//            |> Seq.mapi(fun i (m : Match) -> m.Groups.[1].Value, i )
//            |> Map.ofSeq

//        let info =
//            { TempTable.Name = name
//              Columns = columns }
        
//        ({ props with ExistingConnection = Some connection }, info)

//    let tempTableData data (props, info : TempTable) =
//        (props, info, data)

//    let loadTempTable mapper (props : Sql.SqlProps, info : TempTable, data) =
//        let items =
//            data
//            |> Seq.map(fun item -> 
//                let cols = mapper item

//                let arr = Array.zeroCreate info.Columns.Count
//                cols
//                |> List.iter(fun (name, p : SqlParameter) -> 
//                    let index = info.Columns |> Map.find name
//                    arr.[index] <- p.Value
//                )
//                box arr
//            )

//        use reader = new TempTableLoader(info.Columns.Count, items)

//        use bulkCopy = new SqlBulkCopy(props.ExistingConnection.Value)
//        props.Timeout |> Option.iter (fun x -> bulkCopy.BulkCopyTimeout <- x)
//        bulkCopy.BatchSize <- 5000
//        bulkCopy.DestinationTableName <- info.Name
//        bulkCopy.WriteToServer(reader)

//        props

//    let executeStream (read: RowReader -> 't) (props : Sql.SqlProps) =
//        seq {
//            if props.SqlQuery.IsNone then failwith "No query provided to execute. Please use Sql.query"
//            let connection = Sql.getConnection props
//            try
//                if not (connection.State.HasFlag ConnectionState.Open)
//                then connection.Open()
//                use command = new SqlCommand(props.SqlQuery.Value, connection)
//                props.Timeout |> Option.iter (fun x -> command.CommandTimeout <- x)
//                do Sql.populateCmd command props
//                if props.NeedPrepare then command.Prepare()
//                use reader = command.ExecuteReader()
//                let rowReader = RowReader(reader)
//                while reader.Read() do
//                    read rowReader
//            finally
//                if props.ExistingConnection.IsNone
//                then connection.Dispose()
//        }


let getUsers (connectionString : string) =
    seq {
        let someData = [
            {| Col1 = "Hello world"; Col2 = 1 |}
        ]

        yield! 
            connectionString
            |> Sql.connect
            |> Sql.createTempTable 
                "CREATE TABLE #Temp(
                    HashCode NVARCHAR(50) NOT NULL,
                    DateTest DATETIME2(0) NULL
                )"
            |> Sql.tempTableData someData
            |> Sql.loadTempTable (fun row -> [
                "HashCode", Sql.string row.Col1
                "DateTest", Sql.dateTimeOrNone (Some System.DateTime.Now)
                //"asd", Sql.string row.Col1
                ])
            |> Sql.query "
                 SELECT
                    HashCode
                  FROM #Temp
                  WHERE HashCode = @hashCode"
            |> Sql.parameters [
                "@hashCode", Sql.string "Hello world" ]
            |> Sql.executeStream (fun reader ->
                {
                    Id = 1
                    Username = reader.string "HashCode"
                })

    }