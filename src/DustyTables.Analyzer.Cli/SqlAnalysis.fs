module SqlAnalysis

open System
open System.Data
open Microsoft.Data.SqlClient
open DustyTables.Analyzer.Core
open FSharp.Core

let createWarning (message: string) (range) : Message =
    { Message = message
      Type = "SQL Analysis"
      Code = "SQL0001"
      Severity = Warning
      Range = range
      Fixes = [ ] }

let findQuery (operation: SqlOperation) =
    operation.blocks
    |> List.tryFind (function | SqlAnalyzerBlock.Query(query, range) -> true | _ -> false)
    |> Option.map(function | SqlAnalyzerBlock.Query(query, range) -> (query, range) | _ -> failwith "should not happen")

let findParameters (operation: SqlOperation) =
    operation.blocks
    |> List.tryFind (function | SqlAnalyzerBlock.Parameters(parameters, range) -> true | _ -> false)
    |> Option.map(function | SqlAnalyzerBlock.Parameters(parameters, range) -> (parameters, range) | _ -> failwith "should not happen")

[<RequireQualifiedAccess>]
type SqlType =
    | String of int
    | DateTime of int
    | DateTime2 of int
    | Date
    | Int

open System.Text.RegularExpressions
let (|Regex|_|) pattern input =
    let m = Regex.Match(input, pattern, RegexOptions.IgnoreCase)
    if m.Success then Some(List.tail [ for g in m.Groups -> g.Value ])
    else None

let sqlTypeFromString s =
    match s with
    | Regex @"nvarchar\(([0-9]+)\)" [ l ] -> SqlType.String (Int32.Parse(l))
    | Regex @"nvarchar\(max\)" [] -> SqlType.String -1
    | Regex @"datetime2\(([0-9]+)\)" [ l ] -> SqlType.DateTime2 (Int32.Parse(l))
    | Regex @"datetime\(([0-9]+)\)" [ l ] -> SqlType.DateTime (Int32.Parse(l))
    | Regex @"date" [] -> SqlType.Date
    | Regex @"int" [] -> SqlType.Int

    | x -> invalidOp $"Unhandled sql type: {x}"

let extractParameters connection query =
    try 
        let parameters = [
            use cmd = new SqlCommand("sys.sp_describe_undeclared_parameters", connection, CommandType = CommandType.StoredProcedure)
            cmd.Parameters.AddWithValue("@tsql", query) |> ignore

            use cursor = cmd.ExecuteReader()
            while cursor.Read() do
                let name = string cursor.["name"]                   
                let typ = string cursor.["suggested_system_type_name"] |> sqlTypeFromString
                name, typ
            ]

        Ok (parameters)

    with e ->
        Error e.Message






let extractOutputColumns connection query =
    try 
        let outputColumns = [
            use cmd = new SqlCommand("sys.sp_describe_first_result_set", connection, CommandType = CommandType.StoredProcedure)
            cmd.Parameters.AddWithValue("@tsql", query) |> ignore

            use cursor = cmd.ExecuteReader()
            while cursor.Read() do
                let name = string cursor.["name"]                   
                let nullable : bool = unbox cursor.["is_nullable"]
                let typ = string cursor.["system_type_name"] |> sqlTypeFromString

                name, typ, nullable
            ]

        Ok (outputColumns)
                            


    with e ->
        Error e.Message
    
let findColumnReadAttempts (operation: SqlOperation) =
    operation.blocks
    |> List.tryFind (function | SqlAnalyzerBlock.ReadingColumns(attempts) -> true | _ -> false)
    |> Option.map(function | SqlAnalyzerBlock.ReadingColumns(attempts) -> attempts | _ -> failwith "should not happen")


let findTempColumns (operation: SqlOperation) =
    operation.blocks
    |> List.tryFind (function | SqlAnalyzerBlock.TempTableColumns(attempts, _) -> true | _ -> false)
    |> Option.map(function | SqlAnalyzerBlock.TempTableColumns(attempts, range) -> attempts, range | _ -> failwith "should not happen")

let findColumn (name: string) availableColumns =
    availableColumns
    |> List.tryFind (fun (n, _, _) -> n = name)

let checkColType_ shouldCheckNull name typ nullable =
    let checkNull shouldBe =
        if shouldCheckNull then
            shouldBe = nullable
        else true

    match name, typ with
    | Regex(@"\.int$") [], SqlType.Int -> checkNull false
    | Regex(@"\.intOrNone$") [], SqlType.Int -> checkNull true
    | Regex(@"\.string$") [], SqlType.String _ -> checkNull false
    | Regex(@"\.stringOrNone$") [], SqlType.String _ -> checkNull true
    | Regex(@"\.dateTime$") [], (SqlType.DateTime2 _ | SqlType.DateTime _ | SqlType.Date) -> checkNull false
    | Regex(@"\.dateTimeOrNone$") [], (SqlType.DateTime2 _ | SqlType.DateTime _ | SqlType.Date) -> checkNull true
    | _ -> false

let checkColTypeWithNullable name typ nullable =
    checkColType_ true name typ nullable

let checkColType name typ =
    checkColType_ false name typ false

let analyzeParameters parameterAttempts availableParameters = [
    match parameterAttempts with
    | None -> ()
       
    | Some (parameterAttempts, range) ->
        for (attempt : UsedParameter) in parameterAttempts do
            match availableParameters |> List.tryFind(fun (n, _) -> n = "@" + attempt.name) with
            | None ->
                createWarning $"@{attempt.name} cannot be found" attempt.paramFuncRange

            | Some (_, typ) ->
                let pass = checkColType attempt.paramFunc typ
                
                if not pass then 
                    createWarning $"@{attempt.name} is the wrong parameter type" attempt.paramFuncRange
]



let analyzeColumnReadingAttempts (columnReadAttempts: ColumnReadAttempt list) availableColumns = [
    for attempt in columnReadAttempts do
        match findColumn attempt.columnName availableColumns with
        | None ->
            createWarning $"{attempt.columnName} cannot be found" attempt.funcCallRange

        | Some (_, typ, nullable) -> 
            let pass = checkColTypeWithNullable attempt.funcName typ nullable

            if not pass then 
                createWarning $"{attempt.columnName} is trying to read the wrong type" attempt.funcCallRange
            
    ]

let analyzeTempTableColumns parameterAttempts availableParameters = [
    match parameterAttempts with
    | None -> ()
       
    | Some (parameterAttempts, range) ->
        for (name, typ, nullable) in availableParameters do
            match parameterAttempts |> List.tryFind(fun (attempt : UsedParameter) -> attempt.name = name) with
            | None ->
                createWarning $"{name} cannot be found on the table" range

            | Some (attempt : UsedParameter) ->
                let pass = checkColTypeWithNullable attempt.paramFunc typ nullable

                if not pass then 
                    createWarning $"{attempt.name} is the wrong type" attempt.paramFuncRange
                
        for attempt in parameterAttempts do                
            if availableParameters |> List.exists(fun (n, _, _) -> attempt.name = n) then
                ()
            else    
                createWarning $"{attempt.name} is missing" attempt.paramFuncRange
    ]

let analyzeOperation (operation: SqlOperation) (connectionString: string) =
    match findQuery operation with
    | None -> 
        []
        //invalidOp "Not done"

    | Some (query, queryRange) ->
        try
            use connection = new SqlConnection(connectionString)
            connection.Open()

            let tempTables = ResizeArray()

            let tempMessages = 
                operation.blocks
                |> List.collect(fun block ->
                    match block with
                    | SqlAnalyzerBlock.CreateTempTable (tempTableQuery, range) -> 
                        let name = Regex(@"CREATE TABLE #([^(]+)\(", RegexOptions.IgnoreCase).Match(tempTableQuery).Groups.[1].Value
                        tempTables.Add name

                        let tempTableQuery = tempTableQuery.Replace("#", "##")
                    
                        use cmd = connection.CreateCommand()
                        cmd.CommandText <- tempTableQuery
                        cmd.ExecuteNonQuery() |> ignore

                        // Read the output from the temp table
                        match extractOutputColumns connection $"SELECT * FROM ##{name}" with
                        | Ok columns ->
                            let writeColumns = findTempColumns operation
                            analyzeTempTableColumns writeColumns columns

                        | _ -> 
                            invalidOp "bang"

                    | _ -> 
                        []
                )

            let tempQuery = query.Replace("#", "##")

            let queryMessages = 
                match extractParameters connection tempQuery with
                | Error msg ->
                    [ createWarning $"Sql Query Error: {msg}" queryRange ]

                | Ok parameters ->
                    match extractOutputColumns connection tempQuery with
                    | Error msg -> 
                        [ createWarning $"Sql Query Error: {msg}" queryRange ]

                    | Ok outputColumns -> 
                        let parameterAttempts = findParameters operation
                        let readingAttempts = findColumnReadAttempts operation |> Option.defaultValue []
            
                        [ yield! analyzeParameters parameterAttempts parameters
                          yield! analyzeColumnReadingAttempts readingAttempts outputColumns ]

            tempTables
            |> Seq.iter(fun name ->
                use cmd = connection.CreateCommand()
                cmd.CommandText <- $"DROP TABLE ##{name}"
                cmd.ExecuteNonQuery() |> ignore
            )

            [ yield! tempMessages
              yield! queryMessages ]

        with e ->
            [ createWarning $"Sql Connection Error: {e.Message}" queryRange ]


        