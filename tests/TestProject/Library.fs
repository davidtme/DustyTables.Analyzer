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
                "DateTest_", Sql.dateTimeOrNone (Some System.DateTime.Now)
                //"asd", Sql.string row.Col1
                ])
            |> Sql.query "
                 SELECT
                    HashCode
                  FROM #Temp
                  WHERE HashCode = @hashCode"
            |> Sql.parameters [
                "@hashCode_", Sql.string "Hello world" ]
            |> Sql.executeStream (fun reader ->
                {
                    Id = 1
                    Username = reader.string "HashCode"
                })

    }