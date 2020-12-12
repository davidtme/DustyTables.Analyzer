// Learn more about F# at http://fsharp.org

open System
open System.IO
open FSharp.Compiler.SourceCodeServices
open FSharp.Compiler.Text
open System.Xml
open DustyTables.Analyzer.Core
open Microsoft.Data.SqlClient
open System.Data

let checker =
    FSharpChecker.Create(
        keepAllBackgroundResolutions = true,
        keepAssemblyContents = true,
        ImplicitlyStartBackgroundWork = true)

open Argu

type OutputMode = 
    | MSBuild

type CliArguments =
    | Project of string
    | Output_Mode of OutputMode

with
    interface IArgParserTemplate with
        member s.Usage =
            match s with
            | _ -> "Export"

[<EntryPoint>]
let main arg =
    let parser = ArgumentParser.Create<CliArguments>()
    let result = parser.Parse(arg)

    let project = result.GetResult Project
    let mode = result.GetResult Output_Mode

    let document = XmlDocument()
    document.LoadXml(File.ReadAllText project)

    let fsharpFileNodes = document.GetElementsByTagName("Compile")

    let files = [
        for item in 0 .. fsharpFileNodes.Count - 1 do
            let relativePath = fsharpFileNodes.[item].Attributes.["Include"].InnerText
            let projectParent = Directory.GetParent project
            Path.Combine(projectParent.FullName, relativePath)
        ]

    let messages =
        files
        |> List.collect(fun fsharpFile -> 
            match Project.context fsharpFile with
            | Some context ->
                match SyntacticAnalysis.findSqlOperations context with
                | [] -> []
                | syntacticBlocks  ->
                    let connectionString = SqlAnalyzer.tryFindConnectionString context.FileName

                    let r =
                        syntacticBlocks
                        |> List.collect (fun block ->
                            SqlAnalysis.analyzeOperation block connectionString)
                        |> List.distinctBy (fun message -> message.Range)
                        |> List.map(fun m -> fsharpFile, m)

                    [ yield! r ]

            | _ -> 
                []
            
        )

    match mode with 
    | MSBuild ->
        messages
        |> List.iter(fun (file, msg) -> 
            Console.Error.WriteLine($"{file}({msg.Range.StartLine}) : error {msg.Code}: {msg.Message}")
        )

    if messages.IsEmpty then 0
    else 1
