// Learn more about F# at http://fsharp.org

[<EntryPoint>]
let main arg =
   
    let cs = "Data Source=.; Integrated Security=True; Initial Catalog=DataWarehouse2"

    let a = 
        TestProject.getUsers cs
        |> Seq.toList

    let b = a

    0 // return an integer exit code
