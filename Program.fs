open GoogleCloudProvider
open System

[<EntryPoint>]
let main _ =

    Async.RunSynchronously <| downloadFilesTo Environment.CurrentDirectory

    0
