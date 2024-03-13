open GoogleCloudProvider
open System

type a = { Id: string }

type b = { Id: string }

type du =
    | A of a
    | B of b

[<EntryPoint>]
let main _ =

    let f1 (data: a seq) : du seq = data |> Seq.map A

    let f2 (data: du seq) : a seq =
        data
        |> Seq.choose (function
            | A a -> Some a
            | _ -> None)

    let f2 (data: du seq) : a seq =
        data
        |> Seq.choose (fun x ->
            match x with
            | A a -> Some a
            | _ -> None)

    Async.RunSynchronously <| downloadFilesTo Environment.CurrentDirectory


    0
