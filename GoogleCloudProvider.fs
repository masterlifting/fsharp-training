module GoogleCloudProvider

open Google.Cloud.Storage.V1
open System.IO
open System
open FSharpx.Control
open Parser

let private client =
    let clientBUilder = StorageClientBuilder()

    clientBUilder.JsonCredentials <-
        File.ReadAllText
        <| Path.Combine(Environment.CurrentDirectory, "googleCloudCredentials.json")

    clientBUilder.Build()

let downloadFilesTo (path: string) =
    async {
        let options = ListObjectsOptions()
        options.Delimiter <- "/"
        let bucket = client.ListObjects("psiproject-243915.appspot.com", "PSI/", options)
        let dateMin = DateTimeOffset.Now.AddYears(-3)

        let selected =
            bucket
            |> Seq.filter (fun x ->
                Option.ofNullable x.TimeCreatedDateTimeOffset
                |> Option.map ((<=) (dateMin)) // where dateMin <= x.TimeCreatedDateTimeOffset
                |> Option.defaultValue false)
            |> Seq.take 3

        // for doc in selected do
        //     let ms = new MemoryStream()
        //     do! client.DownloadObjectAsync(doc, ms) |> Async.AwaitTask |> Async.Ignore
        //     let path = Path.Combine(path, doc.Name)
        //     use fs = File.Create(path)
        //     ms.Position <- 0L
        //     ms.CopyTo(fs)

        let distributorAgent =
            MailboxProcessor<AsyncReplyChannel<Google.Apis.Storage.v1.Data.Object option>>.Start(fun inbox ->
                let enum = selected.GetEnumerator()

                let rec innerLoop () =
                    async {
                        let! rc = inbox.Receive()

                        if enum.MoveNext() then
                            enum.Current |> Some |> rc.Reply
                        else
                            None |> rc.Reply

                        return! innerLoop ()
                    }

                innerLoop ())

        let downloadWorker continuation =
            let rec innerLoop () =
                async {
                    match! distributorAgent.PostAndAsyncReply id with
                    | Some doc ->
                        let ms = new MemoryStream()
                        do! client.DownloadObjectAsync(doc, ms) |> Async.AwaitTask |> Async.Ignore
                        do! continuation ms
                        return! innerLoop ()
                    | _ -> ()
                }

            innerLoop ()

        let bq = BlockingQueueAgent<MemoryStream option>(10)

        let parsingWorker continuation =
            let rec innerLoop () =
                async {
                    match! bq.AsyncGet() with
                    | Some ms ->
                        ms.Position <- 0L
                        let parser = xmlProvider.Load ms
                        do! ms.DisposeAsync().AsTask() |> Async.AwaitTask
                        do! continuation parser
                        return! innerLoop ()
                    | _ -> do! bq.AsyncAdd None
                }

            innerLoop ()

        let bq_parsers = BlockingQueueAgent<xmlProvider.Survey option>(10)

        let rec counterWorker acc =
            async {
                match! bq_parsers.AsyncGet() with
                | Some parser ->
                    return! counterWorker (acc + (parser.Questions.Questions |> Array.sumBy (fun x -> x.Winter)))
                | _ -> return acc // stop
            }

        async {
            do!
                List.replicate 3 (downloadWorker (Some >> bq.AsyncAdd))
                |> Async.Parallel
                |> Async.Ignore

            do! bq.AsyncAdd None
        }
        |> Async.Start

        async {
            do!
                List.replicate 3 (parsingWorker (Some >> bq_parsers.AsyncAdd))
                |> Async.Parallel
                |> Async.Ignore

            do! bq_parsers.AsyncAdd None
        }
        |> Async.Start

        let count = counterWorker 0 |> Async.RunSynchronously

        printfn "count is %i" count
    }
