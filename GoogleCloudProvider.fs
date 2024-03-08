module GoogleCloudProvider

open Google.Cloud.Storage.V1
open System.IO
open System

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
            |> Seq.take 1

        for doc in selected do
            use ms = new MemoryStream()

            do! client.DownloadObjectAsync(doc, ms) |> Async.AwaitTask |> Async.Ignore

            ms.Position <- 0L
            use file = File.Create <| Path.Combine(path, doc.Name)

            ms.CopyTo(file)
            file.Flush()
    }
