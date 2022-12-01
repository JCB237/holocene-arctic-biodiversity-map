
open BiodiversityCoder.Core

let run () = 
    result {
        printfn "Loading graph"
        let! graph = Storage.loadOrInitGraph "../../data/"

        // Required output:
        // TSV containing:
        // - LatitudeDD
        // - LongitudeDD
        // - LatinName
        // - ProxyGroup
        // - CalYearOldest
        // - CalYearYoungest

        printfn "Finding included and complete sources"

        let! sourceIds = 
            graph.Nodes<Sources.SourceNode> ()
            |> Result.ofOption "Could not load sources"

        // Get all sources that are included and finished digitising.
        let! includedAndCompleteSources =
            sourceIds
            |> Seq.map(fun kv -> kv.Key)
            |> Seq.toList
            |> Storage.loadAtoms graph.Directory (typeof<Sources.SourceNode>).Name
            |> Result.map(fun sources ->
                sources
                |> List.map(fun (s:Graph.Atom<GraphStructure.Node, GraphStructure.Relation>) -> 
                    match s |> fst |> snd with
                    | GraphStructure.Node.SourceNode s2 ->
                        match s2 with
                        | Sources.SourceNode.Included (i,prog) ->
                            match prog with
                            | Sources.CodingProgress.CompletedAll -> Some (s |> fst, i, s |> snd)
                            | _ -> None
                        | _ -> None
                    | _ -> failwith "Not a source node"
                ) |> List.choose id
            )

        printfn "%A" includedAndCompleteSources

        // Source -> many temporal extents -> many outcomes
        // one row per item here.


        return Ok
    }

match run () with
| Ok _ -> printfn "Success"
| Error e -> 
    printfn "Error: %s" e
    exit 1
