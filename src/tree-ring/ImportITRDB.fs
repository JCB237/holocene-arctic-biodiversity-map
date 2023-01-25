module ImportITRDB

open BiodiversityCoder.Core
open FSharp.Data

type Data = CsvProvider<"input-list.csv">

[<Literal>]
let itrdbIndex = "data/itrdb-import-20220112.csv"

type ITRDBIndex = CsvProvider<Sample = itrdbIndex>
type ITRDBStudy = JsonProvider<"noaa-sample.json", Encoding = "UTF-8">

let dataDir = "../../data/"
let itrdbStudy i = sprintf "https://www.ncei.noaa.gov/pub/data/metadata/published/paleo/json/noaa-tree-%i.json" i

let import () = result {

    let! (graph: Storage.FileBasedGraph<GraphStructure.Node,GraphStructure.Relation>) = Storage.loadOrInitGraph dataDir

    // Get all source keys in database
    // Get all source keys in database
    let sourceAtoms = 
        (graph.Nodes<Sources.SourceNode> ()).Value
        |> Map.toList |> List.map fst
        |> Storage.atomsByKey<Sources.SourceNode> graph

    let! (sources: (Graph.UniqueKey * Sources.SourceNode) list) = 
        sourceAtoms
        |> Seq.map (fun a -> 
            match a |> fst |> snd with
            | GraphStructure.Node.SourceNode s -> Ok (a |> fst |> fst, s)
            | _ -> Error "Not a source node" )
        |> Seq.toList
        |> Result.ofList
    
    // Make a list of graph sourceIDs and their title and year
    let sourceTitleByKey =
        sources |> List.choose(fun s ->
            match snd s with
            | Sources.SourceNode.Unscreened u ->
                match u with
                | Sources.Source.Bibliographic b ->
                    match b.Title with
                    | Some t -> Some (fst s, t.Value, b.Year)
                    | None -> None
                | _ -> None
            | _ -> None )
    
    // Load and filter ITRDB index by lat and lon
    let db = ITRDBIndex.Load itrdbIndex

    // For each database entry, load the metadaa file to find out the study.
    // Find the match between our database and the ITRDB.
    let matches =
        db.Rows |> Seq.choose(fun r ->

            printfn "Processing row %s" r.ITRDB_Code

            let json = ITRDBStudy.Load (itrdbStudy r.Study_id)
            let publication = 
                json.Publication
                |> Seq.tryFind(fun p ->
                    p.Type = "publication")

            printfn "Match is: %A" (json.Publication |> Seq.tryHead |> Option.map(fun s -> s.Title))

            match publication with
            | None -> Some (json, (Graph.UniqueKey.FriendlyKey("sourcenode", sprintf "database_itrdb_%s" (json.StudyCode.ToLower())), sprintf "itrdb_%s" (json.StudyCode.ToLower()), None))
            | Some p ->
                sourceTitleByKey
                |> List.tryFind(fun (k,title,year) ->
                    if year.IsSome then title.ToLower() = p.Title.ToLower() && p.PubYear = year.Value
                    else title.ToLower() = p.Title.ToLower())
                |> fun s -> 
                    match s with
                    | Some (a,b,c) -> printfn "Matched in graph? %s" b; Some (json, (a,b,c))
                    | None -> Some (json, (Graph.UniqueKey.FriendlyKey("sourcenode", sprintf "unknown_%s" p.Title), p.Title, None))
            )
        |> Seq.collect(fun (itrdb, m) ->
            itrdb.Site
            |> Seq.map(fun site ->
                itrdb, site, m))

    // What to do with matches?
    let results = 
        matches |> Seq.collect(fun (itrdb, site, (key,_,_)) ->
            
            printfn "Processing %s (%s)" site.SiteName itrdb.StudyCode

            let isPoly, lat,lon, poly =
                match site.Geo.Geometry.Type with
                | "POINT" -> false, float site.Geo.Geometry.Coordinates.[0], float site.Geo.Geometry.Coordinates.[1], ""
                | "POLYGON" -> true, 0., 0., sprintf "POLYGON(???)"
                | _ -> failwithf "Unknown geom type %s" site.Geo.Geometry.Type

            let palaeoData =
                site.PaleoData
                |> Seq.map(fun palaeo ->
                    let taxon =
                        if palaeo.Species.Length <> 1
                        then ""
                        else palaeo.Species.[0].ScientificName
                    palaeo.EarliestYearCe,
                    palaeo.MostRecentYearCe,
                    taxon ) |> Seq.toList

            palaeoData |> Seq.iter(fun p -> printfn "Palaeo-data is: %A" p)

            palaeoData
            |> Seq.map(fun (earliest,latest,taxon) ->
                sprintf "\"%s\",\"%s\",%b,%f,%f,\"%s\",%i,%i,%i,\"%s\",,,,\"%s\",\"%s\",\"%s\",false"
                    key.AsString
                    site.SiteName
                    isPoly
                    lat
                    lon
                    poly
                    latest
                    latest
                    earliest
                    taxon
                    itrdb.StudyName
                    itrdb.Investigators
                    itrdb.Doi
                )
        )

    System.IO.File.WriteAllText("itrdb-output.csv", "studyId,siteName,isPoly,latDD (float),lonDD (float),polyWKT (string),collectionYear,latestYear,earliestYear,genus,species,subspecies,auth,studyTitle (string),investigators (string),webLocation (string),added\n")
    System.IO.File.AppendAllLines("itrdb-output.csv", results)

    // Need to make a 'ContainsDatasetFor' relation type to point from database to source.
    // - A database is an object in and of itself as a source (e.g. ITRDB)
    // - A database may contain data from a source publication.

    return! Ok ()
}
