open Tesseract

type MicrofossilRecord =
    { Measure: string; MicrofossilType: string; Morphotype: string; KeyUsed: string }

let doWhile f c = seq {
    yield! f ()
    while c () do
        yield! f ()
    }

printfn "Attempting to extract morphotype lists from (pollen) diagram images"

let engine = new TesseractEngine(@"/usr/local/share/tessdata", "eng", EngineMode.Default)
engine.DefaultPageSegMode <- PageSegMode.AutoOsd

let pendingPath = "/Volumes/Server HD/GitHub Projects/holocene-arctic-biodiversity-map/src/palaeo-diagram-digitise/data/pending"
let donePath = "/Volumes/Server HD/GitHub Projects/holocene-arctic-biodiversity-map/src/palaeo-diagram-digitise/data/done"

let datasetFromDirectory path =
    System.IO.Directory.GetFiles(path, "*.json")
    |> Array.choose(fun f -> 
        let fName = f.Replace(".json", "")
        printfn "%A" fName
        if System.IO.File.Exists(System.IO.Path.Combine(path, fName + ".png")) then Some (f, fName + ".png")
        else if System.IO.File.Exists(System.IO.Path.Combine(path, fName + ".jpg")) then Some (f, fName + ".jpg")
        else None )

let inputDataset = datasetFromDirectory pendingPath

/// Get the names of each pollen 
let preprocessDiagram (jsonName:string) imgFile =

    printfn "Preprocessing %s..." imgFile

    let newTaxonJsonFile = System.IO.Path.Combine(pendingPath, jsonName.Replace(".json", "") + "_taxa.json")
    if System.IO.File.Exists newTaxonJsonFile then 
        printfn "Skipping (already done)."
        ()
    else
        // 1. Turn pollen diagram into morphotype names
        let img = Pix.LoadFromFile(System.IO.Path.Combine(pendingPath, imgFile))
        let rotatedImg = img.Rotate(angleInRadians = float32 0.785)
        let page = engine.Process rotatedImg
        let meanConfidence = page.GetMeanConfidence ()
        let iter = page.GetIterator()
        iter.Begin()
        let result = Seq.toList (seq {
            yield! doWhile (fun _ ->
                doWhile (fun _ ->
                    doWhile (fun _ -> seq {
                        yield! doWhile (fun _ ->
                            seq {
                                if iter.IsAtBeginningOf PageIteratorLevel.Block 
                                    then yield "<BLOCK>"
                                let confidence = iter.GetConfidence(PageIteratorLevel.Word)
                                if confidence >= meanConfidence then yield iter.GetText(PageIteratorLevel.Word)
                                yield "[space]"
                                if iter.IsAtFinalOf(PageIteratorLevel.TextLine, PageIteratorLevel.Word)
                                    then yield "[end line]"
                            }
                        ) (fun _ -> iter.Next(PageIteratorLevel.TextLine, PageIteratorLevel.Word))
                        if iter.IsAtFinalOf(PageIteratorLevel.Para, PageIteratorLevel.TextLine)
                            then yield "[end paragraph]"
                    }) (fun _ -> iter.Next(PageIteratorLevel.Para, PageIteratorLevel.TextLine))
                ) (fun _ -> iter.Next(PageIteratorLevel.Block, PageIteratorLevel.Para))
            ) (fun _ -> iter.Next(PageIteratorLevel.Block))
        })

        let groupedIntoTaxa =
            List.fold (fun (state,currentName) i ->
                if i = "[space]" then (state, currentName + " ")
                else if i = "[end line]" || i = "<BLOCK>" || i = "[end paragraph]" 
                then (currentName :: state, "")
                else (state, currentName + i)
                ) ([],"") result 
            |> fst
            |> List.map(fun s -> s.Trim())
            |> List.where(fun s -> s.Length > 0)
            |> List.map(fun s -> { MicrofossilType = "pollen"; Morphotype = s; Measure = "abundance"; KeyUsed = "unknown" })
            |> List.rev

        printfn "Identified %i taxa." groupedIntoTaxa.Length

        groupedIntoTaxa
        |> System.Text.Json.JsonSerializer.Serialize
        |> fun t -> System.IO.File.WriteAllText(newTaxonJsonFile, t)


module AddToGraph =

    open BiodiversityCoder.Core
    open BiodiversityCoder.Core.GraphStructure
    open FSharp.Data

    type Data = JsonProvider<"diagram.json">

    open Exposure.StudyTimeline
    open FieldDataTypes
    open Population.Context
    open Population

    let addSite graph (records:MicrofossilRecord list) (row:Data.Root) = result {
        printfn "Finding source"
        // If a database entry, make the database entry node here with updated graph.
        // If a bibliographic source, load the source node with original graph.
        let! sourceNode = 
            graph
            |> Storage.atomByKey<Sources.SourceNode> (Graph.UniqueKey.FriendlyKey ("sourcenode", row.StudyId))
            |> Result.ofOption ("Could not load source: " + row.StudyId)
        printfn "Found source."

        let timelineNode =
            ExposureNode <|
            Exposure.ExposureNode.TimelineNode(Continuous <| Irregular)

        let earliestExtentBP = row.Site.EarliestYearYbp
        let earliestExtentAgeEarlyLate =
            if row.Site.EarliestYearYbpUncertainty <> 0
            then Some(row.Site.EarliestYearYbp + row.Site.EarliestYearYbpUncertainty, row.Site.EarliestYearYbp - row.Site.EarliestYearYbpUncertainty)
            else None
        let latestExtentBP = row.Site.LatestYearYbp
        let latestExtentAgeEarlyLate =
            if row.Site.LatestYearYbpUncertainty <> 0
            then Some(row.Site.LatestYearYbp + row.Site.LatestYearYbpUncertainty, row.Site.LatestYearYbp - row.Site.LatestYearYbpUncertainty)
            else None

        let individualDates =
            row.Dates |> Seq.map(fun date ->
                match date.Type with
                | "Radiocarbon" ->
                    let depth =
                        if date.DepthCmMin = date.DepthCmMax 
                        then StratigraphicSequence.createDepth date.DepthCmMin |> Result.forceOk |> StratigraphicSequence.DepthPoint
                        else StratigraphicSequence.DepthBand (StratigraphicSequence.createDepth date.DepthCmMax |> Result.forceOk, StratigraphicSequence.createDepth date.DepthCmMin |> Result.forceOk)
                    let dateError =
                        if date.YearError = 0 then OldDate.MeasurementError.NoDatingErrorSpecified
                        else OldDate.MeasurementError.DatingErrorPlusMinus (float date.YearError * 1.<OldDate.calYearBP> )
                    match date.Unit with
                    | "YBP" -> 
                        {
                            Date = OldDate.OldDatingMethod.RadiocarbonUncalibrated <| (float date.Year) * 1.<OldDate.uncalYearBP>
                            MaterialDated = FieldDataTypes.Text.createShort date.MaterialDated |> Result.forceOk
                            SampleDepth = Some depth
                            Discarded = date.Discarded
                            MeasurementError = dateError
                        }
                    | "cal yr BP" ->
                        {
                            Date = OldDate.OldDatingMethod.RadiocarbonCalibrated((float date.Year) * 1.<OldDate.calYearBP>, date.CalCurve |> Text.createShort |> Result.forceOk)
                            MaterialDated = FieldDataTypes.Text.createShort date.MaterialDated |> Result.forceOk
                            SampleDepth = Some depth
                            Discarded = date.Discarded
                            MeasurementError = dateError
                        }
                    | _ -> failwith "Unknown radiocarbon unit"
                | _ -> failwith "Unknown date type"
                |> Exposure.ExposureNode.DateNode
                |> ExposureNode
                )

        let samplingLocation =
            match row.Site.IsPoly with
            | false -> Geography.Site((Geography.createLatitude (float row.Site.LatDd) |> Result.forceOk), (Geography.createLongitude (float row.Site.LonDd) |> Result.forceOk))
            | true -> Geography.Area(Geography.Polygon.TryCreate (SimpleValue.Text row.Site.PolyWkt) |> Result.ofOption "Bad WKT" |> Result.forceOk)

        let! origin =
            match row.Site.Origin with
            | "LakeSediment" -> Ok LakeSediment
            | "PeatCore" -> Ok PeatCore
            | _ -> Error "Only lake sediments and peat cores are supported"

        let contextNode = Node.PopulationNode <| PopulationNode.ContextNode {
            Name = Text.createShort row.Site.Name |> Result.forceOk
            SamplingLocation = samplingLocation
            SampleOrigin = origin
            SampleLocationDescription = None
        }

        let! proxyTaxaWithMeasures =
            records
            |> List.map(fun r ->
                match r.MicrofossilType with
                | "pollen" -> BioticProxies.MicrofossilGroup.Pollen |> Ok
                | "macrofossil" -> BioticProxies.MicrofossilGroup.PlantMacrofossil |> Ok
                | "diatom" -> BioticProxies.MicrofossilGroup.Diatom |> Ok
                | "ostracod" -> BioticProxies.MicrofossilGroup.Ostracod |> Ok
                | s when s.StartsWith "other_" -> (s.Replace("other_", "").Trim() |> Text.createShort) |> Result.lift BioticProxies.MicrofossilGroup.OtherMicrofossilGroup
                | _ -> Error "Unknown microfossil group"
                |> Result.bind(fun g -> 
                    r.Morphotype |> Text.createShort |> Result.lift(fun m -> BioticProxies.Microfossil(g, m)))
                |> Result.lift(fun proxy ->
                    let measure = 
                        match r.Measure with
                        | "presence-absence" -> Outcomes.Biodiversity.PresenceAbsence
                        | "abundance" -> Outcomes.Biodiversity.Abundance
                        | _ -> failwithf "Unknown biodiversity measure %s" r.Measure
                    proxy |> BioticProxies.Morphotype, measure)
            ) |> Result.ofList

        let mayOrMayNotExistTaxon = (Population.Taxonomy.Kingdom("Unknown (not yet coded)" |> Text.createShort |> Result.forceOk))
        let existingInference = Population.BioticProxies.InferenceMethodNode.IdentificationKeyOrAtlas (Text.create "Unknown (not yet coded)" |> forceOk)

        printfn "Validated input data. Adding nodes and relations to graph..."

        return! result {

            let! startDateNode = Storage.atomByKey (Graph.UniqueKey.FriendlyKey("calyearnode", sprintf "%iybp" <| earliestExtentBP)) graph |> Result.ofOption ""
            let! endDateNode = Storage.atomByKey (Graph.UniqueKey.FriendlyKey("calyearnode", sprintf "%iybp" <| latestExtentBP)) graph |> Result.ofOption ""
            let! measureNode = Storage.atomByKey (Graph.UniqueKey.FriendlyKey("biodiversitydimensionnode", "abundance")) graph |> Result.ofOption ""

            // Add things now, only after checks are complete.
            let! (graphWithDatasetEntry,addedTimelineNode) = 
                graph
                |> fun g -> Storage.addNodes g (Seq.append [ 
                    timelineNode
                    contextNode ] individualDates)
                |> Result.bind(fun (g, addedNodes) -> 
                    let timelineNode = addedNodes |> Seq.find(fun s -> (s |> fst |> snd).NodeType() = "IndividualTimelineNode")
                    let contextNode = addedNodes |> Seq.find(fun s -> (s |> fst |> snd).NodeType() = "ContextNode")
                    let dateNodes = addedNodes |> Seq.where(fun s -> (s |> fst |> snd).NodeType() = "IndividualDateNode")

                    Storage.addRelation sourceNode timelineNode (ProposedRelation.Source Sources.SourceRelation.HasTemporalExtent) g
                    |> Result.bind(fun g -> Storage.addRelation timelineNode startDateNode (ProposedRelation.Exposure Exposure.ExposureRelation.ExtentEarliest) g )
                    |> Result.bind(fun g -> Storage.addRelation timelineNode endDateNode (ProposedRelation.Exposure Exposure.ExposureRelation.ExtentLatest) g )
                    |> Result.bind(fun g -> Storage.addRelation timelineNode contextNode (ProposedRelation.Exposure Exposure.ExposureRelation.IsLocatedAt) g )
                    |> Result.bind(fun g ->
                        match earliestExtentAgeEarlyLate with
                        | Some (early, late) -> 
                            let early = Storage.atomByKey (Graph.UniqueKey.FriendlyKey("calyearnode", sprintf "%iybp" <| early)) graph
                            let late = Storage.atomByKey (Graph.UniqueKey.FriendlyKey("calyearnode", sprintf "%iybp" <| late)) graph
                            Storage.addRelation timelineNode early.Value (ProposedRelation.Exposure Exposure.ExposureRelation.ExtentEarliestUncertainty) g
                            |> Result.bind(fun g ->
                                    Storage.addRelation timelineNode late.Value (ProposedRelation.Exposure Exposure.ExposureRelation.ExtentEarliestUncertainty) g )
                        | None -> Ok g )
                    |> Result.bind(fun g ->
                        match latestExtentAgeEarlyLate with
                        | Some (early, late) -> 
                            let early = Storage.atomByKey (Graph.UniqueKey.FriendlyKey("calyearnode", sprintf "%iybp" <| early)) graph
                            let late = Storage.atomByKey (Graph.UniqueKey.FriendlyKey("calyearnode", sprintf "%iybp" <| late)) graph
                            Storage.addRelation timelineNode early.Value (ProposedRelation.Exposure Exposure.ExposureRelation.ExtentLatestUncertainty) g
                            |> Result.bind(fun g ->
                                    Storage.addRelation timelineNode late.Value (ProposedRelation.Exposure Exposure.ExposureRelation.ExtentLatestUncertainty) g )
                        | None -> Ok g )
                    |> Result.bind(fun g ->
                        // Add a relation from the timeline to each date node.
                        Seq.fold(fun state dateNode ->
                            state
                            |> Result.bind(fun g ->
                                Storage.addRelation timelineNode dateNode (ProposedRelation.Exposure Exposure.ExposureRelation.ConstructedWithDate) g )
                        ) (Ok g) dateNodes
                        )
                    |> Result.lift(fun r -> r, timelineNode)
                )

            let! graphWithTaxa =
                Seq.fold(fun state (t,x) -> 
                    state 
                    |> Result.bind(fun s ->
                        // Add proxy if it doesn't exist
                        match Storage.addNodes s [ Node.PopulationNode <| PopulationNode.BioticProxyNode t ] with 
                        | Ok o -> Ok o
                        | Error e -> if e = "node already exists" then Ok (s,[]) else Error e)
                    |> Result.bind(fun (s,_) ->
                        Storage.addProxiedTaxon
                            t
                            mayOrMayNotExistTaxon
                            []
                            existingInference
                            s
                        |> Result.bind(fun (g, proxiedKey) -> 
                            Storage.addRelationByKey g (addedTimelineNode |> fst |> fst) proxiedKey (ProposedRelation.Exposure Exposure.ExposureRelation.HasProxyInfo)
                            |> Result.lift(fun r -> r, proxiedKey))
                        |> Result.bind(fun (g, proxiedKey) -> 
                            Storage.addRelationByKey g proxiedKey (measureNode |> fst |> fst) (ProposedRelation.Population <| Population.PopulationRelation.MeasuredBy) )
                        
                        )
                ) (Ok graphWithDatasetEntry) proxyTaxaWithMeasures

            return graphWithTaxa
        }
    }

// Step 1. Extract morphotypes from each pollen (/ other group) diagram and 
// save into seperate json array file.
inputDataset
|> Array.iter(fun (a,b) -> preprocessDiagram a b)

engine.Dispose()

printfn "--------------"
printfn "Success!"
printfn "Pre-processed microfossil taxon information from images into json files."
printfn "Please check generated json and amend taxon names, biodiversity measures, morphotype keys used, and microfossil group names (e.g. pollen) now."
printfn "Check the generated json for accuracy. You may quit (ctrl+C) or continue to add new files to graph (return/enter)."
System.Console.ReadLine() |> ignore

// Step 2. Add un-added datasets into graph
open BiodiversityCoder.Core
open BiodiversityCoder.Core.FieldDataTypes

printfn "Loading graph"

let graph = Storage.loadOrInitGraph "../../data/"

let loadedData =
    inputDataset
    |> Seq.map(fun (jsonFile,imgName) ->
        let json = AddToGraph.Data.Load jsonFile
        let json2 = 
            jsonFile.Replace(".json", "_taxa.json")
            |> System.IO.File.ReadAllText
            |> System.Text.Json.JsonSerializer.Deserialize<list<MicrofossilRecord>>
        json, json2, [jsonFile; imgName; jsonFile.Replace(".json", "_taxa.json")])

match graph with
| Ok g ->

    let mayOrMayNotExistTaxon = (Population.Taxonomy.Kingdom("Unknown (not yet coded)" |> Text.createShort |> Result.forceOk))
    let existingInference = Population.BioticProxies.InferenceMethodNode.IdentificationKeyOrAtlas (Text.create "Unknown (not yet coded)" |> forceOk)
    let g = Storage.addNodes g [ 
        GraphStructure.Node.PopulationNode <| GraphStructure.TaxonomyNode mayOrMayNotExistTaxon
        GraphStructure.Node.PopulationNode <| GraphStructure.InferenceMethodNode existingInference ] |> Result.forceOk |> fst

    match (
        Seq.fold(fun graph (json, microfossils, files) ->
            graph |> Result.bind(fun g ->
                match AddToGraph.addSite g microfossils json with
                | Ok g -> 
                    files |> List.iter(fun f -> System.IO.File.Move(f, f.Replace(pendingPath, donePath)))
                    Ok g
                | Error e -> Error e )
            ) (Ok g) loadedData ) with
    | Ok _ -> ()
    | Error e ->
        printfn "Error: %s" e
        exit 1
| Error _ -> 
    printfn "Could not load graph"
    exit 1
