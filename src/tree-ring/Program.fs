
open BiodiversityCoder.Core
open BiodiversityCoder.Core.GraphStructure
open FSharp.Data

type Data = CsvProvider<"input-list.csv">

// Steps required.
// 1. Make a timeline in the study.

open Exposure.StudyTimeline
open FieldDataTypes
open Population.Context
open Population

let addStudy graph (row:Data.Row) = result {
    printfn "Finding source"

    let! sourceNode = 
        graph
        |> Storage.atomByKey<Sources.SourceNode> (Graph.UniqueKey.FriendlyKey ("sourcenode", row.StudyId))
        |> Result.ofOption "Could not load sources"

    // Only process data if source has no data attached.
    if 
        sourceNode |> snd |> Seq.exists(fun (_,_,_,r) -> r = GraphStructure.Relation.Source Sources.SourceRelation.HasTemporalExtent)
        then 
            printfn "Skipping %s as there is already a timeline on this source." row.StudyId
            return! Ok graph
    else

        let timelineNode =
            ExposureNode <|
            Exposure.ExposureNode.TimelineNode(
                Continuous <| Regular (1.<FieldDataTypes.OldDate.calYearBP>, WoodAnatomicalFeatures))

        let individualDate =
            ExposureNode <|
            Exposure.ExposureNode.DateNode {
                Date = OldDate.OldDatingMethod.CollectionDate <| (float row.CollectionYear) * 1.<FieldDataTypes.OldDate.AD>
                MaterialDated = FieldDataTypes.Text.createShort "wood increment" |> Result.forceOk
                SampleDepth = None
                Discarded = false
                MeasurementError = OldDate.MeasurementError.NoDatingErrorSpecified
            }

        let samplingLocation =
            match row.IsPoly with
            | false -> Geography.Site((Geography.createLatitude row.LatDD |> Result.forceOk), (Geography.createLongitude row.LonDD |> Result.forceOk))
            | true -> Geography.Area(Geography.Polygon.TryCreate (SimpleValue.Text row.PolyWKT) |> Result.ofOption "Bad WKT" |> Result.forceOk)

        let contextNode = Node.PopulationNode <| PopulationNode.ContextNode {
            Name = Text.createShort row.SiteName |> Result.forceOk
            SamplingLocation = samplingLocation
            SampleOrigin = LivingOrganism
            SampleLocationDescription = None
        }

        let! proxyTaxon = (sprintf "%s %s %s" row.Genus row.Species row.Auth) |> Text.createShort |> Result.lift BioticProxies.ContemporaneousWholeOrganism
        let! existingTaxonNode = 
            let key = makeUniqueKey(Node.PopulationNode (PopulationNode.TaxonomyNode (Taxonomy.TaxonNode.Species(Text.createShort row.Genus |> Result.forceOk, Text.createShort row.Species |> Result.forceOk, Text.createShort row.Auth |> Result.forceOk))))
            Storage.atomByKey key graph 
            |> Result.ofOption (sprintf "Cannot find taxon. Create %s %s %s first in BiodiversityCoder." row.Genus row.Species row.Auth)

        let! existingTaxon =
            match fst existingTaxonNode |> snd with
            | Node.PopulationNode p ->
                match p with
                | TaxonomyNode t -> Ok t
                | _ -> Error "Not a taxon node"
            | _ -> Error "Not a taxon node"

        // Add things now, only after checks are complete.
        let! newGraph = 
            graph
            |> fun g -> Storage.addNodes g [ 
                timelineNode
                individualDate
                contextNode ]
            |> Result.bind(fun (g, addedNodes) -> 
                Storage.addRelation sourceNode addedNodes.Head (ProposedRelation.Source Sources.SourceRelation.HasTemporalExtent) g
                |> Result.bind(fun g -> Storage.addRelation addedNodes.Head addedNodes.[1] (ProposedRelation.Exposure Exposure.ExposureRelation.ConstructedWithDate) g )
                |> Result.bind(fun g -> Storage.addRelation addedNodes.Head addedNodes.[2] (ProposedRelation.Exposure Exposure.ExposureRelation.IsLocatedAt) g )
                |> Result.bind(fun g -> 
                    Storage.addProxiedTaxon
                        proxyTaxon
                        existingTaxon
                        []
                        BioticProxies.InferenceMethodNode.Implicit
                        g
                )
                |> Result.bind(fun (g, proxiedKey) -> 
                    Storage.addRelationByKey g (addedNodes.Head |> fst |> fst) proxiedKey (ProposedRelation.Exposure Exposure.ExposureRelation.HasProxyInfo))
            )
        return! Ok newGraph
}

let data = Data.Load "input-list.csv"
printfn "Loading graph"
let graph = Storage.loadOrInitGraph "../../data/"
match graph with
| Ok g ->
    match (
        Seq.fold(fun graph row ->
            graph |> Result.bind(fun g ->
                match addStudy g row with
                | Ok g -> 
                    
                    Ok g
                | Error e -> Error e )
            ) (Ok g) data.Rows ) with
    | Ok _ -> ()
    | Error e ->
        printfn "Error: %s" e
        exit 1
| Error _ -> 
    printfn "Could not load graph"
    exit 1
