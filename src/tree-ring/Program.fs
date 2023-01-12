
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
        row.Added
        // sourceNode |> snd |> Seq.exists(fun (_,_,_,r) -> r = GraphStructure.Relation.Source Sources.SourceRelation.HasTemporalExtent)
        then 
            printfn "Skipping %s as it is marked as already entered." row.StudyId
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

        let! proxyTaxon = 
            if System.String.IsNullOrEmpty row.Subspecies
            then (sprintf "%s %s %s" row.Genus row.Species row.Auth) |> Text.createShort |> Result.lift BioticProxies.ContemporaneousWholeOrganism
            else (sprintf "%s %s ssp. %s" row.Genus row.Species row.Subspecies) |> Text.createShort |> Result.lift BioticProxies.ContemporaneousWholeOrganism
        let! existingTaxonNode = 
            let key = 
                if System.String.IsNullOrEmpty row.Subspecies
                then makeUniqueKey(Node.PopulationNode (PopulationNode.TaxonomyNode (Taxonomy.TaxonNode.Species(Text.createShort row.Genus |> Result.forceOk, Text.createShort row.Species |> Result.forceOk, Text.createShort row.Auth |> Result.forceOk))))
                else makeUniqueKey(Node.PopulationNode (PopulationNode.TaxonomyNode (Taxonomy.TaxonNode.Subspecies(Text.createShort row.Genus |> Result.forceOk, Text.createShort row.Species |> Result.forceOk, Text.createShort row.Subspecies |> Result.forceOk, Text.createShort row.Auth |> Result.forceOk))))
            Storage.atomByKey key graph 
            |> Result.ofOption (sprintf "Cannot find taxon. Create %s %s %s first in BiodiversityCoder." row.Genus row.Species row.Auth)

        let! existingTaxon =
            match fst existingTaxonNode |> snd with
            | Node.PopulationNode p ->
                match p with
                | TaxonomyNode t -> Ok t
                | _ -> Error "Not a taxon node"
            | _ -> Error "Not a taxon node"

        let! startDateNode = Storage.atomByKey (Graph.UniqueKey.FriendlyKey("calyearnode", sprintf "%iybp" <| 1950 - row.EarliestYear)) graph |> Result.ofOption ""
        let! endDateNode = Storage.atomByKey (Graph.UniqueKey.FriendlyKey("calyearnode", sprintf "%iybp" <|1950 - row.LatestYear)) graph |> Result.ofOption ""
        let! collectionDateNode = Storage.atomByKey (Graph.UniqueKey.FriendlyKey("calyearnode", sprintf "%iybp" <|1950 - row.CollectionYear)) graph |> Result.ofOption ""
        let! measureNode = Storage.atomByKey (Graph.UniqueKey.FriendlyKey("biodiversitydimensionnode", "presence")) graph |> Result.ofOption ""

        // Add things now, only after checks are complete.
        let! newGraph = 
            graph
            |> fun g -> Storage.addNodes g [ 
                timelineNode
                individualDate
                contextNode ]
            |> Result.bind(fun (g, addedNodes) -> 
                let timelineNode = addedNodes |> Seq.find(fun s -> (s |> fst |> snd).NodeType() = "IndividualTimelineNode")
                let dateNode = addedNodes |> Seq.find(fun s -> (s |> fst |> snd).NodeType() = "IndividualDateNode")
                let contextNode = addedNodes |> Seq.find(fun s -> (s |> fst |> snd).NodeType() = "ContextNode")
                
                Storage.addRelation sourceNode timelineNode (ProposedRelation.Source Sources.SourceRelation.HasTemporalExtent) g
                |> Result.bind(fun g -> Storage.addRelation timelineNode startDateNode (ProposedRelation.Exposure Exposure.ExposureRelation.ExtentEarliest) g )
                |> Result.bind(fun g -> Storage.addRelation timelineNode endDateNode (ProposedRelation.Exposure Exposure.ExposureRelation.ExtentLatest) g )
                |> Result.bind(fun g -> Storage.addRelation dateNode collectionDateNode (ProposedRelation.Exposure <| Exposure.ExposureRelation.TimeEstimate (OldDate.OldDateSimple.HistoryYearAD ((float row.CollectionYear) * 1.<OldDate.AD>))) g )
                |> Result.bind(fun g -> Storage.addRelation timelineNode dateNode (ProposedRelation.Exposure Exposure.ExposureRelation.ConstructedWithDate) g )
                |> Result.bind(fun g -> Storage.addRelation timelineNode contextNode (ProposedRelation.Exposure Exposure.ExposureRelation.IsLocatedAt) g )
                |> Result.bind(fun g -> 
                    Storage.addProxiedTaxon
                        proxyTaxon
                        existingTaxon
                        []
                        BioticProxies.InferenceMethodNode.Implicit
                        g
                )
                |> Result.bind(fun (g, proxiedKey) -> 
                    Storage.addRelationByKey g (timelineNode |> fst |> fst) proxiedKey (ProposedRelation.Exposure Exposure.ExposureRelation.HasProxyInfo)
                    |> Result.lift(fun r -> r, proxiedKey))
                |> Result.bind(fun (g, proxiedKey) -> 
                    Storage.addRelationByKey g proxiedKey (measureNode |> fst |> fst) (ProposedRelation.Population <| Population.PopulationRelation.MeasuredBy) )
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
