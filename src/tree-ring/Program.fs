
open BiodiversityCoder.Core
open BiodiversityCoder.Core.GraphStructure
open FSharp.Data

// Editable variables
let studyId = "pub_alia_tgdctawddfstsae_2020"

let siteName = "Laanila"
let latDD = 69.0
let lonDD = 72.4
// The Laanila site in northern Finland is located at 68°28′–68°31′N; 27°16′–27°24′E; 220–310 m a.s.l.

let collectionYear = 2008
let latestYear = 2007
let earliestYear = 745

let genus = "Pinus" // NB As given by author.
let species = "sylvestris" // NB As given by author.
let auth = "L." // NB As given by author.

// Make sure each is only added once
let alreadyAdded = false

// Steps required.
// 1. Make a timeline in the study.

open Exposure.StudyTimeline
open FieldDataTypes
open Population.Context
open Population

let addStudy () = result {
    if alreadyAdded then return Ok ()
    else
        
        printfn "Loading graph"
        let! graph = Storage.loadOrInitGraph "../../data/"

        printfn "Finding source"

        let! sourceNode = 
            graph
            |> Storage.atomByKey<Sources.SourceNode> (Graph.UniqueKey.FriendlyKey ("sourcenode", studyId))
            |> Result.ofOption "Could not load sources"

        let timelineNode =
            ExposureNode <|
            Exposure.ExposureNode.TimelineNode(
                Continuous <| Regular (1.<FieldDataTypes.OldDate.calYearBP>, WoodAnatomicalFeatures))

        let individualDate =
            ExposureNode <|
            Exposure.ExposureNode.DateNode {
                Date = OldDate.OldDatingMethod.CollectionDate <| (float collectionYear) * 1.<FieldDataTypes.OldDate.AD>
                MaterialDated = FieldDataTypes.Text.createShort "wood increment" |> Result.forceOk
                SampleDepth = None
                Discarded = false
                MeasurementError = OldDate.MeasurementError.NoDatingErrorSpecified
            }

        let contextNode = Node.PopulationNode <| PopulationNode.ContextNode {
            Name = Text.createShort siteName |> Result.forceOk
            SamplingLocation = Geography.Site((Geography.createLatitude latDD |> Result.forceOk), (Geography.createLongitude lonDD |> Result.forceOk))
            SampleOrigin = LivingOrganism
            SampleLocationDescription = None
        }

        let! proxyTaxon = (sprintf "%s %s %s" genus species auth) |> Text.createShort |> Result.lift BioticProxies.ContemporaneousWholeOrganism
        let! existingTaxonNode = 
            let key = makeUniqueKey(Node.PopulationNode (PopulationNode.TaxonomyNode (Taxonomy.TaxonNode.Species(Text.createShort genus |> Result.forceOk, Text.createShort species |> Result.forceOk, Text.createShort auth |> Result.forceOk))))
            Storage.atomByKey key graph 
            |> Result.ofOption (sprintf "Cannot find taxon. Create %s %s %s first in BiodiversityCoder." genus species auth)

        let! existingTaxon =
            match fst existingTaxonNode |> snd with
            | Node.PopulationNode p ->
                match p with
                | TaxonomyNode t -> Ok t
                | _ -> Error "Not a taxon node"
            | _ -> Error "Not a taxon node"

        // Add timeline to source.
        // Add date into timeline.
        // Add proxy info.
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


        return Ok ()
}

match addStudy () with
| Ok _ -> printfn "Success"
| Error e -> 
    printfn "Error: %s" e
    exit 1
