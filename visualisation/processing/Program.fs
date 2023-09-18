
open BiodiversityCoder.Core
open BiodiversityCoder.Core.GraphStructure
open FSharp.Data

type IndividualMeasureCsv = CsvProvider<
    Sample = "source_id, source_title, source_year, source_authors, source_type, site_name, LatDD, LonDD, inferred_from, inferred_using, biodiversity_measure, inferred_as, sample_origin, earliest_extent, latest_extent, proxy_category",
    Schema = "source_id (string option), source_title (string option), source_year (int option), source_authors (string option), source_type (string), site_name (string), LatDD (float), LonDD (float), inferred_from (string option), inferred_using (string option), biodiversity_measure (string option), inferred_as (string), sample_origin (string), earliest_extent (int option), latest_extent (int option), proxy_category (string)", HasHeaders = true>

type IndividualSiteCsv = CsvProvider<
    Sample = "source_id, source_title, source_year, source_authors, source_type, site_name, LatDD, LonDD, inferred_from, inferred_using, biodiversity_measure, inferred_as, sample_origin, earliest_extent, latest_extent, proxy_category, variability_temp, variability_precip, max_temp, max_precip, min_temp, min_precip",
    Schema = "source_id (string option), source_title (string option), source_year (int option), source_authors (string option), source_type (string), site_name (string), LatDD (float), LonDD (float), inferred_from (string option), inferred_using (string option), biodiversity_measure (string option), inferred_as (string), sample_origin (string), earliest_extent (int option), latest_extent (int option), proxy_category (string), variability_temp (float), variability_precip (float), max_temp (float), max_precip (float), min_temp (float), min_precip (float)", HasHeaders = true>

type CryosphereData = CsvProvider<"../../src/cryo-db/cryo_db.csv">

let unwrap (f:float<_>) = float f

let origin = function
    | Population.Context.LakeSediment _ -> "Lake sediment core"
    | Population.Context.PeatCore _ -> "Peat core"
    | Population.Context.Excavation _ -> "Excavation"
    | Population.Context.Subfossil -> "Subfossil material"
    | Population.Context.LivingOrganism -> "Living organism"
    | Population.Context.OtherOrigin (a,_) -> "Other"

let proxyToGroup = function
    | Population.BioticProxies.ContemporaneousWholeOrganism _ -> "Contemporaneous Whole Organism"
    | Population.BioticProxies.AncientDNA _ -> "Ancient DNA"
    | Population.BioticProxies.Morphotype m ->
        match m with
        | Population.BioticProxies.Macrofossil _-> "Marcofossil"
        | Population.BioticProxies.Megafossil _ -> "Megafossil"
        | Population.BioticProxies.Microfossil (g,_) -> g.ToString()

let extractLatLon sampleLocation =
    match sampleLocation with
    | FieldDataTypes.Geography.Site (lat: FieldDataTypes.Geography.Latitude,lon) -> 
        unwrap lat.Value, unwrap lon.Value
    | FieldDataTypes.Geography.SiteDMS coord -> 
        let m = System.Text.RegularExpressions.Regex.Match(coord.Value, "^([0-9]{1,2})[:|°]([0-9]{1,2})[:|'|′]?([0-9]{1,2}(?:\.[0-9]+){0,1})?[\"|″]([N|S]),([0-9]{1,3})[:|°]([0-9]{1,2})[:|'|′]?([0-9]{1,2}(?:\.[0-9]+){0,1})?[\"|″]([E|W])$")
        let deg, min, sec, dir = m.Groups.[1].Value, m.Groups.[2].Value, m.Groups.[3].Value, m.Groups.[4].Value
        let degLon, minLon, secLon, dirLon = m.Groups.[5].Value, m.Groups.[6].Value, m.Groups.[7].Value, m.Groups.[8].Value
        let lat = (float deg + float min / 60. + float sec / (60. * 60.)) * (if dir = "S" then -1. else 1.)
        let lon = (float degLon + float minLon / 60. + float secLon / (60. * 60.)) * (if dirLon = "W" then -1. else 1.)
        lat, lon
    | FieldDataTypes.Geography.Area poly -> 
        let lat = poly.Value |> List.map fst |> List.averageBy(fun v -> v.Value |> unwrap)
        let lon = poly.Value |> List.map snd |> List.averageBy(fun v -> v.Value |> unwrap)
        lat, lon
    | _ -> nan, nan


let run () = 
    result {

        // NB FSharp.Data uses relative path from net6.0 folder
        printfn "Loading cryospheric dataset"
        let cryoDataset = CryosphereData.Load "../../../../../src/cryo-db/cryo_db.csv"

        printfn "Loading graph"
        let! graph = Storage.loadOrInitGraph "../../data/"

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
                            | Sources.CodingProgress.InProgress _
                            | Sources.CodingProgress.Stalled _
                            | Sources.CodingProgress.CompletedAll -> Some (s |> fst, i, s |> snd)
                            | _ -> None
                        | _ -> None
                    | _ -> 
                        printfn "Source is %A" (s |> fst |> fst)                        
                        failwith "Not a source node"
                ) |> List.choose id
            )

        // Make a row for each temporal extent.
        let! temporalExtents =
            includedAndCompleteSources
            |> List.map(fun (node, source, adj) ->
                
                let temporalExtentIds =
                    adj
                    |> List.choose(fun (_,sinkId,_,conn) ->
                        match conn with
                        | GraphStructure.Relation.Source r ->
                            match r with
                            | Sources.SourceRelation.HasTemporalExtent -> Some sinkId
                            | _ -> None
                        | _ -> None
                    )

                let sourceId, sourceName, year, authors, sourceType =
                    match source with
                    | Sources.Bibliographic meta ->
                        (Some (fst node).AsString), (meta.Title |> Option.map(fun v -> v.Value)), meta.Year, meta.Author |> Option.map (fun o -> o.Value), "Bibliographic"
                    | Sources.GreyLiterature g -> None, Some g.Title.Value, None, Some <| sprintf "%s., %s" g.Contact.LastName.Value g.Contact.FirstName.Value, "Grey literature"
                    | Sources.DatabaseEntry d -> Some <| sprintf "%s - %s" d.DatabaseAbbreviation.Value d.UniqueIdentifierInDatabase.Value, d.Title |> Option.map(fun v -> v.Value), None, (d.Investigators |> List.map(fun p -> sprintf "%s., %s" p.LastName.Value p.FirstName.Value) |> String.concat ";" |> Some), "Database entry"
                    | Sources.DarkData d -> None, Some d.Details.Value, None, Some <| sprintf "%s., %s" d.Contact.LastName.Value d.Contact.FirstName.Value, "Dark data"
                    | _ -> None, None, None, None, "None"
                temporalExtentIds
                |> Storage.loadAtoms graph.Directory (typeof<Exposure.StudyTimeline.IndividualTimelineNode>.Name)
                |> Result.lift(fun extents -> extents |> List.map(fun e -> sourceId, sourceName, year, authors, sourceType, e))
            ) |> Result.ofList
        
        // Attach spatial context information to each row
        let withSpatialContextAndOutcomes =
            temporalExtents
            |> List.collect id
            |> List.map(fun (sId, sourceName, year, authors, sourceType, extent) ->

                // Get temporal extent values. Based on links that are always present.
                let earliestExtent = 
                    extent |> snd |> List.tryFind(fun (_,_,_,r) ->
                        match r with
                        | GraphStructure.Relation.Exposure r ->
                            match r with
                            | Exposure.ExposureRelation.ExtentEarliest -> true
                            | _ -> false
                        | _ -> false)
                    |> Option.map(fun (_,sink,_,_) ->
                        System.Text.RegularExpressions.Regex.Replace(sink.AsString, "[aA-zZ]", "") |> int)
                let latestExtent = 
                    extent |> snd |> List.tryFind(fun (_,_,_,r) ->
                        match r with
                        | GraphStructure.Relation.Exposure r ->
                            match r with
                            | Exposure.ExposureRelation.ExtentLatest -> true
                            | _ -> false
                        | _ -> false)
                    |> Option.map(fun (_,sink,_,_) ->
                        System.Text.RegularExpressions.Regex.Replace(sink.AsString, "[aA-zZ]", "") |> int)

                // Load single context node
                let spatialRelation = 
                    extent |> snd |> List.tryFind(fun (_,_,_,r) ->
                        match r with
                        | GraphStructure.Relation.Exposure r ->
                            match r with
                            | Exposure.ExposureRelation.IsLocatedAt -> true
                            | _ -> false
                        | _ -> false) 
                    |> Result.ofOption "Could not find spatial relation"

                // Load many biodiversity outcomes
                let biodiversityOutcomes =
                    extent |> snd |> List.choose(fun (_,sinkId,_,r) ->
                        match r with
                        | GraphStructure.Relation.Exposure r ->
                            match r with
                            | Exposure.ExposureRelation.HasProxyInfo -> Some sinkId
                            | _ -> None
                        | _ -> None)
                    |> Storage.loadAtoms graph.Directory (typeof<Population.ProxiedTaxon.ProxiedTaxonHyperEdge>.Name)
                    |> Result.bind(fun l ->
                        
                        l |> List.where(fun l2 ->
                            match l2 |> fst |> snd with
                            | GraphStructure.Node.PopulationNode p ->
                                match p with
                                | PopulationNode.ProxiedTaxonNode -> true
                                | _ -> false
                            | _ -> false)
                        |> List.map(fun hyperedge ->
                            // Load the proxy, inference method, outcome, and taxon nodes.
                            
                            let popnRelations = 
                                hyperedge |> snd
                                |> List.choose(fun (_,sinkId,_,n) ->
                                    match n with
                                    | GraphStructure.Relation.Population r -> Some (sinkId, r)
                                    | _ -> None )
                            
                            let inferredFrom =
                                popnRelations |> List.tryFind(fun r ->
                                    match snd r with
                                    | Population.PopulationRelation.InferredFrom -> true
                                    | _ -> false) |> Option.map (fst >> Storage.loadAtom graph.Directory (typeof<Population.BioticProxies.BioticProxyNode>.Name))
                                    |> Result.ofOption "Could not load inferred from node" |> Result.bind id
                            
                            let proxyGroups =
                                popnRelations |> List.where(fun r ->
                                    match snd r with
                                    | Population.PopulationRelation.InferredFrom -> true
                                    | _ -> false) 
                                |> List.map (fst >> Storage.loadAtom graph.Directory (typeof<Population.BioticProxies.BioticProxyNode>.Name))
                                |> Result.ofList
                                |> Result.bind( fun p ->
                                    p |> List.map(fun p ->
                                        match p |> fst |> snd with
                                        | GraphStructure.PopulationNode p ->
                                            match p with
                                            | PopulationNode.BioticProxyNode p -> Ok <| proxyToGroup p
                                            | _ -> Error "not correct node"
                                        | _ -> Error "not correct node" )
                                    |> Result.ofList )

                            let inferredUsing =
                                popnRelations |> List.tryFind(fun r ->
                                    match snd r with
                                    | Population.PopulationRelation.InferredUsing -> true
                                    | _ -> false) |> Option.map (fst >> Storage.loadAtom graph.Directory (typeof<Population.BioticProxies.InferenceMethodNode>.Name))
                                    |> Result.ofOption "Could not load inferred using node" |> Result.bind id
                            
                            let outcomeMeasure =
                                popnRelations |> List.tryFind(fun r ->
                                    match snd r with
                                    | Population.PopulationRelation.MeasuredBy -> true
                                    | _ -> false) |> Option.map (fst >> Storage.loadAtom graph.Directory (typeof<Outcomes.Biodiversity.BiodiversityDimensionNode>.Name))
                                    |> Result.ofOption "Could not load outcome node" |> Result.bind id

                            let inferredAs =
                                popnRelations |> List.where(fun r ->
                                    match snd r with
                                    | Population.PopulationRelation.InferredAs -> true
                                    | _ -> false) |> List.map (fst >> Storage.loadAtom graph.Directory (typeof<Population.Taxonomy.TaxonNode>.Name))
                                    |> Result.ofList

                            let from = inferredFrom |> Result.lift(fun (n: Graph.Atom<GraphStructure.Node,obj>) -> (n |> fst |> snd).DisplayName()) |> Result.toOption
                            let using = inferredUsing |> Result.lift(fun (n: Graph.Atom<GraphStructure.Node,obj>) -> (n |> fst |> snd).DisplayName()) |> Result.toOption
                            let by = outcomeMeasure |> Result.lift(fun (n: Graph.Atom<GraphStructure.Node,obj>) -> (n |> fst |> snd).DisplayName()) |> Result.toOption
                            let asT = inferredAs |> Result.lift(fun n -> n |> List.map(fun (n: Graph.Atom<GraphStructure.Node,obj>) -> (n |> fst |> snd).DisplayName()))
                            asT |> Result.lift(fun taxa -> (from, using, by, (taxa |> String.concat ";"), proxyGroups))
                        ) |> Result.ofList
                    )

                // Load one to many proxy groups
                let proxyGroups =
                    extent |> snd |> List.choose(fun (_,sinkId,_,r) ->
                        match r with
                        | GraphStructure.Relation.Exposure r ->
                            match r with
                            | Exposure.ExposureRelation.HasProxyCategory -> Some sinkId
                            | _ -> None
                        | _ -> None)
                    |> Storage.loadAtoms graph.Directory (typeof<Population.BioticProxies.BioticProxyCategoryNode>.Name)
                    |> Result.lift(fun l ->
                        l |> List.where(fun l2 ->
                            match l2 |> fst |> snd with
                            | GraphStructure.Node.PopulationNode p ->
                                match p with
                                | PopulationNode.BioticProxyCategoryNode _ -> true
                                | _ -> false
                            | _ -> false)
                        |> List.map(fun (category: Graph.Atom<Node,obj>) -> (category |> fst |> snd).DisplayName()))

                let allProxyGroups =
                    biodiversityOutcomes
                    |> Result.bind(fun l -> l |> List.map(fun (_,_,_,_,g) -> g) |> Result.ofList)
                    |> Result.lift(fun l -> match proxyGroups with | Ok p -> List.concat [l |> List.concat; p] | Error _ -> l |> List.concat)
                    |> Result.lift(fun l -> l |> List.distinct |> String.concat ";")

                // Add proxy groups from biotic 

                let individualRecords =
                    spatialRelation
                    |> Result.bind(fun (_,sinkId,_,_) -> Storage.loadAtom graph.Directory (typeof<Population.Context.ContextNode>.Name) sinkId)
                    |> Result.bind(fun contextNode ->
                        match contextNode |> fst |> snd with
                        | GraphStructure.Node.PopulationNode p ->
                            match p with
                            | PopulationNode.ContextNode c -> Ok c
                            | _ -> Error "not a context node"
                        | _ -> Error "not a context node" )
                    |> Result.bind(fun context ->
                        let lat, lon = extractLatLon context.SamplingLocation
                        // Multiply out for each biodiversity outcome record.
                        biodiversityOutcomes
                        |> Result.lift(fun ls ->
                            ls |> List.map(fun (from, using, by, taxon, groups) ->
                                sId, sourceName, year, authors, sourceType, context.Name.Value, lat, lon, from, using, by, taxon, origin context.SampleOrigin, earliestExtent, latestExtent, groups |> forceOk |> String.concat ";"))
                    )

                let includedSites =
                    spatialRelation
                    |> Result.bind(fun (_,sinkId,_,_) -> Storage.loadAtom graph.Directory (typeof<Population.Context.ContextNode>.Name) sinkId)
                    |> Result.bind(fun contextNode ->
                        match contextNode |> fst |> snd with
                        | GraphStructure.Node.PopulationNode p ->
                            match p with
                            | PopulationNode.ContextNode c -> Ok c
                            | _ -> Error "not a context node"
                        | _ -> Error "not a context node" )
                    |> Result.bind(fun context ->
                        let lat, lon = extractLatLon context.SamplingLocation
                        
                        // Cryosphere database - link here
                        let varTemp, varPrecip, maxTemp, maxPrecip, minTemp, minPrecip =
                            if earliestExtent.IsSome && latestExtent.IsSome then
                                let cryoData = 
                                    cryoDataset.Rows |> Seq.filter(fun row ->
                                        float row.LatDD = lat && float row.LonDD = lon )
                                    |> Seq.filter(fun c -> 
                                        float c.Year_kBP * 1000. <= float earliestExtent.Value && float c.Year_kBP * 1000. >= float latestExtent.Value)
                                if cryoData |> Seq.isEmpty then nan, nan, nan, nan, nan, nan
                                else
                                    let temps = cryoData |> Seq.map(fun c -> float c.CHELSA_TraCE21k_temp)
                                    let precips = cryoData |> Seq.map(fun c -> float c.CHELSA_TraCE21k_precip)
                                    // Some valid cryosphere data exists for the time-series extent
                                    abs ((temps |> Seq.max) - (temps |> Seq.min)),
                                    abs ((precips |> Seq.max) - (precips |> Seq.min)),
                                    temps |> Seq.max, precips |> Seq.max, temps |> Seq.min, precips |> Seq.min
                            else nan, nan, nan, nan, nan, nan
                        
                        // Multiply out for each biodiversity outcome record.
                        biodiversityOutcomes
                        |> Result.lift(fun ls ->
                            let from = ls |> List.choose(fun (from, using, by, taxa, groups) -> from) |> List.distinct |> String.concat ";"
                            let using = ls |> List.choose(fun (from, using, by, taxa, groups) -> using) |> List.distinct |> String.concat ";"
                            let by = ls |> List.choose(fun (from, using, by, taxa, groups) -> by) |> List.distinct |> String.concat ";"
                            let taxa = ls |> List.map(fun (from, using, by, taxa, groups) -> taxa) |> List.distinct |> String.concat ";"
                            [ sId, sourceName, year, authors, sourceType, context.Name.Value, lat, lon, Some from, Some using, Some by, taxa, origin context.SampleOrigin , earliestExtent, latestExtent, allProxyGroups |> forceOk, varTemp, varPrecip, maxTemp, maxPrecip, minTemp, minPrecip ]
                            )
                        )

                (match individualRecords with | Ok i -> i | Error _ -> []),
                (match includedSites with | Ok i -> i | Error e -> printfn "Error was %s" e; [])
            )

        // 1. Save individual records
        let allRecords = 
            withSpatialContextAndOutcomes 
            |> List.map fst
            |> List.concat
        printfn "Generated %i row(s)" allRecords.Length
        let csv = new IndividualMeasureCsv (allRecords |> List.map IndividualMeasureCsv.Row)
        let csvStr = csv.SaveToString('\t')
        System.IO.File.WriteAllText("../thalloo-static-site/map-data/ahbdb.txt", csvStr)

        // 1. Save site-based records
        let siteRecords = 
            withSpatialContextAndOutcomes 
            |> List.map snd
            |> List.concat
        printfn "Generated %i sites(s)" siteRecords.Length
        let csv = new IndividualSiteCsv (siteRecords |> List.map IndividualSiteCsv.Row)
        let csvStr = csv.SaveToString('\t')
        System.IO.File.WriteAllText("../thalloo-static-site/map-data/ahbdb_sites.txt", csvStr)

        return Ok
    }

match run () with
| Ok _ -> printfn "Success"
| Error e -> 
    printfn "Error: %s" e
    exit 1
