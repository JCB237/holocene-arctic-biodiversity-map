module ImportNeotoma

open BiodiversityCoder.Core
open FSharp.Data
open BiodiversityCoder.Core.GraphStructure
open BiodiversityCoder.Core.FieldDataTypes
open BiodiversityCoder.Core.Exposure.StudyTimeline
open System.Text.RegularExpressions

let neotomaSitesBy dbName proxy = "https://api.neotomadb.org/v2.0/data/sites?database=" + dbName + "&datasettype=" + proxy + "&loc=POLYGON%28%28-178.4601%2050.40568%2C-177.8985%2050.35086%2C-174.074%2050.75898%2C-171.774%2051.0818%2C-167.5484%2051.89807%2C-163.403%2052.91533%2C-159.9529%2053.9082%2C-157.2517%2054.88893%2C-155.0854%2055.77218%2C-152.9726%2056.34477%2C-152.0633%2056.80162%2C-151.5284%2057.3803%2C-151.1807%2058.0808%2C-152.3575%2059.91431%2C-153.133%2060.76101%2C-155.2994%2062.04629%2C-156.2087%2062.77116%2C-156.4226%2063.34984%2C-156.1552%2064.2696%2C-155.1389%2064.87269%2C-153.668%2065.33563%2C-148.907%2065.87167%2C-144.8155%2065.79248%2C-140.9911%2065.402%2C-140.9911%2064.91533%2C-139.627%2064.52548%2C-137.541%2064.33665%2C-134.8933%2064.42193%2C-132.2189%2063.78233%2C-132.3793%2063.136%2C-130.9084%2062.96609%2C-130.534%2062.21076%2C-129.517%2062.14984%2C-128.7688%2061.06558%2C-127.8328%2061.02294%2C-126.2548%2060.44426%2C-126.2281%2059.65238%2C-124.9176%2059.49401%2C-123.6606%2058.58639%2C-122.7246%2057.17319%2C-122.6978%2056.81989%2C-121.9757%2056.07066%2C-121.4141%2056.08893%2C-120.1304%2057.1001%2C-118.5792%2057.47776%2C-117.9641%2056.90517%2C-117.2687%2057.10619%2C-117.5896%2057.75797%2C-115.8512%2058.31228%2C-114.888%2058.1051%2C-113.3105%2058.50111%2C-112.6152%2058.90314%2C-111.3582%2059.02497%2C-107.4267%2058.90314%2C-105.046%2058.88487%2C-103.629%2058.75695%2C-102.7197%2058.3792%2C-101.8104%2058.28182%2C-100.0987%2057.50213%2C-99.56388%2057.66659%2C-98.9755%2057.3133%2C-96.86268%2057.3133%2C-96.32779%2056.86253%2C-94.5894%2056.74071%2C-93.4126%2056.09502%2C-92.50333%2056.1376%2C-91.51378%2055.13868%2C-90.84517%2055.16913%2C-89.8288%2054.82802%2C-89.13352%2054.86456%2C-88.7591%2054.4869%2C-87.12769%2053.79248%2C-86.45907%2053.17116%2C-85.60325%2050.64324%2C-84.58696%2050.33258%2C-83.8113%2050.2716%2C-81.4846%2050.31431%2C-80.81598%2050.57624%2C-80.2276%2050.41786%2C-79.6927%2050.6006%2C-79.131%2050.30213%2C-78.62293%2050.65543%2C-77.7671%2050.77116%2C-77.28571%2051.28284%2C-76.45663%2051.53258%2C-72.44495%2052.33664%2C-69.93097%2052.5132%2C-70.22516%2052.2026%2C-69.98446%2051.41685%2C-69.34259%2051.43512%2C-68.51351%2051.06964%2C-67.92513%2051.31939%2C-67.63094%2051.9163%2C-66.8821%2051.91025%2C-65.30417%2051.56304%2C-64.74254%2051.92243%2C-63.83322%2052.06253%2C-62.496%2051.89197%2C-62.54949%2052.37319%2C-62.09483%2052.58639%2C-59.98201%2051.87979%2C-59.76806%2052.06862%2C-58.64479%2051.99553%2C-57.84245%2052.26964%2C-56.23778%2052.19045%2C-55.08777%2052.70213%2C-52.70751%2053.12243%2C-50.5946%2053.896%2C-49.15049%2054.54781%2C-44.63067%2056.78944%2C-41.63529%2058.50111%2C-40.27132%2059.2869%2C-37.24919%2060.75492%2C-34.20031%2061.73563%2C-30.91074%2062.4605%2C-28.74444%2062.76507%2C-23.82345%2063.15492%2C-20.31992%2062.97218%2C-18.18036%2062.99655%2C-15.39893%2063.28893%2C-12.6175%2063.01482%2C-10.74538%2062.65543%2C-8.605829%2062.03411%2C-7.669771%2061.2605%2C-6.840691%2060.96812%2C-5.744167%2061.3336%2C-5.28951%2062.26558%2C-5.28951%2065.56101%2C-0.1813106%2066.5478%2C2.97454%2066.5478%2C7.01296%2066.5478%2C11.02463%2066.5478%2C15.5444%2066.55391%2C14.52816%2066.1336%2C14.50142%2065.30517%2C13.64559%2064.58639%2C13.99327%2063.93462%2C14.8491%2063.84934%2C16.13283%2064.19045%2C16.50725%2064.54984%2C16.7747%2065.402%2C18.51309%2065.63411%2C19.42241%2065.92649%2C19.77008%2066.5478%2C25.33294%2066.5478%2C31.75162%2066.5478%2C32.50047%2065.3904%2C33.19582%2064.5437%2C34.0249%2063.9468%2C35.81679%2063.29502%2C37.52843%2063.03918%2C41.51336%2062.59451%2C47.15646%2062.21685%2C52.34489%2061.95492%2C55.39376%2061.84528%2C61.19732%2061.72345%2C63.31014%2061.766%2C66.67994%2061.9001%2C69.03346%2062.16812%2C75.29167%2062.80162%2C81.09523%2063.32548%2C86.31041%2063.7092%2C90.96395%2063.85543%2C95.77796%2063.90416%2C97.67682%2063.75188%2C99.14777%2063.91634%2C103.9885%2063.64832%2C106.7164%2063.46558%2C111.5304%2063.10619%2C115.1677%2063.12446%2C116.1037%2062.26558%2C117.6014%2062.30822%2C117.441%2063.19147%2C120.1956%2063.161%2C123.0841%2063.1731%2C126.7748%2062.99045%2C129.1818%2062.8747%2C132.097%2062.57015%2C136.6168%2061.85746%2C140.1203%2060.85847%2C142.8482%2059.26863%2C144.9343%2057.48385%2C148.0902%2054.96812%2C150.3367%2053.40263%2C151.8344%2052.48284%2C153.7333%2051.61177%2C155.0437%2051.26456%2C157.0496%2050.86253%2C160.2589%2050.50314%2C161.8101%2050.34477%2C164.31654375%2050.4553346993816%2C166.6241%2050.29604%2C170.2614%2050.36913%2C172.9358%2050.30822%2C175.0219%2050.30822%2C179.1138%2050.32649%2C178.903428125%2084.865057491862%2C128.77118124999984%2084.8591233598105%2C17.256687499999988%2084.97200081563113%2C-68.32088124999994%2084.92301784186877%2C-179.21095000000005%2084.90487663547282%2C-179.6991%2050.31312%2C-178.4601%2050.40568%29%29&limit=9999&offset=0"

let proxies = [
    "pollen"
    // "plant macrofossil"
    // "diatom"
    // "insect"
    // "macroinvertebrate"
    // "vertebrate fauna"
]

let databases = [
    "Cooperative Holocene Mapping Project"
    "African Pollen Database"
    "European Pollen Database"
    "Indo-Pacific Pollen Database"
    "Latin American Pollen Database"
    "North American Pollen Database"
    "Pollen Database of Siberia and the Russian Far East"
    "Canadian Pollen Database"
    "FAUNMAP"
    "Neotoma"
    "North American Plant Macrofossil Database"
    "Academy of Natural Sciences of Drexel University"
    "NDSU Insect Database"
    "North American Non-Marine Ostracode Database Project (NANODe)"
    "MioMap"
    "Alaskan Archaeofaunas"
    "French Institute of Pondicherry Palynology and Paleoecology Database"
    "Japanese Pollen Database"
    "Neotoma Midden Database"
    "Chinese Pollen Database"
    "Holocene Perspective on Peatland Biogeochemistry"
    "ANTIGUA"
    "Neotoma Testate Amoebae Database"
    "Deep-Time Palynology Database"
    "Neotoma Biomarker Database"
    "Alpine Pollen Database"
    "Canadian Museum of Nature-Delorme Ostracoda-Surface Samples"
    "Diatom Paleolimnology Data Cooperative (DPDC)"
    "Neotoma Ostracode Database"
    "Faunal Isotope Database"
    "Neotoma Charcoal Data"
    "Pollen Monitoring Programme"
    "PaleoVertebrates of Latin America"
    "St. Croix Watershed Research Station of the Science Museum of Minnesota"
]

type NeotomaSites = JsonProvider<"neotoma-sites-by-location.json", Encoding = "UTF-8">
type NeotomaPublications = JsonProvider<"neotoma-publications.json", Encoding = "UTF-8">
type NeotomaDataset = JsonProvider<"neotoma-dataset.json", Encoding = "UTF-8">

let dataDir = "../../data/"

let import () = result {

    let! (graph: Storage.FileBasedGraph<GraphStructure.Node,GraphStructure.Relation>) = Storage.loadOrInitGraph dataDir
    
    // 1. All datasets within our Arctic region that intersect 12500ybp to 0ybp
    let arcticDatasets =
        proxies
        |> List.allPairs databases
        |> List.collect(fun (db, proxy) ->
            let response = NeotomaSites.Load(neotomaSitesBy db proxy)
            if response.Status <> "success" then failwithf "Neotoma returned an error: %s" response.Message
            response.Data |> Array.collect(fun d -> 
                d.Collectionunits
                |> Array.collect(fun c -> c.Datasets |> Array.map(fun d -> d.Datasetid))
            ) |> Array.toList
        )

    printfn "Neotoma: found %i relevant datasets" arcticDatasets.Length

    // 2. All publications associated with each dataset
    let arcticDatasetsWithPublications = 
        arcticDatasets
        |> List.collect(fun datasetId ->
            let response = NeotomaPublications.Load(sprintf "https://api.neotomadb.org/v2.0/data/datasets/%i/publications" datasetId)
            if response.Status <> "success" then failwithf "Neotoma returned an error: %s" response.Message
            if response.Data.Length = 0 then [ datasetId, None ]
            else response.Data |> Array.map(fun d -> datasetId, Some d.Publication) |> Array.toList ) 
        |> List.groupBy snd

    printfn "Neotoma: found %i relevant publications" arcticDatasetsWithPublications.Length

    // 3. Match publications to keys in our database
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
    
    // For each database entry, load the metadaa file to find out the study.
    // Find the match between our database and the ITRDB.
    let matches =
        arcticDatasetsWithPublications |> Seq.collect(fun (publication, datasets) ->

            let pubName, pubYear, isOrphaned =
                match publication with
                | Some pub ->
                    match pub.Pubtype with
                    | "Edited Book"
                    | "Authored Book" -> pub.Booktitle, pub.Year, false
                    | "Book Chapter" -> pub.Articletitle, pub.Year, false
                    | "Journal Article" -> pub.Articletitle, pub.Year, false
                    | "Legacy" -> pub.Citation, pub.Year, false
                    | _ -> pub.Articletitle, pub.Year, false
                | _ -> "neotoma dataset", 9999, true
            if pubName.Length = 0 then failwithf "Publication did not have a name?? %A" publication

            printfn "Processing Neotoma publication: %s" pubName

            if isOrphaned then
                // Is a dataset without a publication.
                datasets
                |> Seq.map(fun (x,_) ->
                    (x, (Graph.UniqueKey.FriendlyKey("sourcenode", sprintf "database_neotoma_%i" x), sprintf "neotoma_%i" x, None)))
            else
                // Search in our graph for a matching title
                sourceTitleByKey
                |> List.tryFind(fun (k,title,year) ->
                    if year.IsSome then 
                        Regex.Replace(title.ToLower(), @"[^aA-zZ0-9]", replacement = "") = Regex.Replace(pubName.ToLower(), @"[^aA-zZ0-9]", replacement = "")
                            && pubYear = year.Value
                    else Regex.Replace(title.ToLower(), @"[^aA-zZ0-9]", replacement = "") = Regex.Replace(pubName.ToLower(), @"[^aA-zZ0-9]", replacement = ""))
                |> fun s -> 
                    match s with
                    | Some (a,b,c) -> 
                        printfn "Matched in graph: %s" b
                        datasets |> Seq.map(fun (x,_) -> (x, (a, b, c)))
                    | None -> 
                        printfn "Not matched in graph."
                        datasets |> Seq.map(fun (x,_) -> (x, (Graph.UniqueKey.FriendlyKey("sourcenode", sprintf "grey_%s" pubName), pubName, Some pubYear)))
        )
        // NB Ensure each dataset should only feature in one publication. 
        // Assume that earliest source is the primary source.
        |> Seq.groupBy fst // group by dataset ID
        |> Seq.map(fun (datasetId, entries) ->
            if entries |> Seq.length = 1 then entries |> Seq.head
            else
                // Multiple publications for this dataset
                let earliestRealPub =
                    entries 
                    |> Seq.filter(fun (_,(_,_,y)) -> y.IsSome)
                    |> Seq.sortBy(fun (_,(_,_,y)) -> y.Value)
                    |> Seq.tryHead
                match earliestRealPub with
                | Some early -> 
                    printfn "Skipping extra pubs for %i: %A" datasetId (entries |> Seq.except [early])
                    early
                | None ->
                    // Only contains non-defined neotoma datasets. Just take the first
                    printfn "Skipping extra pubs for %i: %A" datasetId (entries |> Seq.tail)
                    (entries |> Seq.head))

    let matchesString = Microsoft.FSharpLu.Json.Compact.serialize(matches)
    System.IO.File.WriteAllText("neotoma-cached-results.json", matchesString)

    // Uncomment this and comment above to use cached file.
    // let matches : seq<int * (Graph.UniqueKey * string * int option)> =
    //     System.IO.File.ReadAllText("neotoma-cached-results.json")
    //     |> Microsoft.FSharpLu.Json.Compact.deserialize

    // What do we need to know?
    // Temporal extent:
    // - Start year (cal yr BP)
    // - End year (cal yr BP)
    // - Continuous / discontinuous - hiatuses?
    // - Irregular / regular?

    // What to do with matches?
    let processMatch (datasetId, (key: Graph.UniqueKey,name,_)) graph = 
        
        printfn "Processing dataset ID %i (%s)" datasetId name

        let json = NeotomaDataset.Load(sprintf "https://api.neotomadb.org/v2.0/data/downloads/%i" datasetId)
        if json.Status <> "success" then failwithf "Could not get Neotoma data for dataset %i" datasetId
        if json.Data.Length <> 1 then failwithf "Dataset did not have result of length 1"
        let data = json.Data.[0]
        
        // 1. Get site details
        let coordsLatLon =
            let m = System.Text.RegularExpressions.Regex.Matches(data.Site.Geography, "\[([-0-9.]*),([-0-9.]*)\]")
            m |> Seq.map(fun m -> float m.Groups.[2].Value, float m.Groups.[1].Value) |> Seq.toList

        let location = 
            match coordsLatLon.Length with
            | 1 -> Geography.Site((Geography.createLatitude (fst coordsLatLon.Head) |> Result.forceOk), (Geography.createLongitude (snd coordsLatLon.Head) |> Result.forceOk))
            | _ -> Geography.Area(Geography.createPolygon (coordsLatLon |> List.map(fun (lat,lon) -> (Geography.createLatitude lat |> Result.forceOk), (Geography.createLongitude lon |> Result.forceOk))) |> forceOk)

        let validSampleOrigin = 
            match data.Site.Collectionunit.JsonValue.GetProperty("defaultchronology") with
            | JsonValue.Null -> 
                printfn "Skipping %i as there is a 'null' chronology (not dated)" datasetId
                None
            | _ ->
                let earliestExtentBP, latestExtentBP =
                    match data.Site.Collectionunit.Dataset.Agerange |> Seq.tryFind(fun x -> x.Units = "Calibrated radiocarbon years BP") with
                    | Some bpDate -> 
                        // Cap ages to Holocene (limitation of our database).
                        (if float bpDate.Ageold > 11700. then 11700 else int bpDate.Ageold),
                        (if float bpDate.Ageyoung < -70. then -70 else int bpDate.Ageyoung)
                    | None -> 
                        // TODO Add in "Radiocarbon years BP"
                        match data.Site.Collectionunit.Dataset.Agerange |> Seq.tryFind(fun x -> x.Units = "Calendar years BP") with
                        | Some bpDate ->
                            // Cap ages to Holocene (limitation of our database).
                            (if float bpDate.Ageold > 11700. then 11700 else int bpDate.Ageold),
                            (if float bpDate.Ageyoung < -70. then -70 else int bpDate.Ageyoung)
                        | None ->
                            match data.Site.Collectionunit.Dataset.Agerange |> Seq.tryFind(fun x -> x.Units = "Radiocarbon years BP") with
                            | Some bpDate ->
                                (if float bpDate.Ageold > 11700. then 11700 else int bpDate.Ageold),
                                (if float bpDate.Ageyoung < -70. then -70 else int bpDate.Ageyoung)
                            | None ->
                                printfn "Warning: Skipping entry as it does not contain BP dates or cal yr BP dates: %A" data.Site.Collectionunit.Dataset.Agerange
                                22000, 21000 // Some dates out of our timeline, so that it is skipped.
                match data.Site.Collectionunit.Collunittype with
                | "Animal midden" -> Some (Population.Context.OtherOrigin("Midden" |> Text.createShort |> forceOk))
                | "Composite" -> Some (Population.Context.OtherOrigin("Composite" |> Text.createShort |> forceOk))
                | "Excavation" -> Some (Population.Context.OtherOrigin("Excavation" |> Text.createShort |> forceOk))
                | "Section"
                | "Core" ->
                    match data.Site.Collectionunit.Depositionalenvironment with
                    | s when s.ToLower().Contains("mire") -> Some Population.Context.PeatCore
                    | "Mire" -> Some Population.Context.PeatCore
                    | _ -> Some Population.Context.LakeSediment
                | "Modern"
                | "Plot"
                | "Surface float"
                | "Trap"
                | "Unknown"
                | "Isolated specimen" -> None
                | _ -> failwithf "unknown col unit type: %s" data.Site.Collectionunit.Collunittype
                |> Option.bind(fun s ->
                    // Drop entry if there is only one point in the time-series,
                    // or doesn't overlap with Holocene period.
                    let overlap = max 0 latestExtentBP <= min 11700 earliestExtentBP
                    if data.Site.Collectionunit.Dataset.Samples.Length < 2 || not overlap
                    then None
                    else Some s)
                |> Option.bind(fun r ->
                    if obj.ReferenceEquals(data.Site.Collectionunit.Defaultchronology,null)
                    then 
                        printfn "Skipping %i as there is a 'null' chronology (not dated?)" datasetId
                        None
                    else Some (r, earliestExtentBP, latestExtentBP))

        match validSampleOrigin with
        | None -> 
            printfn "Skipping entry as it is not relevant for our systematic map."
            Ok graph
        | Some (sampleOrigin, earliestExtentBP, latestExtentBP) ->

            // Make a context node
            let contextNode = Node.PopulationNode <| PopulationNode.ContextNode {
                Name = Text.createShort (data.Site.Sitename |> Seq.truncate 99 |> System.String.Concat) |> Result.forceOk
                SamplingLocation = location
                SampleOrigin = sampleOrigin
                SampleLocationDescription = None
            }

            // 2. Make timeline node (use BP dates, rather than Neotoma's Cal Yr BP dates)
            let timelineNode = ExposureNode <| Exposure.ExposureNode.TimelineNode(Continuous <| Irregular)

            // 3. Make proxied taxon nodes
            let mayOrMayNotExistTaxon = (Population.Taxonomy.Kingdom("Placeholder from NeotomaDB Import" |> Text.createShort |> Result.forceOk))
            let existingInference = Population.BioticProxies.InferenceMethodNode.Implicit

            let proxiedTaxa = 
                data.Site.Collectionunit.Dataset.Samples
                |> Seq.collect(fun s ->
                    s.Datum |> Seq.map(fun d ->
                        match d.Elementtype with
                        | "pollen"
                        | "pollen/spore"
                        | "spore" -> Some <| Population.BioticProxies.Morphotype(Population.BioticProxies.Microfossil (Population.BioticProxies.Pollen, d.Variablename |> Text.createShort |> Result.forceOk))
                        | "volume"
                        | "counted"
                        | "quantity added"
                        | "concentration" -> None
                        | "palynomorph" -> Some <| Population.BioticProxies.Morphotype(Population.BioticProxies.Microfossil (Population.BioticProxies.OtherMicrofossilGroup("Palynomorph" |> Text.createShort |> forceOk), d.Variablename |> Text.createShort |> Result.forceOk))
                        | "colony" -> Some <| Population.BioticProxies.Morphotype(Population.BioticProxies.Microfossil (Population.BioticProxies.OtherMicrofossilGroup("Colony" |> Text.createShort |> forceOk), d.Variablename |> Text.createShort |> Result.forceOk))
                        | "trichoblast" ->  Some <| Population.BioticProxies.Morphotype(Population.BioticProxies.Microfossil (Population.BioticProxies.OtherMicrofossilGroup("Trichoblast" |> Text.createShort |> forceOk), d.Variablename |> Text.createShort |> Result.forceOk))
                        | "suberized basal cells" -> Some <| Population.BioticProxies.Morphotype(Population.BioticProxies.Microfossil (Population.BioticProxies.OtherMicrofossilGroup("Suberised Basal Cells" |> Text.createShort |> forceOk), d.Variablename |> Text.createShort |> Result.forceOk))
                        | _ -> 
                            printfn "[Warning] Unknown proxy type: %s" d.Elementtype
                            None
                        )
                ) |> Seq.distinct |> Seq.choose id

            let authors =
                data.Site.Collectionunit.Dataset.Datasetpi |> Seq.map(fun pi ->
                    { LastName = pi.Familyname |> Text.createShort |> forceOk; FirstName = pi.Initials |> Text.createShort |> forceOk }
                ) |> Seq.toList

            result {

                let! graph, sourceNode = 
                    if key.AsString.StartsWith "sourcenode_database_"
                    then
                        // Is a database entry. Make the database entry here.
                        // a) get the database itself.
                        let webLocation =
                            data.Site.Collectionunit.Dataset.Doi 
                            |> Seq.tryHead 
                            |> Option.map(fun s -> System.Uri("https://doi.org/" + s))

                        let databaseCode = key.AsString.Split("_").[2]
                        let databaseNode = 
                            graph
                            |> Storage.atomByKey<Sources.SourceNode> (Graph.UniqueKey.FriendlyKey ("sourcenode", sprintf "database_%s" databaseCode))
                            |> Result.ofOption (sprintf "Could not load sources (%s)" databaseCode)
                            |> Result.forceOk
                        // b) make the database entry node.
                        let databaseEntryNode : Sources.DatabaseDatasetNode = {
                            DatabaseAbbreviation = databaseCode |> Text.createShort |> Result.forceOk
                            UniqueIdentifierInDatabase = key.AsString.Split("_").[3] |> Text.createShort |> Result.forceOk
                            Investigators = authors
                            Title = name |> Text.createShort |> Result.forceOk |> Some
                            WebLocation = webLocation
                        }
                        let node = Node.SourceNode(Sources.SourceNode.Included(Sources.Source.DatabaseEntry databaseEntryNode, Sources.CodingProgress.CompletedAll))
                        // c) add relation from database to database entry.
                        Storage.addNodes graph [ node ]
                        |> Result.bind(fun (graph,nodes) -> Storage.addRelation databaseNode nodes.Head (ProposedRelation.Source(Sources.SourceRelation.HasDataset)) graph )
                        |> Result.bind(fun graph ->
                            match Storage.atomByKey (GraphStructure.makeUniqueKey node) graph with
                            | Some a -> Ok (graph, a)
                            | None -> Error "Could not load database entry node" )

                    else if key.AsString.StartsWith "sourcenode_grey_"
                    then
                        let contact = 
                            if authors.IsEmpty then { FirstName = "Unknown" |> Text.createShort |> forceOk; LastName = "Unknown" |> Text.createShort |> forceOk } 
                            else authors.Head
                        let node = Node.SourceNode(Sources.SourceNode.Included(Sources.GreyLiterature {
                            Contact = contact
                            Title = name |> Text.create |> Result.forceOk
                            License = ``No license specified``
                        }, Sources.CodingProgress.InProgress []))
                        let key = GraphStructure.makeUniqueKey node
                        match Storage.atomByKey key graph with
                        | Some n ->
                            printfn "Grey literature node already exists. Attaching to existing node (%s)." key.AsString
                            Ok (graph, n)
                        | None ->
                            Storage.addNodes graph [ node ]
                            |> Result.bind(fun graph ->
                                match Storage.atomByKey key (fst graph) with
                                | Some a -> Ok (fst graph, a)
                                | None -> Error "Could not load database entry node" )
                    else
                        graph
                        |> Storage.atomByKey<Sources.SourceNode> key
                        |> Result.ofOption (sprintf "Could not load sources (%s)" key.AsString)
                        |> Result.lift(fun r -> graph, r)

                let! startDateNode = Storage.atomByKey (Graph.UniqueKey.FriendlyKey("calyearnode", sprintf "%iybp" <| earliestExtentBP)) graph |> Result.ofOption ""
                let! endDateNode = Storage.atomByKey (Graph.UniqueKey.FriendlyKey("calyearnode", sprintf "%iybp" <| latestExtentBP)) graph |> Result.ofOption ""
                let! measureNode = Storage.atomByKey (Graph.UniqueKey.FriendlyKey("biodiversitydimensionnode", "abundance")) graph |> Result.ofOption ""

                // Add things now, only after checks are complete.
                let! (graphWithDatasetEntry,addedTimelineNode) = 
                    graph
                    |> fun g -> Storage.addNodes g [ 
                        timelineNode
                        contextNode ]
                    |> Result.bind(fun (g, addedNodes) -> 
                        let timelineNode = addedNodes |> Seq.find(fun s -> (s |> fst |> snd).NodeType() = "IndividualTimelineNode")
                        let contextNode = addedNodes |> Seq.find(fun s -> (s |> fst |> snd).NodeType() = "ContextNode")

                        Storage.addRelation sourceNode timelineNode (ProposedRelation.Source Sources.SourceRelation.HasTemporalExtent) g
                        |> Result.bind(fun g -> Storage.addRelation timelineNode startDateNode (ProposedRelation.Exposure Exposure.ExposureRelation.ExtentEarliest) g )
                        |> Result.bind(fun g -> Storage.addRelation timelineNode endDateNode (ProposedRelation.Exposure Exposure.ExposureRelation.ExtentLatest) g )
                        |> Result.bind(fun g -> Storage.addRelation timelineNode contextNode (ProposedRelation.Exposure Exposure.ExposureRelation.IsLocatedAt) g )
                        |> Result.lift(fun r -> r, timelineNode)
                    )

                let! graphWithTaxa =
                    Seq.fold(fun state t -> 
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
                    ) (Ok graphWithDatasetEntry) proxiedTaxa

                return graphWithTaxa
            }

    let neotomaPlaceholder = (Population.Taxonomy.Kingdom("Placeholder from NeotomaDB Import" |> Text.createShort |> Result.forceOk))

    // Fold results into graph
    let r =
        Storage.addNodes graph [ Node.PopulationNode(PopulationNode.TaxonomyNode(neotomaPlaceholder)) ]
        |> Result.bind(fun graph ->
            Seq.fold(fun state i -> 
                state |> Result.bind(fun g ->
                    processMatch i g)) (Ok <| fst graph) matches
        )

    printfn "Result was %A" r

    return Ok
}
