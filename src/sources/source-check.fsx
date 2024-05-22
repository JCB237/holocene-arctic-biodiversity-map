#r "nuget: Cyjs.NET,0.0.4"
#r "nuget: Elmish,3.1.0"
#r "nuget: FSharp.Data,5.0.2"
#r "nuget: Bolero,0.18.16"
#r "nuget: Newtonsoft.Json,13.0.3"
#r "nuget: Microsoft.FSharpLu.Json,0.11.7"
#r "../../dist/BiodiversityCoder.Core.dll"

open BiodiversityCoder.Core
open BiodiversityCoder.Core.Storage
open Bolero

let directory = "data/"
let graph: Storage.FileBasedGraph<GraphStructure.Node,GraphStructure.Relation> =
    Storage.loadOrInitGraph directory
    |> Result.forceOk

let sources: Graph.Atom<GraphStructure.Node,GraphStructure.Relation> list =
    graph.Nodes<Sources.SourceNode> ()
    |> Result.ofOption "No source nodes in database"
    |> Result.lift (Map.toList >> List.map fst)
    |> Result.bind (Storage.loadAtoms graph.Directory "SourceNode")
    |> Result.forceOk

let loadSourcesPointingToOtherSources () : list<Graph.Atom<GraphStructure.Node,GraphStructure.Relation>> =
    Storage.loadIndex directory
    |> Result.forceOk
    |> List.filter(fun i -> i.NodeTypeName = "SourceNode")
    |> List.map(fun i -> i.NodeId)
    |> Storage.loadAtoms directory "SourceNode"
    |> Result.forceOk
    |> List.filter(fun (_,r) -> r |> Seq.exists(fun (a,b,_,_) -> b.AsString.Contains "sourcenode") )


let processSource (source:Graph.Atom<GraphStructure.Node,GraphStructure.Relation>) =
    printfn "Source is %s" (source |> fst |> fst).AsString
    result {
        let! sourceType =
            match source |> fst |> snd with
            | GraphStructure.Node.SourceNode s ->
                match s with
                | Sources.SourceNode.Excluded (s,_,_)
                | Sources.SourceNode.Included (s,_)
                | Sources.SourceNode.Unscreened s -> Ok s
            | _ -> Error "Not a source node"

        let textOrEmpty = function
            | Some (s:FieldDataTypes.Text.Text) -> s.Value
            | None -> ""
        let shortTextOrEmpty = function
            | Some (s:FieldDataTypes.Text.ShortText) -> s.Value
            | None -> ""

        let toStringOrEmpty = function
            | Some s -> s.ToString()
            | None -> ""

        let crossRefSearchTerm =
            match sourceType with
            | Sources.Source.GreyLiterature g ->
                Some <| sprintf "%s, %s. %s" g.Contact.LastName.Value g.Contact.FirstName.Value g.Title.Value
            | Sources.Source.Bibliographic b ->
                Some <| sprintf "%s (%s). %s. %s" (textOrEmpty b.Author) (toStringOrEmpty b.Year) (textOrEmpty b.Title) (shortTextOrEmpty b.Journal)
            | _ -> None

        match crossRefSearchTerm with
        | Some term ->
            // Search in crossref for a match.
            let! isMatch = Sources.CrossRef.tryMatch (System.Web.HttpUtility.UrlEncode term)

            match isMatch with
            | Some m ->
                printfn "Found source: %A" m
                printfn "The old source was: %A" sourceType
                printfn "Should the source be replaced? [y/n]"
                let answer = System.Console.ReadLine()
                match answer with
                | "y" ->
                    // - Generate new node data and key
                    let nodeToSave =
                        match source |> fst |> snd with
                        | GraphStructure.Node.SourceNode s ->
                            match s with
                            | Sources.SourceNode.Excluded (_,a,b) -> Sources.SourceNode.Excluded (m,a,b)
                            | Sources.SourceNode.Included (_,a) -> Sources.SourceNode.Included (m,a)
                            | Sources.SourceNode.Unscreened _ -> Sources.SourceNode.Unscreened m
                        | _ -> failwith "Impossible"
                        |> GraphStructure.Node.SourceNode
                    
                    let key = GraphStructure.makeUniqueKey nodeToSave
                    
                    // TODO Make check less fatal:
                    if sources |> Seq.exists(fun s -> (s |> fst |> fst) = key)
                    then return! Error "Node already exists"
                    else
                        // - Update relations to use the new key as source
                        let rels =
                            source |> snd
                            |> List.map(fun (source, sink, weight, connData) -> key, sink, weight, connData )

                        // - Construct the new entity to save to file
                        let newAtom : Graph.Atom<GraphStructure.Node,GraphStructure.Relation> = (key, nodeToSave), rels

                        // - Replace atom in file with the new contents, then move to new key location.
                        let oldFileName = (sprintf "atom-%s.json" ((fst source |> fst).AsString.ToLower()))
                        let newFileName = (sprintf "atom-%s.json" (key.AsString.ToLower()))

                        System.IO.File.AppendAllLines("changed-keys.txt", [ sprintf "%s\t%s\t%A\t%A\n" oldFileName newFileName source newAtom ])

                        printfn "Replacing contents of atom file (%s) and moving -> %s" oldFileName newFileName
                        Microsoft.FSharpLu.Json.Compact.serializeToFile (System.IO.Path.Combine(directory, oldFileName)) newAtom
                        System.IO.File.Move(System.IO.Path.Combine(directory, oldFileName), System.IO.Path.Combine(directory, newFileName))

                        // - Replace index entry
                        printfn "Replacing old index entry with a new one"
                        let index = Storage.loadIndex directory |> Result.forceOk
                        let oldIndexEntry = index |> List.find(fun i -> i.NodeId = (source |> fst |> fst))
                        let newIndex = 
                            index |> List.except [ oldIndexEntry ] |> List.append([
                                {
                                    NodeId = key
                                    PrettyName = (newAtom |> fst |> snd).DisplayName ()
                                    NodeTypeName = "SourceNode"
                                }])
                        Storage.replaceIndex directory newIndex |> ignore

                        // - Replace sink on outward relations to this node
                        printfn "Scanning for incoming links to the changed node"
                        let sinkSources = loadSourcesPointingToOtherSources ()
                        sinkSources
                        |> List.iter(fun ((k,n),r) ->
                            if r |> Seq.exists(fun (_,sink,_,_) -> sink = ((fst source |> fst)))
                            then
                                printfn "Found an incoming connection from %s" k.AsString
                                let rels =
                                    r |> Seq.map(fun (so,sink,w,data) ->
                                        if sink = ((fst source |> fst))
                                        then (so, key, w, data)
                                        else (so, sink, w, data)
                                        )
                                let atom = (k,n),rels
                                Storage.makeCacheFile directory (sprintf "atom-%s.json" (k.AsString.ToLower())) atom |> ignore
                            else ()
                            )
                        
                        printfn "Finished source."
                        return! Ok ()
                | _ ->
                    printfn "Not processing source, as answer was not y"
                    return! Ok ()
            | None -> 
                printfn "No match found. Skipping."
                return! Ok ()
            return Error isMatch
        | None ->
            printfn "Source does not require checking."
            return Ok ()

    }

let grey =
    sources
    |> List.filter(fun s ->
        match s |> fst |> snd with
        | GraphStructure.Node.SourceNode s ->
            match s with
            | Sources.SourceNode.Included (s,_)
            | Sources.SourceNode.Excluded (s,_,_)
            | Sources.SourceNode.Unscreened s ->
                match s with
                | Sources.Source.GreyLiterature _ -> true
                | _ -> false
        | _ -> false )

for g in List.rev grey do 
    processSource g |> ignore


let biblio =
    sources
    |> List.filter(fun s ->
        match s |> fst |> snd with
        | GraphStructure.Node.SourceNode s ->
            match s with
            | Sources.SourceNode.Included (s,_)
            | Sources.SourceNode.Excluded (s,_,_)
            | Sources.SourceNode.Unscreened s ->
                match s with
                | Sources.Source.Bibliographic _ -> true
                | _ -> false
        | _ -> false )

for g in biblio do 
    processSource g |> ignore

// When year and volume match, and titles are close, safe to assume its a definite match.