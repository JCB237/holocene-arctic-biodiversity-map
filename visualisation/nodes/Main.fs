module BiodiversityCoder.Web.Client.Main

open Elmish
open Bolero
open Bolero.Html
open BiodiversityCoder.Core

type Page =
    | Home
    | NodeDetail of nodetype: string * nodeId: string

// What should be the default display?
// - Sources -> timelines ->
                        // -> places
                        // -> dates + calibrations

// - Geographical query.

// Don't hold whole graph in memory at once

type Model =
    {
        Page: Page
        Sources: option<Sources.SourceNode list>
        Index: 
    }

// let indexUrl = "https://raw.githubusercontent.com/AndrewIOM/holocene-arctic-biodiversity-map/main/data/atom-index.json"
let indexFile = "/Volumes/Server HD/GitHub Projects/holocene-arctic-biodiversity-map/data/atom-index.json"
let graphDirectory = "/Volumes/Server HD/GitHub Projects/holocene-arctic-biodiversity-map/data/"

let initModel =
    {
        Page = Home
        Sources = None
    }

type Message =
    | LoadSources
    | LoadedSources

// Filter by author. Search by ...

let update message model =
    match message with
    | LoadSources -> 
        let source =
            BiodiversityCoder.Core.Storage.loadOrInitGraph graphDirectory
            |> Result.forceOk
        { model with Sources = (source.Nodes<Sources.SourceNode> ()) }, Cmd.none
    | LoadedSources -> model, Cmd.none

let view model dispatch =
    cond model.Sources <| function
    | Some sources ->
        ul {
            forEach sources <| fun s ->
                li { "" }
        }
    | None -> p { "Loading sources" }

type MyApp() =
    inherit ProgramComponent<Model, Message>()

    override this.Program =
        Program.mkProgram (fun _ -> initModel, Cmd.ofMsg LoadSources) update view
