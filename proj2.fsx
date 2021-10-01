#r "nuget: Akka.FSharp"

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Security.Cryptography
open System.Text

let mutable numNodes = fsi.CommandLineArgs.[1] |> int
let topology = fsi.CommandLineArgs.[2]
let algorithm = fsi.CommandLineArgs.[3]

//Creating ActorSystem
let system = ActorSystem.Create("Project2")

type SupervisorComm = 
    | Begin of string
   

type WorkerComm = 
    | BuildNetwork of string * IActorRef * list<IActorRef> 
    | Rumour of string 
    | Terminate of string * IActorRef

// type CommunicationMessages =
//     | WorkerMessage of int * IActorRef
//     | EndMessage of IActorRef * string
//     | SupervisorMessage of int
//     | CoinMessage of string


//Full Topology 
let createFulltopology (actor:String) (actorList:list<IActorRef>) =

    let id = (actor.Split '-').[1] |> int
    let mutable neighbourList =[]

    for i in 1..actorList.Length do 
        if i <> id then
            neighbourList <- actorList.[i] :: neighbourList
    neighbourList

