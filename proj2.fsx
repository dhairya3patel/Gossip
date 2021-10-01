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
let createFulltopology (actor:string) (actorList:list<IActorRef>) =
    let id = (actor.Split '_').[1] |> int
    let neighbours = actorList |> List.indexed |> List.filter (fun (i, _) -> i <> id-1) |> List.map snd    
    neighbours


let Gossip (mailbox: Actor<_>) =
    let mutable neighbours = []
    let mutable actorName = ""
    let mutable supervisorRef = mailbox.Self
    let rec loop () =
        actor {
            let! message = mailbox.Receive()
            let workermessage: WorkerComm = message
            match workermessage with
                | BuildNetwork(topology,supervisor,actorList) ->                
                    actorName <- mailbox.Self.Path.Name
                    if topology = "full" then 
                        neighbours <- createFulltopology actorName actorList
                    supervisorRef <- supervisor    
            return! loop()            
    }

    loop ()    


let Supervisor (mailbox: Actor<_>) =
    
    let rec loop () = actor {
        let! message = mailbox.Receive()
        let supervisormessage: SupervisorComm = message 
        match supervisormessage with
            | Begin(_) ->
                if algorithm = "gossip" then
                    let actorList = [ for i in 1 .. numNodes do yield (spawn system ("Actor_" + string (i))) Gossip]
                    actorList |> List.iter(fun node -> node <! BuildNetwork(topology,mailbox.Self,actorList))
        return! loop()
    }
    loop()

let supervisor = spawn system "supervisor" Supervisor
supervisor <! Begin("Begin")