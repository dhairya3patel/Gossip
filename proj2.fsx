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
let r = Random()
//Creating ActorSystem
let system = ActorSystem.Create("Project2")

type Comm =
    | Begin of string    
    | BuildNetwork of string * IActorRef * list<IActorRef> 
    | Rumour of int * IActorRef //*list<IActorRef>
    | Terminate of string// * IActorRef

//Full Topology 
let createFulltopology (actor:IActorRef) (actorList:list<IActorRef>) =
    let id = (actor.Path.Name.Split '_').[1] |> int
    let mutable neighbours = []
    let mutable temp = 0
    for i in actorList do
        temp <- (i.Path.Name.Split '_').[1] |> int
        if id <> temp then
            neighbours <- i :: neighbours

//Line topology
let createLineTopology (actor:string) (actorList:list<IActorRef>) =
    let mutable neighbours = []
    let id = (actor.Split '_').[1] |> int
    if(id = 0) then
        neighbours <- actorList.[id+1] :: neighbours
    elif(id = numNodes - 1) then
        neighbours <- actorList.[numNodes - 2] :: neighbours
    else
        neighbours <- actorList.[id-1] :: neighbours
        neighbours <- actorList.[id+1] :: neighbours
    Console.WriteLine(neighbours)
    neighbours


let Gossip (mailbox: Actor<_>) =
    let mutable neighbours = []
    let mutable count = 0
    let mutable threshold = 10
    let mutable supervisorRef = mailbox.Self
    let mutable gossipStart = false
    let mutable firstTime = true
    let rec loop () =
        actor {
            let! workermessage = mailbox.Receive()
            // let workermessage: WorkerComm = message
            match workermessage with
                | BuildNetwork(topology,supervisor,actorList) ->                
                    // actorName <- mailbox.Self.Path.Name
                    if topology = "full" then 
                        neighbours <- createFulltopology actorName actorList
                    else 
                        // topology = "full" then 
                        neighbours <- createLineTopology actorName actorList
                    supervisorRef <- supervisor    
            return! loop()            
    }

    loop ()    


let Supervisor (mailbox: Actor<_>) =
    
    let rec loop () = actor {
        let! supervisormessage = mailbox.Receive()
        // let supervisormessage: SupervisorComm = message 
        match supervisormessage with
            | Begin(_) ->
                if algorithm = "gossip" then
                    let actorList = [ for i in 0 .. numNodes - 1 do yield (spawn system ("Actor_" + string (i))) Gossip]
                    actorList |> List.iter(fun node -> node <! BuildNetwork(topology,mailbox.Self,actorList))
        return! loop()
    }
    loop()

let supervisor = spawn system "supervisor" Supervisor
supervisor <! Begin("Begin")