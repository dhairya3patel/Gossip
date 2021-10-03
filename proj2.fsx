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
    neighbours

let createLineTopology (actor:IActorRef) (actorList:list<IActorRef>) =
    let mutable neighbours = []
    let id = (actor.Path.Name.Split '_').[1] |> int
    if(id = 1) then
        neighbours <- actorList.[id] :: neighbours
    elif(id = numNodes) then
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
                |   BuildNetwork(topology,supervisor,actorList) ->                
                        if topology = "full" then 
                            neighbours <- createFulltopology mailbox.Self actorList
                        else
                            neighbours <- createLineTopology mailbox.Self actorList
                        count <- neighbours.Length    
                        supervisorRef <- supervisor    
                |   Rumour(gossip,source) ->
                        if source = supervisorRef then
                            Console.WriteLine (mailbox.Self.Path.Name + " gossip start " + threshold.ToString() + "Count " + count.ToString())
                            firstTime <- false
                            neighbours.[(r.Next(neighbours.Length))] <! Rumour(gossip,mailbox.Self)
                            gossipStart <- true
                        else
                            if not firstTime then
                                threshold <- threshold - 1
                                Console.WriteLine (mailbox.Self.Path.Name + " Received Again " + threshold.ToString() + "Count " + count.ToString())
                                if threshold <> 0 then
                                    neighbours.[(r.Next(neighbours.Length))] <! Rumour(gossip,mailbox.Self)
                                else
                                    count <- count - 1
                                    Console.WriteLine (mailbox.Self.ToString() + "Down " + threshold.ToString() + "Count " + count.ToString())
                                    if count = 0 then
                                        supervisorRef <! Terminate("Done")
                            else 
                                Console.WriteLine (mailbox.Self.Path.Name + " First " + threshold.ToString() + "Count " + count.ToString())
                                firstTime <- false
                                neighbours.[(r.Next(neighbours.Length))] <! Rumour(gossip,mailbox.Self)
                |   _ -> Console.WriteLine "Hi" 
                        //ignore()
            return! loop()            
    }

    loop ()    


let Supervisor (mailbox: Actor<_>) =
    
    let rec loop () = actor {
        let! supervisormessage = mailbox.Receive()
        // let supervisormessage: SupervisorComm = message 
        match supervisormessage with
            |   Begin(_) ->
                    if algorithm = "gossip" then
                        let actorList = [ for i in 1 .. numNodes do yield (spawn system ("Actor_" + string (i))) Gossip]
                        actorList |> List.iter(fun node -> node <! BuildNetwork(topology,mailbox.Self,actorList))
                        let gossip = r.Next()
                        actorList.[(r.Next(1,numNodes))] <! Rumour(gossip,mailbox.Self)

            |   Terminate(_) ->
                    Console.WriteLine "Done"
                    system.WhenTerminated.Wait()

            |   _ -> ignore()        
        return! loop()
    }
    loop()

let supervisor = spawn system "supervisor" Supervisor
supervisor <! Begin("Begin")