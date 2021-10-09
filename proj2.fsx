#r "nuget: Akka.FSharp"

open System
open Akka.Actor
open Akka.FSharp


let mutable numNodes = fsi.CommandLineArgs.[1] |> int
let topology = fsi.CommandLineArgs.[2]
let algorithm = fsi.CommandLineArgs.[3]
let r = Random()

//Creating ActorSystem
let system = ActorSystem.Create("Project2")
let mutable count = 0
let timer = System.Diagnostics.Stopwatch()


type Comm =
    | Begin of string    
    | BuildNetwork of string * IActorRef * list<IActorRef> 
    | Rumour of int * IActorRef *list<IActorRef>* IActorRef
    | Terminate of string
    | Acknowledge of IActorRef * list<IActorRef> * int

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
        neighbours <- actorList.[id] :: neighbours
        neighbours <- actorList.[id-2] :: neighbours
    // Console.WriteLine(neighbours)
    neighbours


let Gossip (mailbox: Actor<_>) =
    let mutable neighbours = []
    let mutable threshold = 50
    let mutable supervisorRef = mailbox.Self
    let mutable flag = false
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
                        supervisorRef <- supervisor    
                |   Rumour(gossip,source,actorList,supervisorRef) -> 

                        if not firstTime then
                            if source <> mailbox.Self then
                                threshold <- threshold - 1
                            Console.WriteLine (mailbox.Self.Path.Name + " Received Again " + threshold.ToString()) //+ "Count " + count.ToString())
                            if threshold > 0 then
                                neighbours.[r.Next(neighbours.Length)] <! Rumour(gossip,mailbox.Self,actorList,supervisorRef)//,received)

                            else if threshold = 0 then
                                supervisorRef <! Terminate("Down")
                                Console.WriteLine (mailbox.Self.ToString() + "Down " + threshold.ToString())// + "Count " + count.ToString())

                        else 
                            Console.WriteLine (mailbox.Self.Path.Name + " First " + threshold.ToString())// + "Count " + count.ToString())
                            firstTime <- false
                            neighbours.[r.Next(neighbours.Length)] <! Rumour(gossip,mailbox.Self,actorList,supervisorRef)//,received + 1)
                            system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromSeconds(0.5),mailbox.Self,Rumour(gossip,mailbox.Self,actorList,supervisorRef))
                |   _ -> Console.WriteLine workermessage 
                        //ignore()
            return! loop()            
    }

    loop ()    


let Supervisor (mailbox: Actor<_>) =
    
    let rec loop () = actor {
        let! supervisormessage = mailbox.Receive()
        let mutable gossip = 0
        match supervisormessage with
            |   Begin(_) ->
                    if algorithm = "gossip" then
                        let actorList = [ for i in 1 .. numNodes do yield (spawn system ("Actor_" + string (i))) Gossip]
                        //Console.WriteLine actorList
                        actorList |> List.iter(fun node -> node <! BuildNetwork(topology,mailbox.Self,actorList))
                        gossip <- r.Next()
                        actorList.[r.Next(1,numNodes)] <! Rumour(gossip,mailbox.Self,actorList,mailbox.Self)//,0)
                        timer.Start()

            |   Terminate(termMsg) -> 
                    //Console.WriteLine termMsg
                    if termMsg = "Down" then
                        count <- count + 1
                        //Console.WriteLine count
                        if count = numNodes then
                            mailbox.Self <! Terminate("Done")

                    if termMsg = "Done" then
                        mailbox.Context.System.Terminate() |> ignore                                
                               


            |   _ -> ignore()
        return! loop()
    }
    loop()

let supervisor = spawn system "supervisor" Supervisor
supervisor <! Begin("Begin")
#time "on"
system.WhenTerminated.Wait()