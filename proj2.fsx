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
    | Receive of float * float * IActorRef * IActorRef


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

    let mutable firstTime = true
    let rec loop () =
        actor {
            let! workermessage = mailbox.Receive()
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


let Pushsum (mailbox: Actor<_>) =
    let mutable neighbours = []
    let mutable supervisorRef = mailbox.Self
    let mutable s = mailbox.Self.Path.Name.Split('_').[1] |> float
    let mutable w = 1.0
    let mutable threshold = 3
    let mutable ratio = s/w |> float
    let numCheck = 10.0 ** -10.0 |> float
    // Console.WriteLine s
    // Console.WriteLine w
    // Console.WriteLine numCheck
    let mutable firstTime = true
    let rec loop () =
        actor {
            let! workermessage = mailbox.Receive()
            match workermessage with
                |   BuildNetwork(topology,supervisor,actorList) ->                
                        if topology = "full" then 
                            neighbours <- createFulltopology mailbox.Self actorList
                        else
                            neighbours <- createLineTopology mailbox.Self actorList
                        supervisorRef <- supervisor    
                |   Receive(s_1,w_1,source,supervisorRef) ->

                        s <- s + s_1
                        w <- w + w_1

                        let newRatio = s/w |> float

                        if not firstTime then
                            if source <> mailbox.Self then
                                if abs(newRatio - ratio) < numCheck then 
                                    threshold <-  threshold - 1
                                else
                                   threshold <- 3

                            // Console.WriteLine (mailbox.Self.Path.Name + " Received Again " + threshold.ToString() + "Ratio " + abs(newRatio - ratio).ToString())
                            if threshold > 0 then
                                s <- s/2.0
                                w <- w/2.0
                                ratio <- newRatio
                                neighbours.[r.Next(neighbours.Length)] <! Receive(s,w,mailbox.Self,supervisorRef)

                            else if threshold = 0 then
                                supervisorRef <! Terminate("Down")
                                Console.WriteLine (mailbox.Self.ToString() + "Down " + threshold.ToString())

                        else 
                            Console.WriteLine (mailbox.Self.Path.Name + " First " + threshold.ToString() + "Ratio " + abs(newRatio - ratio).ToString())
                            firstTime <- false
                            s <- s/2.0
                            w <- w/2.0
                            neighbours.[r.Next(neighbours.Length)] <! Receive(s,w,mailbox.Self,supervisorRef)//,received + 1)
                            system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromSeconds(0.5),mailbox.Self,Receive(s,w,mailbox.Self,supervisorRef))
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
                        actorList.[r.Next(1,numNodes)] <! Rumour(gossip,mailbox.Self,actorList,mailbox.Self)
                    else
                        let actorList = [ for i in 1 .. numNodes do yield (spawn system ("Actor_" + string (i))) Pushsum]
                        actorList |> List.iter(fun node -> node <! BuildNetwork(topology,mailbox.Self,actorList))
                        let selectedActor = actorList.[r.Next(1,numNodes)]
                        selectedActor <! Receive(selectedActor.Path.Name.Split('_').[1] |> float,1.0,mailbox.Self,mailbox.Self)

            |   Terminate(termMsg) -> 
                    Console.WriteLine termMsg
                    if termMsg = "Down" then
                        count <- count + 1
                        Console.WriteLine count
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