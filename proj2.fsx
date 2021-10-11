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
let timer = System.Diagnostics.Stopwatch()
let mutable dead = []

type Comm =
    | Begin of string    
    | BuildNetwork of string * IActorRef * list<IActorRef> 
    | Rumour of int * IActorRef * IActorRef
    | Terminate of string * IActorRef
    | Receive of float * float * IActorRef * IActorRef
    | Remove of IActorRef


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
    // Console.WriteLine(actor.ToString() + " " + neighbours.ToString())
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
                |   Rumour(gossip,source,supervisor) -> 

                        if not firstTime then
                            // Console.WriteLine (mailbox.Self.Path.Name + " Received Again " + threshold.ToString())
                            
                            if source <> mailbox.Self then
                                threshold <- threshold - 1
                            //    Console.WriteLine (mailbox.Self.Path.Name + " Received Again " + threshold.ToString()) //+ "Count " + count.ToString())
                            if threshold > 0 && neighbours.Length <> 0 then
                                neighbours.[r.Next(neighbours.Length)] <! Rumour(gossip,mailbox.Self,supervisorRef)//,received)
                                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0),mailbox.Self,Rumour(gossip,mailbox.Self,supervisorRef))
                            else if threshold = 0 && source <> mailbox.Self || neighbours.Length = 0 then
                                if not(List.contains mailbox.Self dead) then
                                    supervisor <! Terminate("Down",mailbox.Self)
                                // Console.WriteLine (mailbox.Self.ToString() + "Down " + threshold.ToString())// + "Count " + count.ToString())
                        else 
                            Console.WriteLine (mailbox.Self.Path.Name + " First " + threshold.ToString())// + "Count " + count.ToString())
                            firstTime <- false
                            if neighbours.Length <> 0 then
                                neighbours.[r.Next(neighbours.Length)] <! Rumour(gossip,mailbox.Self,supervisor)//,received + 1)
                                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0),mailbox.Self,Rumour(gossip,mailbox.Self,supervisorRef))
                            else 
                                if not(List.contains mailbox.Self dead) then
                                    supervisor <! Terminate("Down",mailbox.Self)
//                            system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(1.0),TimeSpan.FromSeconds(0.5),mailbox.Self,Rumour(gossip,mailbox.Self,actorList,supervisorRef))
                
                |   Remove(actor) -> //Console.WriteLine actor
                                    //Console.WriteLine (List.contains actor neighbours) 
                                    if List.contains actor neighbours then
                                        let actorId = actor.Path.Name.Split('_').[1] |> int
                                        let mutable temp = 0
                                        let mutable newList = []
                                        for i in neighbours do
                                            temp <- (i.Path.Name.Split '_').[1] |> int
                                            if actorId <> temp then
                                                newList <- i :: newList
                                        neighbours <- newList
                                        // if neighbours.Length = 0 then
                                        //     Terminate("Down",mailbox.Self) |> ignore
                                        //Console.WriteLine (mailbox.Self.ToString() + " " + neighbours.Length.ToString())
                                    // Console.WriteLine (List.contains actor neighbours)
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
    
                |   Receive(s_1,w_1,source,supervisor) ->

                        if source <> mailbox.Self then //supervisor then
                            s <- s + s_1
                            w <- w + w_1

                        let newRatio = s/w |> float

                        if not firstTime then
                            if source <> mailbox.Self  then //&& source <> supervisor then
                                if abs(newRatio - ratio) <= numCheck || newRatio = ratio then 
                                    threshold <-  threshold - 1
                                else
                                   threshold <- 3

                            //Console.WriteLine (mailbox.Self.Path.Name + " Received Again " + threshold.ToString() + " Ratio " + abs(newRatio - ratio).ToString())
                            if threshold > 0 && neighbours.Length <> 0 then
                                s <- s/2.0
                                w <- w/2.0
                                ratio <- newRatio
                                neighbours.[r.Next(neighbours.Length)] <! Receive(s,w,mailbox.Self,supervisorRef)
                                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0),mailbox.Self,Receive(s,w,mailbox.Self,supervisorRef))
                            else if (threshold = 0 && source <> mailbox.Self) || neighbours.Length = 0 then
                                if not(List.contains mailbox.Self dead) then
                                    supervisor <! Terminate("Down",mailbox.Self)
                                    //Console.WriteLine (mailbox.Self.ToString() + "Down " + threshold.ToString())

                        else 
                            Console.WriteLine (mailbox.Self.Path.Name + " First " + threshold.ToString() + " Ratio " + abs(newRatio - ratio).ToString())
                            firstTime <- false
                            s <- s/2.0
                            w <- w/2.0
                            if neighbours.Length <> 0 then
                                neighbours.[r.Next(neighbours.Length)] <! Receive(s,w,mailbox.Self,supervisorRef)//,received + 1)
                                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0),mailbox.Self,Receive(s,w,mailbox.Self,supervisorRef))
                            else 
                                if not(List.contains mailbox.Self dead) then
                                    supervisor <! Terminate("Down",mailbox.Self)    
//                            system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromSeconds(0.5),mailbox.Self,Receive(s,w,mailbox.Self,supervisorRef))

                |   Remove(actor) -> //Console.WriteLine actor
                                    //Console.WriteLine (List.contains actor neighbours) 
                                    if List.contains actor neighbours then
                                        let actorId = actor.Path.Name.Split('_').[1] |> int
                                        let mutable temp = 0
                                        let mutable newList = []
                                        for i in neighbours do
                                            temp <- (i.Path.Name.Split '_').[1] |> int
                                            if actorId <> temp then
                                                newList <- i :: newList
                                        neighbours <- newList
                                        if neighbours.Length = 0 then
                                            Terminate("Down",mailbox.Self) |> ignore
                                         //Console.WriteLine (mailbox.Self.ToString() + " " + neighbours.Length.ToString())


                |   _ -> Console.WriteLine workermessage 
                        //ignore()
            return! loop()            
    }

    loop ()


let Supervisor (mailbox: Actor<_>) =
    let mutable actorList = []
    let rec loop () = actor {
        let! supervisormessage = mailbox.Receive()
        let mutable gossip = 0
        match supervisormessage with
            |   Begin(_) ->
                    if algorithm = "gossip" then
                        actorList <-  [ for i in 1 .. numNodes do yield (spawn system ("Actor_" + string (i))) Gossip]
                        //Console.WriteLine actorList
                        actorList |> List.iter(fun node -> node <! BuildNetwork(topology,mailbox.Self,actorList))
                        gossip <- r.Next()
                        actorList.[r.Next(1,numNodes)] <! Rumour(gossip,mailbox.Self,mailbox.Self)
                    else
                        actorList <- [ for i in 1 .. numNodes do yield (spawn system ("Actor_" + string (i))) Pushsum]
                        actorList |> List.iter(fun node -> node <! BuildNetwork(topology,mailbox.Self,actorList))
                        let selectedActor = actorList.[r.Next(1,numNodes)]
                        selectedActor <! Receive(selectedActor.Path.Name.Split('_').[1] |> float,1.0,mailbox.Self,mailbox.Self)

            |   Terminate(termMsg,actor) -> 
                    //Console.WriteLine termMsg
                    if termMsg = "Down" then
                        if not (List.contains actor dead) then
                            dead <- actor :: dead
                            actorList |> List.iter(fun node -> node <! Remove(actor) )   
                            Console.WriteLine dead.Length
                        if dead.Length = numNodes then
                            mailbox.Self <! Terminate("Done",mailbox.Self)

                            // system.Terminate() |> ignore
                    if termMsg = "Done" && actor = mailbox.Self then
                        mailbox.Context.System.Terminate() |> ignore
                        system.Terminate() |> ignore
                    //     // mailbox.Context.System.Terminate() |> ignore                                
                               


            |   _ -> ignore()
        return! loop()
    }
    loop()

let supervisor = spawn system "supervisor" Supervisor
supervisor <! Begin("Begin")
#time "on"
system.WhenTerminated.Wait()