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
let timer = Diagnostics.Stopwatch()
let mutable dead = []
let mutable time = 0

type Comm =
    | Begin of string
    | BuildNetwork of string * IActorRef * list<IActorRef>
    | Rumour of int * IActorRef * IActorRef
    | Terminate of string * IActorRef
    | Receive of float * float * IActorRef * IActorRef
    | Remove of IActorRef


//Full Topology
let createFulltopology (actor: IActorRef) (actorList: list<IActorRef>) =
    let id = (actor.Path.Name.Split '_').[1] |> int
    let mutable neighbours = []
    let mutable temp = 0

    for i in actorList do
        temp <- (i.Path.Name.Split '_').[1] |> int

        if id <> temp then
            neighbours <- i :: neighbours

    neighbours

//Line Topology
let createLineTopology (actor: IActorRef) (actorList: list<IActorRef>) =
    let mutable neighbours = []
    let id = (actor.Path.Name.Split '_').[1] |> int
    if id - 1 > 0 then
        neighbours <- actorList.[id - 2] :: neighbours

    if id + 1 < numNodes + 1 then
        neighbours <- actorList.[id] :: neighbours

    // Console.WriteLine(actor.ToString() + "Neighbours: " + neighbours.ToString())
    neighbours


let cubeRootCorrection (number: int) =
    let mutable rows =
        Math.Round(Math.Pow(number |> float, 0.33)) |> int

    rows


//3D Topology
let create3dTopology (actor: IActorRef) (actorList: list<IActorRef>) =
    let mutable neighbours = []
    let id = (actor.Path.Name.Split '_').[1] |> int
    let seed = cubeRootCorrection (numNodes)

    let seedSquare = seed * seed

    if id - 1 > 0 then
        neighbours <- actorList.[id - 2] :: neighbours

    if id - seed > 0 then
        neighbours <- actorList.[id - seed - 1] :: neighbours

    if id - (seedSquare) > 0 then
        neighbours <- actorList.[id - (seedSquare) - 1] :: neighbours

    if id + 1 < numNodes + 1 then
        neighbours <- actorList.[id] :: neighbours

    if id + seed < numNodes + 1 then
        neighbours <- actorList.[id + seed - 1] :: neighbours

    if id + (seedSquare) < numNodes + 1 then
        neighbours <- actorList.[id + (seedSquare) - 1] :: neighbours

    neighbours

//Imperfect 3D Topology
let createImperfect3dTopology (actor: IActorRef) (actorList: list<IActorRef>) =
    let mutable neighbours = []
    let id = (actor.Path.Name.Split '_').[1] |> int
    let seed = cubeRootCorrection (numNodes)

    let seedSquare = seed * seed

    if id - 1 > 0 then
        neighbours <- actorList.[id - 2] :: neighbours

    if id - seed > 0 then
        neighbours <- actorList.[id - seed - 1] :: neighbours

    if id - (seedSquare) > 0 then
        neighbours <- actorList.[id - (seedSquare) - 1] :: neighbours

    if id + 1 < numNodes + 1 then
        neighbours <- actorList.[id] :: neighbours

    if id + seed < numNodes + 1 then
        neighbours <- actorList.[id + seed - 1] :: neighbours

    if id + (seedSquare) < numNodes + 1 then
        neighbours <- actorList.[id + (seedSquare) - 1] :: neighbours

    let mutable newRandomActor = actorList.[r.Next(1, numNodes) - 1]

    while List.contains newRandomActor neighbours
          && (newRandomActor <> actorList.[id]) do
        newRandomActor <- actorList.[r.Next(1, numNodes) - 1]

    neighbours <- newRandomActor :: neighbours

    neighbours


let Gossip (mailbox: Actor<_>) =
    let mutable neighbours = []
    let mutable threshold = 10
    let mutable supervisorRef = mailbox.Self

    let mutable firstTime = true

    let rec loop () =
        actor {
            let! workermessage = mailbox.Receive()

            match workermessage with
            | BuildNetwork (topology, supervisor, actorList) ->
                if topology = "full" then
                    neighbours <- createFulltopology mailbox.Self actorList
                elif topology = "3d" then
                    neighbours <- create3dTopology mailbox.Self actorList
                elif topology = "line" then
                    neighbours <- createLineTopology mailbox.Self actorList
                elif topology = "imperfect3d" then
                    neighbours <- createImperfect3dTopology mailbox.Self actorList
                else
                    printfn "Enter valid Topology!"

                supervisorRef <- supervisor
            | Rumour (gossip, source, supervisor) ->

                if neighbours.Length = 0 then
                    Terminate("Down", mailbox.Self) |> ignore

                if not firstTime then

                    if neighbours.Length = 0 then
                        if not (List.contains mailbox.Self dead) then
                            supervisor <! Terminate("Down", mailbox.Self)

                    if source <> mailbox.Self then
                        threshold <- threshold - 1

                    if threshold > 0 && neighbours.Length <> 0 then
                        neighbours.[r.Next(neighbours.Length)]
                        <! Rumour(gossip, mailbox.Self, supervisorRef)

                        system.Scheduler.ScheduleTellOnce(
                            TimeSpan.FromSeconds(1.0),
                            mailbox.Self,
                            Rumour(gossip, mailbox.Self, supervisorRef)
                        )

                    else if threshold = 0 && source <> mailbox.Self
                            || neighbours.Length = 0 then
                        if not (List.contains mailbox.Self dead) then
                            supervisor <! Terminate("Down", mailbox.Self)

                else

                    firstTime <- false

                    if neighbours.Length <> 0 then
                        neighbours.[r.Next(neighbours.Length)]
                        <! Rumour(gossip, mailbox.Self, supervisor)

                        system.Scheduler.ScheduleTellOnce(
                            TimeSpan.FromSeconds(1.0),
                            mailbox.Self,
                            Rumour(gossip, mailbox.Self, supervisorRef)
                        )
                    else if not (List.contains mailbox.Self dead) then
                        supervisor <! Terminate("Down", mailbox.Self)


            | Remove (actor) ->
                if actor <> mailbox.Self then
                    if List.contains actor neighbours then
                        let actorId = actor.Path.Name.Split('_').[1] |> int
                        neighbours <- neighbours |> List.indexed |> List.filter(fun(_,x)-> x.Path.Name.Split('_').[1] |> int <> actorId) |> List.map snd
                        if neighbours.Length = 0 then
                            Terminate("Down", mailbox.Self) |> ignore
            | _ -> ignore ()

            return! loop ()
        }

    loop ()


let Pushsum (mailbox: Actor<_>) =
    let mutable neighbours = []
    let mutable supervisorRef = mailbox.Self

    let mutable s =
        mailbox.Self.Path.Name.Split('_').[1] |> float

    let mutable w = 1.0
    let mutable threshold = 3
    let mutable ratio = s / w |> float
    let numCheck = 10.0 ** -20.0 |> float
    let mutable firstTime = true

    let rec loop () =
        actor {
            let! workermessage = mailbox.Receive()

            match workermessage with
            | BuildNetwork (topology, supervisor, actorList) ->
                if topology = "full" then
                    neighbours <- createFulltopology mailbox.Self actorList
                elif topology = "3d" then
                    neighbours <- create3dTopology mailbox.Self actorList
                elif topology = "line" then
                    neighbours <- createLineTopology mailbox.Self actorList
                elif topology = "imperfect3d" then
                    neighbours <- createImperfect3dTopology mailbox.Self actorList
                else
                    printfn "Enter valid Topology!"

                supervisorRef <- supervisor

            | Receive (s_1, w_1, source, supervisor) ->

                if source <> mailbox.Self && source <> supervisor then
                    s <- s + s_1
                    w <- w + w_1

                let newRatio = s / w |> float

                if not firstTime then
                    if source <> mailbox.Self && source <> supervisor then
                        if abs (newRatio - ratio) <= numCheck
                           || newRatio = ratio then
                            threshold <- threshold - 1
                        else
                            threshold <- 3

                    if threshold > 0 && neighbours.Length <> 0 then
                        s <- s / 2.0
                        w <- w / 2.0
                        ratio <- newRatio

                        neighbours.[r.Next(neighbours.Length)]
                        <! Receive(s, w, mailbox.Self, supervisorRef)

                        system.Scheduler.ScheduleTellOnce(
                            TimeSpan.FromSeconds(1.0),
                            mailbox.Self,
                            Receive(s, w, mailbox.Self, supervisorRef)
                        )
                    else if (threshold = 0 && source <> mailbox.Self)
                            || neighbours.Length = 0 then
                        if not (List.contains mailbox.Self dead) then
                            supervisor <! Terminate("Down", mailbox.Self)
                else
                    //Console.WriteLine (mailbox.Self.ToString() + "First") 
                    firstTime <- false
                    s <- s / 2.0
                    w <- w / 2.0

                    if neighbours.Length <> 0 then
                        neighbours.[r.Next(neighbours.Length)]
                        <! Receive(s, w, mailbox.Self, supervisorRef)

                        system.Scheduler.ScheduleTellOnce(
                            TimeSpan.FromSeconds(1.0),
                            mailbox.Self,
                            Receive(s, w, mailbox.Self, supervisorRef)
                        )
                    else if not (List.contains mailbox.Self dead) then
                        supervisor <! Terminate("Down", mailbox.Self)

            | Remove (actor) ->
                if List.contains actor neighbours then
                    let actorId = actor.Path.Name.Split('_').[1] |> int
                    neighbours <- neighbours |> List.indexed |> List.filter(fun(_,x)-> x.Path.Name.Split('_').[1] |> int <> actorId) |> List.map snd
                    if neighbours.Length = 0 then
                        Terminate("Down", mailbox.Self) |> ignore


            | _ -> ignore ()

            return! loop ()
        }

    loop ()


let Supervisor (mailbox: Actor<_>) =
    let mutable actorList = []

    let rec loop () =
        actor {
            let! supervisormessage = mailbox.Receive()
            let mutable gossip = 0

            match supervisormessage with
            | Begin (_) ->
                if algorithm = "gossip" then
                    actorList <-
                        [ for i in 1 .. numNodes do
                              yield (spawn system ("Actor_" + string (i))) Gossip ]

                    actorList
                    |> List.iter (fun node ->
                        node
                        <! BuildNetwork(topology, mailbox.Self, actorList))

                    gossip <- r.Next()

                    actorList.[r.Next(1, numNodes)]
                    <! Rumour(gossip, mailbox.Self, mailbox.Self)

                    timer.Start()
                else
                    actorList <-
                        [ for i in 1 .. numNodes do
                              yield (spawn system ("Actor_" + string (i))) Pushsum ]

                    actorList
                    |> List.iter (fun node ->
                        node
                        <! BuildNetwork(topology, mailbox.Self, actorList))

                    let selectedActor = actorList.[r.Next(1, numNodes)]

                    selectedActor
                    <! Receive(selectedActor.Path.Name.Split('_').[1] |> float, 1.0, mailbox.Self, mailbox.Self)

                    timer.Start()
            | Terminate (termMsg, actor) ->

                if termMsg = "Down" then
                    if not (List.contains actor dead) then
                        //Console.WriteLine actor
                        dead <- actor :: dead
                        Console.WriteLine dead.Length
                        actorList
                        |> List.iter (fun node -> node <! Remove(actor))
                    if dead.Length = numNodes then
                        mailbox.Self <! Terminate("Done", mailbox.Self)

                if termMsg = "Done" && actor = mailbox.Self then
                    mailbox.Context.System.Terminate() |> ignore
                    time <- timer.ElapsedMilliseconds |> int
                    system.Terminate() |> ignore
                    


            | _ -> ignore ()

            return! loop ()
        }

    loop ()

let supervisor = spawn system "supervisor" Supervisor
supervisor <! Begin("Begin")
system.WhenTerminated.Wait()
Console.WriteLine("Time taken to Converge " + time.ToString())
