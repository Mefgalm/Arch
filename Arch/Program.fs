open System
open Akka.FSharp
open Akka.Actor

let inline (^) f x = f x

let inline (<^?) (actor: IActorRef) message: 'a =
    actor.Ask(message) |> Async.AwaitTask |> Async.RunSynchronously :?> 'a

let system = System.create "my-system" (Configuration.load())


let reduceResults results =
    List.reduce (fun r1 r2 ->
        match r1, r2 with
        | Ok (), Ok () -> Ok ()
        | Ok (), Error str 
        | Error str, Ok _ -> Error str
        | Error str1, Error str2 -> Error (sprintf "%s %s" str1 str2)) results

type Cmd =
    | CreateUser of string
    | RemoveUser of Guid

type Evt =
    | UserCreated of Guid * string
    | UserRemoved of Guid

let idGenerator () = Guid.NewGuid()

[<RequireQualifiedAccessAttribute>]
type Request =
    | CreateUser of FirstName: string * Email: string
    | ChangeUserEmail of Id: Guid * NewEmail: string

[<RequireQualifiedAccessAttribute>]
type Command =
    | CreateUser of Id: Guid * FirstName: string * Email: string
    | RemoveUser of Id: Guid
    | ChangeUserEmail of Id: Guid * NewEmail: string
    
type ForwardCommand = Forward of Command * Compensation: Command
type BackwardCommand = Backward of Command * Compensation: Command

[<RequireQualifiedAccessAttribute>]
type SagaState =
    | Empty
    | Fill of ForwardCommand list
    | Forward of Currect: ForwardCommand * Remains: ForwardCommand list * Completed: ForwardCommand list
    | Backward of Currect: BackwardCommand * Remains: BackwardCommand list * Completed: BackwardCommand list
    | BackwardAbort of Remains: BackwardCommand list * Completed: BackwardCommand list
    | ForwardComplete of Completed: ForwardCommand list
    | BackwardComplete of Completed: BackwardCommand list
    | Stop

[<RequireQualifiedAccessAttribute>]
type SagaEvent =
    | SagaForwardCommandAdded of ForwardCommand
    | Started
    | Forwarded
    | ForwardFailed
    | Backwarded
    | BackwardFailed
    | ForwardDone
    | BackwardDone
    | Stoped

[<RequireQualifiedAccessAttribute>]
type SagaCommand =
    | AddForwardCommand of ForwardCommand
    | Start
    | Stop

let initSaga = SagaState.Empty

let forwardToBackward (Forward (command, compensation)) = Backward (command, compensation)

let applyEvent state event =
    printfn "Current state is: %A and event %A" state event

    match state, event with
    | SagaState.Empty, SagaEvent.SagaForwardCommandAdded cmd ->
        SagaState.Fill ^ [cmd]

    | SagaState.Fill cmds, SagaEvent.SagaForwardCommandAdded cmd ->
        SagaState.Fill ^ (cmd::cmds)

    | SagaState.Fill (_::_ as cmds), SagaEvent.Started ->
        let current, remains = cmds |> List.rev |> (fun (x::xs) -> x, xs)
        SagaState.Forward (current, remains, [])

    | SagaState.Forward (current, next::remains, completed), SagaEvent.Forwarded ->
        SagaState.Forward (next, remains, current::completed)

    | SagaState.Forward (current, [], completed), SagaEvent.Forwarded ->
        SagaState.ForwardComplete (current::completed)
    
    | SagaState.Forward (current, _, completed), SagaEvent.ForwardFailed ->
        SagaState.Backward (current |> forwardToBackward, completed |> List.map forwardToBackward, [])

    | SagaState.Backward (current, next::remains, completed), SagaEvent.Backwarded ->
        SagaState.Backward (next, remains, current::completed)

    | SagaState.Backward (current, [], completed), SagaEvent.Backwarded ->
        SagaState.BackwardComplete (current::completed)

    | SagaState.Backward (_, remains, completed), SagaEvent.BackwardFailed ->
        SagaState.BackwardAbort (remains, completed)

    | _, SagaEvent.Stoped ->
        SagaState.Stop
    | state, event -> failwith (sprintf "Wrong state %A %A" state event)

let playEvents state events = events |> List.fold applyEvent state 

[<RequireQualifiedAccess>]
type SagaResponse =
    | ForwardComplete
    | BackwardComplete
    | BackwardAbort of string
    | Stop of string

type User = 
    { Id: Guid
      FirstName: string
      Email: string }

type UserEvent =
    | UserCreated of Id: Guid * FirstName: string * Email: string
    | EmailUpdated of Id: Guid * NewEmail: string
    
type DomainEvent = 
    | User of UserEvent

let createUser id firstName email =
    UserCreated (id, firstName, email)
    |> DomainEvent.User
    |> List.singleton
    |> Result<DomainEvent list, string>.Ok

let updateEmail newEmail =
    EmailUpdated (Guid.Empty, newEmail)
    |> List.singleton
    |> Result<UserEvent list, string>.Ok
//    "wrong email"
//    |> Result<UserEvent list, string>.Error

type ReadBaseRepository<'key, 'entity> =
    { Get: ('key -> 'entity)
      Update: ('entity -> unit) }

type ReadUnitOfWork =
    { UserRepository: ReadBaseRepository<Guid, User> }


let eventStore () = Ok ()

let mongoDb () = Ok ()


let eventHandler event =
    match event with 
    | User (UserCreated (id, firstName, email) as e) -> 
         reduceResults [eventStore(); mongoDb ()]
        
    | User (EmailUpdated (id, newEmail) as e) ->
        reduceResults [eventStore(); mongoDb ()]


let eventHandlerActor =
    spawn system "event-handler-actor" 
        (fun mailbox -> 
            let rec loop () = actor {

                let! event = mailbox.Receive()

                eventHandler event |> ignore //I'm totally sure that it's ok (づ｡◕‿‿◕｡)づ
            }
            loop ())

type UserRepository =
    { Get: (Guid -> User) }


type WriteUnitOfWork = 
    { UserRepository: UserRepository }


let commandHandlerActor =
    spawn system "command-handler-actor" 
        (actorOf2 (fun mailbox command -> 
                   match command with
                   | Command.CreateUser (id, firstName, lastName) ->
                       //send to all
                       let userEvents = createUser id firstName lastName
                       
                       //sync
                       let result =
                           userEvents
                            |> Result.map (List.map eventHandler)
                            |> Result.bind reduceResults
                            
                       mailbox.Sender() <! result

                   | Command.ChangeUserEmail (id, newEmail) ->
                       let userEmails = updateEmail newEmail
                       
                       //async
                       let result =
                           match userEmails with
                           | Ok events -> 
                                events |> List.iter ((<!) eventHandlerActor)
                                Ok ()
                           | Error str -> Error str
                                
                       mailbox.Sender() <! result

                   | Command.RemoveUser id -> 
                       mailbox.Sender() <! (Ok ^ [])))


let runSaga state eventHandlerActor =
    spawn system ("saga-handler-actor-" + Guid.NewGuid().ToString()) 
            (fun mailbox -> 
                let rec loop state = actor {
                    match state with 
                    | SagaState.Empty ->
                        let! msg = mailbox.Receive()

                        match msg with
                        | SagaCommand.AddForwardCommand forwardCommand ->
                            let events = [SagaEvent.SagaForwardCommandAdded forwardCommand]
                            return! loop (events |> playEvents state)
                        | _ ->
                            return! loop state
                    | SagaState.Fill _ ->
                        let! msg = mailbox.Receive()

                        match msg with
                        | SagaCommand.AddForwardCommand forwardCommand ->
                            let events = [SagaEvent.SagaForwardCommandAdded forwardCommand]
                            
                            return! loop (events |> playEvents state)
                        | SagaCommand.Start ->
                            let events = [SagaEvent.Started]
                            return! loop (events |> playEvents state)
                        
                        | _ ->
                            return! loop state
                    | SagaState.Forward (Forward (command, _), _, _) ->
                        let sagaEvents =
                            let result: Result<unit, string> = commandHandlerActor <^? command 

                            match result with
                            | Ok () -> [SagaEvent.Forwarded]
                            | Error _ -> [SagaEvent.ForwardFailed]                            

                        return! loop (sagaEvents |> playEvents state)
                    | SagaState.Backward (Backward (_, compensation), _, _) ->
                        let sagaEvents =
                            let result: Result<unit, string> = commandHandlerActor <^? compensation
                            
                            match result with
                            | Ok () -> [SagaEvent.Backwarded]
                            | Error _ -> [SagaEvent.BackwardFailed]

                        return! loop (sagaEvents |> playEvents state)

                    | SagaState.BackwardAbort (remains, completed) ->
                        mailbox.Sender() <! SagaResponse.BackwardAbort "BackwardAbort"

                    | SagaState.ForwardComplete completed ->
                        mailbox.Sender() <! SagaResponse.ForwardComplete

                    | SagaState.BackwardComplete completed ->
                        mailbox.Sender() <! SagaResponse.BackwardComplete

                    | SagaState.Stop ->
                        mailbox.Sender() <! SagaResponse.Stop
                }
            
                loop state)


let requestHandler eventHandlerActor request =
    match request with
    | Request.CreateUser (firstName, email) ->                    
                    
        let saga = runSaga initSaga eventHandlerActor

        let userId = idGenerator()

        saga <! SagaCommand.AddForwardCommand ^ Forward ((Command.CreateUser (userId, firstName, email)), Command.RemoveUser ^ userId)
        saga <! SagaCommand.AddForwardCommand ^ Forward ((Command.ChangeUserEmail (userId, "new-email@g.com"), Command.ChangeUserEmail (userId, email)))
        
        saga <^? SagaCommand.Start
    | _ ->
        SagaResponse.ForwardComplete
        

[<EntryPoint>]
let main argv =
    
    let request = Request.CreateUser ("vlad", "hot-dog@gmail.com")

    let eventHandlerActor = eventHandlerActor

    //let requestHandlerActor = requestHandlerActor eventHandlerActor
    printfn "%A" (requestHandler eventHandlerActor request)        




    Console.ReadKey() |> ignore
    0