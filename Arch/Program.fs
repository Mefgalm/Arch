open System
open Akka.FSharp
open Akka.Actor

let inline (^) f x = f x

let system = System.create "my-system" (Configuration.load())

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

    | SagaState.Forward (_, next::_, completed), SagaEvent.ForwardFailed ->
        SagaState.Backward (next |> forwardToBackward, completed |> List.map forwardToBackward, [])

    | SagaState.Backward (current, next::remains, completed), SagaEvent.Backwarded ->
        SagaState.Backward (next, remains, current::completed)

    | SagaState.Backward (current, [], completed), SagaEvent.Backwarded ->
        SagaState.BackwardComplete (current::completed)

    | SagaState.Backward (current, remains, completed), SagaEvent.BackwardFailed ->
        SagaState.BackwardAbort (remains, current::completed)

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
    UserCreated (id, firstName, email) |> List.singleton

let updateEmail user newEmail =
    EmailUpdated (user.Id, newEmail) |> List.singleton

type ReadBaseRepository<'key, 'entity> =
    { Get: ('key -> 'entity)
      Update: ('entity -> unit) }

type ReadUnitOfWork =
    { UserRepository: ReadBaseRepository<Guid, User> }

let eventHandlerActor () =
    spawn system "event-handler-actor" 
        (fun mailbox -> 
            let rec loop () = actor {

                let! event = mailbox.Receive()

                match event with 
                | User (UserCreated (id, firstName, email)) as o -> 
                    printfn "%A" o
                | User (EmailUpdated (id, newEmail)) as o -> 
                    printfn "%A" o
            }
            loop ())

type UserRepository =
    { Get: (Guid -> User) }

type WriteUnitOfWork = 
    { UserRepository: UserRepository }

let commandHandlerActor sagaParent =
    spawn sagaParent "command-handler-actor" 
        (actorOf2 (fun mailbox command -> 
                   match command with
                   | Command.CreateUser (id, firstName, lastName) -> 
                       mailbox.Sender() <! (Ok ^ createUser id firstName lastName)

                   | Command.ChangeUserEmail (id, newEmail) ->
                       mailbox.Sender() <! (Ok ^ [])

                   | Command.RemoveUser id -> 
                       mailbox.Sender() <! (Ok ^ [])))

let ask commandHandlerActor command = 
    (commandHandlerActor <? command)
    |> Async.RunSynchronously

let runSaga state eventHandlerActor parent =
    spawn parent "saga-handler-actor"
            (fun mailbox -> 
                let cmdActor () = commandHandlerActor mailbox

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
                            let result = commandHandlerActor mailbox <? command |> Async.RunSynchronously

                            match result with
                            | Ok events -> 
                                events |> Seq.iter ((<!) eventHandlerActor) //should I need handle exceptions from events?

                                [SagaEvent.Forwarded]
                            | Error _ -> [SagaEvent.ForwardFailed]

                        return! loop (sagaEvents |> playEvents state)
                    | SagaState.Backward (Backward (_, compensation), _, _) ->
                        let sagaEvents =
                            match ask (cmdActor()) compensation with
                            | Ok events -> 
                                events |> Seq.iter ((<!) eventHandlerActor)

                                [SagaEvent.Backwarded]
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

let requestHandlerActor eventHandlerActor = 
    spawn system "request-handler-actor" 
        (fun mailbox -> 
            actor {
                let! request = mailbox.Receive()
                
                match request with
                | Request.CreateUser (firstName, email) -> 
                    let saga = runSaga initSaga eventHandlerActor mailbox

                    let userId = idGenerator()

                    saga <! SagaCommand.AddForwardCommand ^ Forward ((Command.CreateUser (userId, firstName, email)), Command.RemoveUser ^ userId)
                    let sagaResult = saga <? SagaCommand.Start |> Async.RunSynchronously

                    match sagaResult with
                    | SagaResponse.ForwardComplete -> mailbox.Sender() <! "ok"
                    | _ -> mailbox.Sender() <! "fail"

                | _ -> ()
            })

let requestAsk commandHandlerActor command =
    async {
        let! response = (commandHandlerActor <? command)
        
        return response
    }
    |> Async.RunSynchronously        

[<EntryPoint>]
let main argv =
    let request = Request.CreateUser ("vlad", "hot-dog@gmail.com")

    let eventHandlerActor = eventHandlerActor()

    let requestHandlerActor = requestHandlerActor eventHandlerActor

    let response = requestAsk requestHandlerActor request
    
    printfn "%A" response

    Console.ReadKey() |> ignore
    0