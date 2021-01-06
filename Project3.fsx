open System.Diagnostics
open System.Threading
#r "packages/Akka.dll"
#r "packages/Akka.FSharp.dll"
open System
open Akka.FSharp
open Akka.Actor
open System.Linq;
open System.Collections.Generic

let system = ActorSystem.Create("Project")

// The node life cycle is defined here.
type LifeCycle = 
    | InitState of string * string  * bool * int
    | InitiateJoin 
    | SetNeighbourhood of List<string> * string
    | SetRoutingTable of List<List<List<string>>> * string  
    | SetLeafNodes of List<string> * List<string> * string
    | Join of string * int * List<List<List<string>>>
    | ResendJoinReq
    | UpdateCurrentState of string
    | StartMessageTransfer 
    | HopMessage of string * string * int
    | MessageReachedDestination of string * int
    | UpdateGlobalCounters of int * string



// CommandLinesArgs is used to read the commands and the input is later assigned to limit and binder.
let args : string array = fsi.CommandLineArgs |> Array.tail
if args.Length <> 2 then
        failwith "Error: Expected 2 arguments example 'dotnet fsi --langversion:preview Program.fsx 100000 24'."
let numNodes = args.[0] |> int 
let messagesCount = args.[1] |> int


// Initially the count of messages received and hop count is set zero.
let mutable reachedMessagesCount = 0
let mutable hopCount = 0 

// Below stats are used to track the usage of the system resources
let timer = Stopwatch()
let random = Random()
let mutable nodeIds =  new List<string>()
let mutable nodes =  new Dictionary< string, IActorRef>()
let mutable joinedNetworkcount = 0

// Node that implements the Pastry network.
let node (inbox: Actor<_>) =
    let mutable nodeId = ""
    let mutable intNodeId = 0
    let mutable globalIndex =0 
    let mutable neighbours= new List<string>()
    let mutable closestNodeId = ""
    let mutable smallerleafNodes =  new List<string>()
    let mutable greaterleafNodes =  new List<string>()
    let mutable routeTable = new List<List<string>>()
    let mutable isPartOfNetwork =  false;
    let mutable totalMessagesReached = 0;
    let mutable allNodesWhoseMsgReachedDestination =0;
    let mutable globalHopsCount =0;
    let mutable totalHops = 0;
    let rec loop () = actor {
        let! message = inbox.Receive ()
        match message with
        // Initialize the node id, route table(null values) and global index for each node.
        | InitState(id,closest,isFailedNode,gIndex)  ->
            nodeId <- id
            intNodeId  <- id |> int
            globalIndex <- gIndex
            let mutable i= 0
            while i<=7 do 
                routeTable.Add(new List<string>())
                let mutable q=0;
                while q<=9 do
                    routeTable.ElementAt(i).Add(null)
                    q<-q+1
                i<-i+1
            
            // If the node is the first one, add it directly to the network. Else assign the closest node to the given parameter.
            if(closest = "-1") then
                neighbours <-  new List<string>();
                smallerleafNodes <- new List<string>();
                greaterleafNodes <- new List<string>()
                isPartOfNetwork <-true
            else  
                closestNodeId <- closest 
        // Node requests the network(the closest node id that joined the network) to join.
        | InitiateJoin ->
            if closestNodeId <> "-1" then  
                nodes.[closestNodeId]<! Join(nodeId, 0, new List<List<List<string>>>()) 

        // If the closest is not part of network then the join request is sent again.
        | ResendJoinReq ->
            nodes.[closestNodeId]<! Join(nodeId, 0, new List<List<List<string>>>())

        // Setting neighbourhood nodes of current node.
        | SetNeighbourhood(neighbourList, senderNodeId) ->
            // If the no. of neighbours returned in list is less than 8, then the node is added to the neighbour list.
            // Else the neighbour nodes from 1 to 7 are added to neighbour list and sender node is added at 8th place of neighbour list.
            if( neighbourList.Count < 8) then 
                for i in  neighbourList do
                    neighbours.Add(i)
            else 
                for i in 1..7 do 
                    neighbours.Add(neighbourList.ElementAt(i))
            neighbours.Add(senderNodeId)
        // This is used to set the leaf nodes of the node.
        | SetLeafNodes(smallerLeafs, greaterLeafs, senderNodeId) ->
            // If the sender node falls in the list of leaf nodes then add the sender to leaf nodes(greater or smaller).
            // After adding the greater nodes to the leaf nodes reorder and remove the smallest or largest node(depending on the sender node).
            if (senderNodeId |> int ) > intNodeId then
                if(greaterLeafs.Count < 3) then
                    for i in greaterLeafs do 
                        greaterleafNodes.Add(i)
                else 
                    for i in 0..1 do
                        greaterleafNodes.Add(greaterLeafs.ElementAt(i))                    
                greaterleafNodes.Add(senderNodeId)
            else
                smallerleafNodes.Add(senderNodeId)
                if(smallerLeafs.Count < 3) then
                    for i in smallerLeafs do 
                        smallerleafNodes.Add(i)
                else 
                    for i in 1..2 do
                        smallerleafNodes.Add(smallerLeafs.ElementAt(i)) 

            if((senderNodeId |>int) < intNodeId) then
                for i in greaterLeafs do 
                    greaterleafNodes.Add(i)
            else 
                for i in smallerLeafs do 
                    smallerleafNodes.Add(i)  
            // Reordering.        
            smallerleafNodes <- smallerleafNodes.OrderBy(fun n -> n).ToList()                 
            greaterleafNodes <- greaterleafNodes.OrderBy(fun n -> n).ToList() 

        // The routing table is set in this function.
        | SetRoutingTable(routeTableLists,senderNodeId) ->
            let mutable set  = new HashSet<string>()
            // Converting all the route table node id's into single list.
            for routeTables in routeTableLists do
                for routes in routeTables do
                    for i in routes do  
                        if not (isNull i) then set.Add(i) |> ignore
            set.Add(senderNodeId) |>ignore
            // Constructing the routing table based on the position that a node fits into.
            let mutable totalNodes = set.ToList();
            let mutable n =7
            while(n>=0) do
                let mutable list = new List<string>(); 
                list <-  totalNodes.Where(fun x -> x.StartsWith(nodeId.Substring(0,n))).ToList();
                totalNodes <- totalNodes.Where(fun x -> (not (x.StartsWith(nodeId.Substring(0,n))))).ToList();
                let mutable q=0;
                while q<=9 && list.Count>0 do 
                    let reqNode = list.Where(fun x-> ((string)(x.Chars(n))) = (string)q ).OrderBy(fun x ->  (Math.Abs((x |> int) - (nodeId |> int ) |> int))).FirstOrDefault(); 
                    routeTable.ElementAt(n).RemoveAt(q)         
                    routeTable.ElementAt(n).Insert(q,reqNode)
                    q<-q+1
                n<-n-1
            
            // Broadcasting about the new node id to each node in the network.
            let updatebleNodesList = new List<string>();
            for i in 0..(globalIndex-1) do
                updatebleNodesList.Add(nodeIds.ElementAt(i))
            for i in updatebleNodesList do
                nodes.[i] <! UpdateCurrentState(nodeId)
            joinedNetworkcount <- joinedNetworkcount+1
            isPartOfNetwork <- true



        // Join function for the new node is defined here. If the node is not in the network then the join request is sent again.
        | Join(newNodeId,index,hopRouteTableList) ->
            // If current node is not part of network, ask the new node to resend the request. 
            if (not isPartOfNetwork && index=0) then 
                inbox.Sender() <! ResendJoinReq         
            else
                let intNewNodeId  =  newNodeId |> int
                if index =0 then 
                   nodes.[newNodeId] <! SetNeighbourhood(neighbours,nodeId) 
                hopRouteTableList.Add(routeTable)
                //If the new node is less than node and smaller leafs are zero then new node is set to smaller leaf list of node.
                //Or if the new node is greater than node and number greater leafs are zero than new node set to greater leaf list of the node.
                //Or the new node is set the node.
                if ((intNodeId>intNewNodeId && smallerleafNodes.Count = 0) || (intNodeId<intNewNodeId && greaterleafNodes.Count=0) || intNodeId= intNewNodeId) then
                    nodes.[newNodeId] <! SetLeafNodes(smallerleafNodes,greaterleafNodes,nodeId)
                    nodes.[newNodeId] <! SetRoutingTable(hopRouteTableList,nodeId)
                else //The physical distance is calculated from the node to new node. Later the new node is assigned according to the distance.
                    let disTanceFromCurrent =  Math.Abs(intNodeId - intNewNodeId) 
                    let lowerBound = if (smallerleafNodes.Count > 0) then   (smallerleafNodes.ElementAt(0) |>int) else intNodeId
                    let upperBound = if (greaterleafNodes.Count > 0) then  (greaterleafNodes.ElementAt(greaterleafNodes.Count-1) |>int) else intNodeId

                    //If the new node is in betwwen lower bound and upper bound, the distance from each node smaller leaf list and greater leaf list is calculated and the new node is set accordingly.
                    if(lowerBound<intNewNodeId && upperBound>intNewNodeId ) then 
                        let mutable shortestDistance = 0
                        let mutable nextNodeId =  ""
                        for l in smallerleafNodes do 
                            let distance  =  Math.Abs((l |> int) - intNewNodeId)
                            if(distance<shortestDistance || shortestDistance = 0) then 
                                shortestDistance <- distance
                                nextNodeId <- l
                        for g in greaterleafNodes do 
                            let distance  =  Math.Abs((g |> int) - intNewNodeId)
                            if(distance<shortestDistance || shortestDistance = 0) then 
                                shortestDistance <- distance
                                nextNodeId <- g
                        if (shortestDistance < disTanceFromCurrent  && shortestDistance<>0 )then
                            (nodes.[nextNodeId]) <! Join(newNodeId,index+1, hopRouteTableList)
                        else
                            nodes.[newNodeId] <! SetLeafNodes(smallerleafNodes,greaterleafNodes,nodeId)
                            nodes.[newNodeId] <! SetRoutingTable(hopRouteTableList,nodeId)
                    else
                        // Get the node from route table using the matching index as a row and +1 index as a column.
                        let mutable k = 0
                        let mutable flag = true
                        while k<=7 && flag do 
                            if(nodeId.Chars(k)<>newNodeId.Chars(k)) then 
                                flag<-false
                            k <- k+1 
                        let nodesList =  routeTable.ElementAt(k-1);
                        let jIndex = (newNodeId.Substring(k-1,1)|>int)
                        let item  =  nodesList.ElementAt(jIndex);
                        // If node id is present forward the join request to that node else get the closer node from the current row, leaf nodes and neighbours.
                        if not (isNull item) then 
                            (nodes.[item]) <! Join(newNodeId,index+1, hopRouteTableList)
                        else
                            let matchableNodes = nodesList.Concat(neighbours).ToList().Concat(smallerleafNodes).ToList().Concat(greaterleafNodes).Where(fun x -> not (isNull x) ).ToList()
                            let mutable shortestDistance = 0
                            let mutable nextNodeId =  ""
                            for l in matchableNodes do 
                                let distance  =  Math.Abs((l |> int) - intNewNodeId)
                                if(distance<shortestDistance || shortestDistance = 0) then 
                                    shortestDistance <- distance
                                    nextNodeId <- l
                            // If the node with shortest distance is found, forward to that node. Else current node can add the new node to network.
                            if shortestDistance < disTanceFromCurrent && shortestDistance<>0 then
                                (nodes.[nextNodeId]) <! Join(newNodeId,index+1, hopRouteTableList)
                            else
                                nodes.[newNodeId] <! SetLeafNodes(smallerleafNodes,greaterleafNodes,nodeId)
                                nodes.[newNodeId] <! SetRoutingTable(hopRouteTableList,nodeId)
        

        // Whenever a node is added to the network, every other node in the newtork will be notified using UpdateCurrentState.
        | UpdateCurrentState(newNodeId) ->  
            let intNewNodeId = newNodeId |> int
            let lowerBound = if (smallerleafNodes.Count > 0) then   (smallerleafNodes.ElementAt(0) |>int) else intNodeId
            let upperBound = if (greaterleafNodes.Count > 0) then  (greaterleafNodes.ElementAt(greaterleafNodes.Count-1) |>int) else intNodeId
            if(smallerleafNodes.Count<3  && intNodeId > intNewNodeId) then
                smallerleafNodes.Add(newNodeId)
                smallerleafNodes <-smallerleafNodes.OrderBy(fun x -> x).ToList() 
            
            else if (greaterleafNodes.Count<3 && intNodeId <intNewNodeId) then 
                greaterleafNodes.Add(newNodeId)
                greaterleafNodes <-greaterleafNodes.OrderBy(fun x -> x).ToList() 

            // If node is in between leaf nodes then add it to the leaf nodes and adjust the leaf nodes accordingly.
            else if(lowerBound < intNewNodeId && upperBound> intNewNodeId) then  
                if(intNodeId> intNewNodeId) then 
                    smallerleafNodes.RemoveAt(0)
                    smallerleafNodes.Add(newNodeId)  
                else
                    greaterleafNodes.RemoveAt(2)
                    greaterleafNodes.Add(newNodeId)
                greaterleafNodes <- greaterleafNodes.OrderBy(fun x -> x).ToList() 
                smallerleafNodes <- smallerleafNodes.OrderBy(fun x -> x).ToList() 

            let mutable k = 0
            let mutable flag = true
            // Check the current node in the routing table in the new node position. 
            // If the distance between the current node is larger than( or null) new node replace the current node with the new node in the same position.   
            while k<=7 && flag do 
                if(nodeId.Chars(k)<>newNodeId.Chars(k)) then 
                    flag<-false
                k <- k+1 
            let jIndex = (newNodeId.Substring(k-1,1)|>int)
            let target  = routeTable.ElementAt(k-1).ElementAt(jIndex);
            if (isNull target) then 
                routeTable.ElementAt(k-1).RemoveAt(jIndex);
                routeTable.ElementAt(k-1).Insert(jIndex,newNodeId)
            else if Math.Abs((target|>int ) - intNodeId) > Math.Abs(intNewNodeId - intNodeId) then
                routeTable.ElementAt(k-1).RemoveAt(jIndex);
                routeTable.ElementAt(k-1).Insert(jIndex,newNodeId)


        // Message transfer is initiated for a node.
        | StartMessageTransfer ->
            let mutable i = 0
            // Generate a random destination and intiate the message transfer(wait for 1 second for each message transfer).
            while(i< messagesCount) do
                let randomDestination = random.Next(0, 99999999).ToString("D8")
                inbox.Self <! HopMessage(nodeId,randomDestination,0)
                Thread.Sleep(1000)
                i<-i+1
        
        // This is used to update the global hop counts.
        // A node(Here we have chose node at index 0) coordinates the global updation. 
        // Every node after getting all the messages transfered to the destination will send a request to the gobal node that keeps track of message transfer in the network.
        | UpdateGlobalCounters(hopsCountOfNode, nodeId) ->
            globalHopsCount <- globalHopsCount + hopsCountOfNode;
            allNodesWhoseMsgReachedDestination <- allNodesWhoseMsgReachedDestination+1
            if(allNodesWhoseMsgReachedDestination = numNodes) then
                reachedMessagesCount <- allNodesWhoseMsgReachedDestination
                hopCount <-hopCount+ globalHopsCount

        // If the message is reached then the destination will send a request to the message initiator to update the reached count and send a request to the coordinator.
        | MessageReachedDestination(key, hops) ->
            totalMessagesReached <- totalMessagesReached + 1
            totalHops <- totalHops + hops
            if ( totalMessagesReached = messagesCount) then   
                nodes.[nodeIds.ElementAt(0)] <! UpdateGlobalCounters(totalHops,nodeId)

        //This is used to forward the message to the destination.
        | HopMessage(parent, destination, currentHopCount)->
           // If there are no other nodes to forward or current node is the final destination then current node is the destination.
            let intDestination = destination |> int
            if ((intNodeId>intDestination && smallerleafNodes.Count = 0) || (intNodeId<intDestination && greaterleafNodes.Count=0) || intNodeId= intDestination) then
                nodes.[parent] <! MessageReachedDestination(destination,currentHopCount)
            
            else
                let disTanceFromCurrent =  Math.Abs(intNodeId - intDestination) 
                let lowerBound = if (smallerleafNodes.Count > 0) then   (smallerleafNodes.ElementAt(0) |>int) else intNodeId
                let upperBound = if (greaterleafNodes.Count > 0) then  (greaterleafNodes.ElementAt(greaterleafNodes.Count-1) |>int) else intNodeId
                // Else check if node is between the smallest leaf node and greatest leaf node.
                if(lowerBound<intDestination && upperBound>intDestination ) then 
                    let mutable shortestDistance = 0
                    let mutable nextNodeId =  ""
                    for l in smallerleafNodes do 
                        let distance  =  Math.Abs((l |> int) - intDestination)
                        if(distance<shortestDistance || shortestDistance = 0) then 
                            shortestDistance <- distance
                            nextNodeId <- l
                    for g in greaterleafNodes do 
                        let distance  =  Math.Abs((g |> int) - intDestination)
                        if(distance<shortestDistance || shortestDistance = 0) then 
                            shortestDistance <- distance
                            nextNodeId <- g
                    // If a node found with shorter distance than current node in the leaf nodes, forward to it. Else current node is the destination.
                    if (shortestDistance < disTanceFromCurrent  && shortestDistance<>0 )then
                        (nodes.[nextNodeId]) <! HopMessage(parent,destination,currentHopCount+1)
                    else
                        nodes.[parent] <! MessageReachedDestination(destination,currentHopCount)
                else
                    // Else check if there is a matching node in the route table at the node's position. 
                    let mutable k = 0
                    let mutable flag = true
                    while k<=7 && flag do 
                        if(nodeId.Chars(k)<>destination.Chars(k)) then 
                            flag<-false
                        k <- k+1 
                    let nodesList =  routeTable.ElementAt(k-1);
                    let mutable jIndex= 0
                    let mutable item =""
                    jIndex <- (destination.Substring(k-1,1)|>int) 
                    item  <-  nodesList.ElementAt(jIndex);
                    // If node is present forward to that node, else forward to the node with shortest distance from the destination in neighbours, leaf node and current row node in routing table.
                    if not (isNull item) then      
                        (nodes.[item]) <! HopMessage(parent, destination,currentHopCount+1)
                    else
                        let matchableNodes = nodesList.Concat(neighbours).ToList().Concat(smallerleafNodes).ToList().Concat(greaterleafNodes).Where(fun x ->not (isNull x)).ToList()
                        let mutable shortestDistance = 0
                        let mutable nextNodeId =  ""
                        for l in matchableNodes do 
                            let distance  =  Math.Abs((l |> int) - intDestination)
                            if(distance<shortestDistance || shortestDistance = 0) then 
                                shortestDistance <- distance
                                nextNodeId <- l
                        if shortestDistance < disTanceFromCurrent && shortestDistance<>0 then
                            (nodes.[nextNodeId]) <! HopMessage(parent, destination,currentHopCount+1)
                        else
                            nodes.[parent] <! MessageReachedDestination(destination,currentHopCount)
        return! loop ()
    }
    loop ()

// Generate random numbers which can be used as a node ids for the actors(nodes).
// We are using 8 digit decimal values to identify the nodes.
// Distance between two nodes is calculated as absolute distance of two 8 digit numbers.
// Neighbours will be nodes that entered just above(8).

let mutable nodeIdInitial = ""
for i in 1 .. numNodes do 
    nodeIdInitial <- random.Next(0, 99999999).ToString("D8")
    while(nodeIdInitial.Length <> 8 || nodeIds.Contains(nodeIdInitial)) do 
        nodeIdInitial <- random.Next(0, 99999999).ToString("D8")
    nodeIds.Add(nodeIdInitial)

// Creating actors.
printfn "Starting the Network join for all nodes. Press Cntrl+c to exit."
for i in 1 .. numNodes do 
    let nodeString = "worker"+ sprintf "%i" i
    let nodeRef = spawn system nodeString node
    let nodeIdNum =  nodeIds.ElementAt(i-1)
    nodes.Add(nodeIdNum,nodeRef)
    let mutable closestNode = ""
    // If node is first to join the network there won't be any previous node. Else the previous node will be the node that just joined the network.
    if(i=1) then
        closestNode <- "-1"
    else 
        closestNode <- nodeIds.ElementAt(i-2)
    nodeRef <! InitState (nodeIdNum,closestNode,false,i-1)
timer.Start()

// Initating join(Node will start requesting the network to join) for each node.
for i in 1..(numNodes-1) do 
    nodes.[nodeIds.ElementAt(i)] <! InitiateJoin

// When all the nodes joined the network, print the time taken.
while joinedNetworkcount <> numNodes-1 do ()
printfn "All nodes joined the network. Time taken for joining the network %i ms" timer.ElapsedMilliseconds

// Once every node is part of network initiate the message transfer.
printfn "Initaiating message transfer. Press Cntrl+c to exit."
timer= Stopwatch()
timer.Start()
for i in 1..(numNodes) do 
    nodes.[nodeIds.ElementAt(i-1)] <! StartMessageTransfer
while reachedMessagesCount <> numNodes  do ()

// Print the time, average hops, hops and total messages.
printfn "All messages reached the destination. Time taken: %i Total Hop Count:%A  Total messages: %A Average Hop count:%A" timer.ElapsedMilliseconds hopCount ((numNodes)*(messagesCount)) ((hopCount|> float)/((numNodes|>float)*(messagesCount |> float)))
system.Terminate ()

   
    

