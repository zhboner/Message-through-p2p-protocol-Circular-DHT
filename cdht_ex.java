import java.io.*;
import java.net.*;
import java.util.Scanner;

public class cdht_ex {
    static int base,
            peerIdentity,
            firstSuccessor,
            secondSuccessor,
            peerPort,
            firstSuccessorPort,
            secondSuccessorPort,
            count = 0;
    static boolean firstPingBack = false,       // Used to check if there is a response from first successor
            secondPingBack = false,             // Same as above
            userInput = false,                  // Control messages shown or not
            requestFileMessage = false;         // Same as above
    static DatagramSocket UDPSocket;
    //static Socket TCPSocket;

    public static DatagramPacket UDPClient(int destinationPort) throws Exception{
//        Send a UDP "Request" message

        String message = "Request";
        InetAddress IPAddress = InetAddress.getByName("localhost");
        byte[] sendData = message.getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, destinationPort);
        return sendPacket;
    }
    public static void UDPServer() throws Exception{
//        Receive "Request" message and send a "Response" message back

        boolean haveShownHideMessage = false,
                haveShownHideRequestMessage = false;        // These two variable are for hide or show message

        String message = "Response";
        byte[] receiveData = new byte[1024];
        byte[] sendData = message.getBytes();
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        while (true){
            UDPSocket.receive(receivePacket);
            int port = receivePacket.getPort();
            InetAddress IPAdress = receivePacket.getAddress();
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAdress, port);

            if (userInput && !haveShownHideMessage) haveShownHideMessage = true;
            if (requestFileMessage && !haveShownHideRequestMessage){
                System.out.println("Ping messages are hide\n");
                haveShownHideRequestMessage = true;
            }
            if (port != firstSuccessorPort && port != secondSuccessorPort){
//                When receieving a request message, send a message back

                UDPSocket.send(sendPacket);
                int peer = port - base;
                if (!userInput && !requestFileMessage)
                    System.out.println("A ping request message was received from Peer " + peer + ".");
            }
            if (port == firstSuccessorPort){
                if (!userInput && !requestFileMessage)
                    System.out.println("A ping response message was received from Peer " + firstSuccessor + ".");
                firstPingBack = true;
            }
            if (port == secondSuccessorPort){
                if (!userInput && !requestFileMessage)
                    System.out.println("A ping response message was received from Peer " + secondSuccessor + ".");
                secondPingBack = true;
            }
        }
    }
    public static int stringToHash(String fileName){
//        Calculate the hah value from a file name

        int w = Integer.parseInt(String.valueOf(fileName.charAt(0)));
        int x = Integer.parseInt(String.valueOf(fileName.charAt(1)));
        int y = Integer.parseInt(String.valueOf(fileName.charAt(2)));
        int z = Integer.parseInt(String.valueOf(fileName.charAt(3)));
        int hash = (w * 1000 + x * 100 + y * 10 + z + 1) % 256;
        return hash;
    }
    public static void TCPServer() throws Exception{
        boolean forwardMessage;                     // Use for control message shown or not, if a peer has forwarded a message, Ping messages will be hide
        String clientMessage;                       // Content of received packet
        ServerSocket welcomeServer = new ServerSocket(peerPort);
        while (true){
            Socket connectionSocket = welcomeServer.accept();
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            clientMessage = inFromClient.readLine();
            String status = clientMessage.split(" ")[0];

            if (status.equals("quit")){
//                It is a "quit" message

                if (clientMessage.split(" ")[1].equals("ok")){
//                    If this message is a response to sender's quit message
//                    The format is
//                    quit ok + peer identity

                    count++;
                    if (count == 2) {
                        System.out.println("I can quit now.");
                        return;
                    }
                } else {
//                    If not, it is a quit request message
//                    THe format is
//                    quit + the quit peer + its first successor + its second successor

                    userInput = false;
                    requestFileMessage = false;
                    int quitPeer = Integer.parseInt(clientMessage.split(" ")[1]);
                    if (quitPeer != firstSuccessor && quitPeer != secondSuccessor){
//                        This quit message will only be sent to predecessor, other peers will just forward this message

                        TCPClient(clientMessage + "\n", firstSuccessorPort);
                    } else {
//                        If this message is sent to a predecessor
//                        Firstly changing successor

                        if (quitPeer != firstSuccessor) TCPClient(clientMessage + "\n", firstSuccessorPort);
                        int newFirstSuccessor = Integer.parseInt(clientMessage.split(" ")[2]);
                        int newSecondSuccessor = Integer.parseInt(clientMessage.split(" ")[3]);
                        if (firstSuccessor == quitPeer){
                            firstSuccessor = (secondSuccessor == newFirstSuccessor) ? newSecondSuccessor : newFirstSuccessor;
                            int tmp = firstSuccessor;
                            firstSuccessor = secondSuccessor;
                            secondSuccessor = tmp;
                        }
                        else secondSuccessor = (firstSuccessor == newFirstSuccessor) ? newSecondSuccessor : newFirstSuccessor;
                        firstSuccessorPort = base + firstSuccessor;
                        secondSuccessorPort = base + secondSuccessor;
//                      Then send a response to quit peer

                        TCPClient(("quit ok " + peerIdentity + "\n"), quitPeer + base);
                        System.out.println(quitPeer + " " + peerIdentity);
                        System.out.println("Peer " + quitPeer + " will depart from the network.");
                        System.out.println("My first successor is now peer " + firstSuccessor);
                        System.out.println("My second successor is now peer " + secondSuccessor);
                    }
                }
            }
            else if (status.equals("ask")){
//                This message is to query successors
//                The format is
//                ask + querying peer's port + querying successor (first / second)

                requestFileMessage = false;
                int reponseToPort = Integer.parseInt(clientMessage.split(" ")[1]);
                String whichSuccessor = clientMessage.split(" ")[2];

                if (whichSuccessor.equals("first")) TCPClient("reponseAsk firstSuccessor " + firstSuccessor + "\n", reponseToPort);
                else TCPClient("reponseAsk secondSuccessor " + secondSuccessor + "\n", reponseToPort);
            }
            else if (status.equals("reponseAsk")){
//                This is a response to querying successors' message
//                The format is
//                resonseAsk + (first successor / second successor) + successor port

                requestFileMessage = false;
                String changedSuccessor = clientMessage.split(" ")[1];
                int newSuccessor = Integer.parseInt(clientMessage.split(" ")[2]);
                if (changedSuccessor.equals("firstSuccessor")){
//                    Update its first successor

                    firstSuccessor = secondSuccessor;
                    secondSuccessor = newSuccessor;
                    firstSuccessorPort = firstSuccessor + base;
                    secondSuccessorPort = secondSuccessor + base;
                    System.out.println("My first successor is now peer " + firstSuccessor);
                    System.out.println("My second successor is now peer " + secondSuccessor);
                } else {
//                    Otherwise, update its second successor

                    secondSuccessor = newSuccessor;
                    secondSuccessorPort = secondSuccessor + base;
                    System.out.println("My first successor is now peer " + firstSuccessor);
                    System.out.println("My second successor is now peer " + secondSuccessor);
                }
            }
            else {
//                it is a file request message

                requestFileMessage = true;
                String fileName = clientMessage.split(" ")[1];
                int hashValue = Integer.parseInt(clientMessage.split(" ")[2]);
                int sourcePeerIdentity = Integer.parseInt(clientMessage.split(" ")[3]);
                int clientPeerIdentity = Integer.parseInt(clientMessage.split(" ")[4]);

                if (status.equals("Response")){
//                    If it is a response to a request
//                    The format is
//                    response + file name + file name's hash value + request peer identity + forwarding peer identity

                    System.out.println("Received a response message from peer " + clientPeerIdentity + ", which has the file " + fileName);
                }
                else{
//                    It is a request message
//                    THe format is
//                    response + file name + file name's hash value + request peer identity + forwarding peer identity

                    if (hashValue == peerIdentity
                            || (peerIdentity < hashValue && firstSuccessor < hashValue && peerIdentity > firstSuccessor)
                            || (peerIdentity < hashValue && firstSuccessor > hashValue))
                        forwardMessage = false;
                    else forwardMessage = true;
                    if (forwardMessage){
                        TCPClient(("Request " + fileName + " " + hashValue + " " + sourcePeerIdentity + " " + peerIdentity + "\n"), firstSuccessorPort);
                        System.out.println("File " + fileName + " is not stored here.\nFile request message has been forwarded to my successor.");
                    }else {
                        TCPClient(("Response " + fileName + " " + hashValue + " " + sourcePeerIdentity + " " + peerIdentity + "\n"), (base + sourcePeerIdentity));
                        System.out.println("File " + fileName + " is stored here.\nA response message, destined for peer " + sourcePeerIdentity + ", has been sent.");
                    }
                }
            }
        }
    }
    public static void TCPClient(String message, int destPort){
//        Send a TCP message

        try {
            Socket clientSocket = new Socket("localhost", destPort);
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            outToServer.writeBytes(message);
        } catch (Exception e){
            System.out.println("----Socket Create failed-----");
        }
    }

    public static void main(String[] args) throws IOException{
//        The main thread, to initialize some variables and to open 6 threads

        peerIdentity = Integer.parseInt(args[0]);
        firstSuccessor = Integer.parseInt(args[1]);
        secondSuccessor = Integer.parseInt(args[2]);
        base = 50000;
        peerPort = base + peerIdentity;
        firstSuccessorPort = base + firstSuccessor;
        secondSuccessorPort = base + secondSuccessor;
        UDPSocket = new DatagramSocket(peerPort);

        // Step 1 and 2, peers ping each other
        final Thread newServer = new Thread(){
            public void run(){
//                The UDP Server thread, used to receiving UDP message

                try {UDPServer();}
                catch (Exception e){return;}
            }
        };
        final Thread firstClient = new Thread(){
            public void run(){
//                UDP client thread, used to send messages to the first successor every 3 seconds

                while (true){
                    try {
                        DatagramPacket packet = UDPClient(firstSuccessorPort);
                        UDPSocket.send(packet);
                        Thread.currentThread().sleep(3000);
                    } catch (Exception e){return;}
                }
            }
        };
        final Thread secondClient = new Thread(){
            public void run(){
//                UDP client thread, used to send messages to the second successor every 3 seconds

                while (true){
                    try {
                        DatagramPacket packet = UDPClient(secondSuccessorPort);
                        UDPSocket.send(packet);
                        Thread.currentThread().sleep(3000);
                    } catch (Exception e){return;}
                }
            }
        };
        final Thread check = new Thread(){
            public void run(){
//                This thread monitor the server thread, if there are no responses from a specified successor over 12 seconds,
//                this peer will be regarded as died
//                When a peer died, it will send a ask message over TCP to query the next successor

                while (true){
                    try {
                        Thread.currentThread().sleep(12000);
                    } catch (InterruptedException e){return;}

                    if (firstPingBack){
                        firstPingBack = false;
                    }else if (!firstPingBack){
                        String changeSuccessor = "ask " + peerPort + " first\n";
                        TCPClient(changeSuccessor, secondSuccessorPort);
                        firstPingBack = false;
                        System.out.println("Peer " + firstSuccessor + " is no longer alive.");
                        // first node shut down
                    }
                    if (secondPingBack){
                        secondPingBack = false;
                    }
                    else if (!secondPingBack){
                        String changeSuccessor = "ask " + peerPort + " second\n";
                        TCPClient(changeSuccessor, firstSuccessorPort);
                        secondPingBack = false;
                        System.out.println("Peer " + secondSuccessor + " is no longer alive.");
                        // second node shut down
                    }
                }
            }
        };
        final Thread requestFileServer = new Thread(){
            public void run(){
//                The TCP server, used to receive TCP messages

                try {
                    TCPServer();
                } catch (Exception e){return;}
            }
        };
        final Thread waitInput = new Thread(){
            public void run(){
//                This thread is to wait users' input
//                When the Enter key is pressed, all messages will be hide. Those messages will be displayed by pressing Enter key again

                Scanner sc = new Scanner(System.in);
                String inputString, sendMessage;
                while (true){
                    inputString = sc.nextLine();
                    if (inputString.equals("")){
                        if (userInput || requestFileMessage){
                            userInput = false;
                            requestFileMessage = false;
                            System.out.println("You have pressed enter key, ping messages are displayed again\n");
                            continue;
                        }
                        if (!userInput){
                            userInput = true;
                            System.out.println("You have pressed enter key, ping messages are hide\n");
                            continue;
                        }
                    }
                    else {
                        if (inputString.equals("quit")){
//                            When user typing quit, then send a quit request to its successor
//                            If the successor is not the target peer, it will forward this message to its successor iteratively

                            sendMessage = "quit " + peerIdentity + " " + firstSuccessor + " " + secondSuccessor + "\n";
                            try {TCPClient(sendMessage, firstSuccessorPort);} catch (Exception e){return;}
                            try {Thread.currentThread().sleep(3000);} catch (InterruptedException e){return;}
//                          Then wait 10 seconds, if received 2 response messages, it is able to interrupt all threads
                            firstClient.interrupt();
                            secondClient.interrupt();
                            check.interrupt();
                            if (count == 2){
                                newServer.interrupt();
                                requestFileServer.interrupt();
                                return;
                            }
                        } else {
                            String command = inputString.split(" ")[0];
                            if (!command.equals("request")) continue;
//                            This thread will only accept command beginning with "request" and "quit"
//                            All other commands will be ignored

                            String fileName = inputString.split(" ")[1];
                            int hashValue = stringToHash(fileName);
                            sendMessage = "request " + fileName + " " + hashValue + " " + peerIdentity + " " + peerIdentity + "\n";
//                            Noticed that the variable peerIdentity have shown twice, it is because the request peer and the forward peer are the same peer
                            try {TCPClient(sendMessage, firstSuccessorPort);} catch (Exception e){return;}
                        }
                    }
                }
            }
        };
        newServer.start();
        firstClient.start();
        secondClient.start();
        check.start();
        requestFileServer.start();
        waitInput.start();
    }
}
