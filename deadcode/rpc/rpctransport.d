module deadcode.rpc.rpctransport;

import deadcode.rpc.rpc;

import core.thread : Fiber;
import std.algorithm;
import std.range;
import std.socket;

private 
{
    version (Windows)
        socket_t INVALID_SOCKET = socket_t.INVALID_SOCKET;
    else 
        socket_t INVALID_SOCKET = socket_t.init;
}

class RPCTransport
{
    enum ReceiveError = Socket.ERROR;

    package
    {
        RPCLoop _loop;
        Socket _socket;
    }

    this(RPCLoop l, Socket s)
    {
        _loop = l;
        _socket = s;
    }

    final ptrdiff_t send(const ubyte[] data)
    {
        return _loop.send(_socket, data);
    }

    final ptrdiff_t receive(ubyte[] data)
    {
        return _loop.receive(_socket, data);
    }

    final void kill()
    {
        _loop.kill(_socket);
    }

    override string toString()
    {
        return _socket.localAddress.toString() ~ " -> " ~ _socket.remoteAddress.toString(); 
    }
}

class RPCLoop
{
    import deadcode.core.signals;
        
    private
    {
        Socket _sock;
		RPCSocketBuffer _sockBuffer;
        SocketSet _socketSet;
        enum MAX_CONNECTIONS = 40;
        struct Client
        {
            Fiber fiber;
            RPC rpc;
            Socket socket;
        }
        
        Client[] _clients;
    }

	@property RPCSocketBuffer listeningSocketBuffer()
	{
		return _sockBuffer;
	}

    mixin Signal!() onConnectionsExceeded;
    
    // (reason if error)
    mixin Signal!(RPC, string) onDisconnected;
    mixin Signal!(RPC, bool) onConnected;

    this()
    {
        _socketSet = new SocketSet(MAX_CONNECTIONS + 1);
		_sockBuffer = new RPCSocketBuffer(INVALID_SOCKET, 1024);
    }

    final void connect(string ip, ushort port)
    {
        auto s = new TcpSocket();
        auto addr = new InternetAddress(ip, port);
        s.connect(addr);
        registerConnection(s, false);
    }

    final void listen(ushort port)
    {
        assert(_sock is null);
        _sock = new TcpSocket();
        assert(_sock.isAlive);
        _sock.blocking = true;
        Linger k;
        k.on = 0; 
        k.time = 2; // 2 seconds
        _sock.setOption(SocketOptionLevel.SOCKET, SocketOption.LINGER, k);
		auto ia = new InternetAddress(port);
        _sock.bind(ia);
        _sock.listen(10);
		_sockBuffer.handle = _sock.handle;
    }

    final void stopListening()
    {
        if (_sock !is null)
        {
            _sock.close();
            _sock = null;
			_sockBuffer.handle = INVALID_SOCKET;
        }
    }

    final ptrdiff_t send(Socket socket, const ubyte[] data)
    {
        import std.stdio;

        foreach (v; _clients)
        {
            if (v.socket is socket)
            {
                return v.socket.send(data);
            }
        }
        return Socket.ERROR;
    }

    final ptrdiff_t receive(Socket socket, ubyte[] data)
    {
        foreach (v; _clients)
        {
            if (v.socket is socket)
            {
                return v.socket.receive(data);
            }
        }
        return Socket.ERROR;
    }

    final void kill(Socket socket)
    {
        size_t idx = -1;
        foreach (i, v; _clients)
        {
            if (v.socket is socket)
            {
                idx = i;
                break;
            }
        }
        
        if (idx != -1)
        {
            if (_clients[idx].rpc.isAlive)
            {
                _clients[idx].rpc.kill();
            }
            else
            {
                import std.algorithm : remove;
                _clients[idx].socket.close();
                _clients = _clients.remove(idx);
            }
        }
    }

    final int select()
    {
        _socketSet.reset();
        
        if (!addSockets(_socketSet))
            return 0;

       
        //string me = _sock !is null ? _sock.localAddress.toString() : "not listening";
        //writeln("Selecting ", me);
        int selectResult = Socket.select(_socketSet, null, null);

        readAndProcessSockets(_socketSet);

        return selectResult;
    }

	// Read data from socketset and make any rpc calls/handle rpc returns for
	// associated RPCs.
    // See processMessages() for a version for pushing data.
	final void readAndProcessSockets(SocketSet socketSet)
    {
        import std.stdio;
        foreach (size_t i, client; _clients)
        {
            if (socketSet.isSet(client.socket))
            {
                auto recvLength = client.rpc.receiveMessage();
                if (recvLength == Socket.ERROR)
                {
                    onDisconnected.emit(client.rpc, "Error receiving message");
                }
                else if (recvLength == 0)
                {
                    if (client.rpc.isAlive)
                    {
                        //debug writeln("Client disconnected");
                        onDisconnected.emit(client.rpc, "Remote end disconnected before receiving message finished");
                    }
                    else
                    {
                        //debug writeln("Client disconnected by kill");
                        onDisconnected.emit(client.rpc, "Disconnect caused by rpc.kill()");
                    }
                }
            }
        }

        if (_sock !is null && socketSet.isSet(_sock))
        {
            Socket clientSocket = null;
            scope (failure)
                if (clientSocket !is null)
                    clientSocket.close();
    
			clientSocket = _sock.accept();
    
			//
			bool al = clientSocket.isAlive;
	
            if (_clients.length == MAX_CONNECTIONS)
                onConnectionsExceeded.emit();
            else 
                registerConnection(clientSocket, true);
        }
    }

	// Read data from socketMessages and make any rpc calls/handle rpc returns for
	// associated RPCs.
    // See processSockets() for a version for pushing data.
	final void processSockets(socket_t[] socketSet)
    {
        import std.stdio;
		foreach (size_t i, s; socketSet)
		{
			auto r = _clients.find!((a, b) => a.socket.handle == b)(s);
            if (!r.empty)
            {
				auto client = r.front;
                if (client.rpc.inBuffer.buffer.used == 0)
                {
                    if (client.rpc.isAlive)
                    {
                        //debug writeln("Client disconnected");
                        onDisconnected.emit(client.rpc, "Remote end disconnected before receiving message finished");
						client.rpc.kill();
                    }
                    else
                    {
                        //debug writeln("Client disconnected by kill");
                        onDisconnected.emit(client.rpc, "Disconnect caused by rpc.kill()");
                    }
                }
				else
				{
					client.rpc.processMessageBuffer();
				}
            }
        }

        if (_sock !is null && socketSet.canFind(_sock.handle))
        {
            Socket clientSocket = null;
            scope (failure)
                if (clientSocket !is null)
                    clientSocket.close();

			clientSocket = *(cast(Socket*)listeningSocketBuffer.buffer.data);
			listeningSocketBuffer.buffer.used = 0;

			bool al = clientSocket.isAlive;
			socket_t as1 = _sock.handle;
			socket_t as = clientSocket.handle;

            if (_clients.length == MAX_CONNECTIONS)
                onConnectionsExceeded.emit();
            else 
                registerConnection(clientSocket, true);
        }
    }


    bool addSockets(T)(ref T outputSockets)
    {
        import std.algorithm : remove;
        
        for (size_t i = 0; i < _clients.length; ++i)
        {
            auto client = _clients[i];

            if (!client.rpc.isAlive || !client.socket.isAlive)
            {
                _clients = _clients.remove(i);
                i--;
            }
            else if (client.rpc.waitForReceive)
            {  
                static if ( is(T == SocketSet) )
                    outputSockets.add(client.socket);
                else
                    outputSockets ~= client.rpc.inBuffer;
            }
        }

        if (_sock !is null)
        {
            static if ( is(T == SocketSet) )
                outputSockets.add(_sock); // listening socket
            else
                outputSockets ~= _sockBuffer;
        }
        else if (_clients.length == 0)
            return false;
        return true;
    }

    // incoming is false in case this transport initiated the connection
    private final void registerConnection(Socket s, bool incoming)
    {
        auto rpc = new RPC(new RPCTransport(this, s));
        auto f = new Fiber( () {
            onConnected.emit(rpc, incoming);
        });
        _clients ~= Client(f, rpc, s);
        f.call();
    }

    private final Socket getSocketForRPC(RPC rpc)
    {
        foreach (c; _clients)
        {
            if (c.rpc is rpc)
                return c.socket;
        }
        return null;
    }

}

version (unittest)
{
    interface API
    {
        void callDelegate();
        string appendBar(string txt);
    }

    class APIService : API
    {
        void delegate() _dg;

        this(void delegate() dg = null)
        {
            _dg = dg;
        }

        final void callDelegate()
        {
            if (_dg !is null)
                _dg();
        }

        final string appendBar(string txt)
        {
            return txt ~ "bar";
        }
    }
}

unittest
{
    import std.concurrency;
    import std.stdio;

    enum port = 54321u;

    enum clientAPIid = "clientService";
    enum serverAPIid = "serverService";

    static void runClient(int apiID)
    {
        auto client = new RPCLoop;
        bool running = true;

        RPC myRPC = null;

        client.onConnected.connectTo( (RPC rpc, bool incoming) {
            writeln("client: Connect ok " ~ rpc.transport.toString());
            myRPC = rpc;
            rpc.publish(new APIService(null), clientAPIid);
            API api = rpc.createReference!API(serverAPIid);
            writeln(api.appendBar("Hello from client " ~ rpc.transport.toString()));
            if (apiID == 1000)
                api.callDelegate();
            rpc.kill();
        });

        client.onDisconnected.connectTo( (RPC rpc, string errorMessage) {
            writeln("client: disconnected: " ~ errorMessage);
        });

        client.onConnectionsExceeded.connectTo( () {
            writeln("client: Connections exceeded");
        });

        client.connect("localhost", port);
        
    //    client.connect("localhost", port);
        
        while (myRPC.isAlive)
            client.select();

    }

    static void runServer()
    {
        auto server = new RPCLoop;
        server.listen(port);
        API api = null;
        bool running = true;

        auto service = new APIService( () {
            writeln("server: client asked us to stop");
            running = false; 
        });

        server.onConnected.connectTo( (RPC rpc, bool incoming) {
            writeln("server: Client connected " ~ rpc.transport.toString());
            
            rpc.publish(service, serverAPIid);
            api = rpc.createReference!API(clientAPIid);
            writeln(api.appendBar("Hello from server " ~ rpc.transport.toString()));
        });

        server.onDisconnected.connectTo( (RPC rpc, string errorMessage) {
            writeln("server: Client disconnected: " ~ errorMessage);
        });

        server.onConnectionsExceeded.connectTo( () {
            writeln("server: Connections exceeded");
        });

        while (running)
            server.select();
    }

    int[] apiIDs = [  1000 ]; 
    
    foreach (ref id; apiIDs)
        spawn(&runClient, id);

    runServer();
}
