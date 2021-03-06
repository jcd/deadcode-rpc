/**
    A basic RPC library for doing remote calls over tcp.
*/
module deadcode.rpc;

public import deadcode.rpc.rpc;
public import deadcode.rpc.rpcproxy;
public import deadcode.rpc.rpctransport;

/**
---
// Interface of an object accessible through RPC
interface API
{
    void callDelegate();
    string appendBar(string txt);
}

// The actual implementation of the API as running on the remote side of the RPC
class APIService : API
{
    final string appendBar(string txt)
    {
        return txt ~ "bar";
    }
}
---
*/
enum Example = 1;

///
unittest
{
    import std.concurrency;
    import std.stdio;

    enum port = 54322u;
    enum serviceID = "myService";

    static void runClient()
    {
        auto client = new RPCLoop;

        client.onConnected.connectTo( (RPC rpc, bool incoming) {
            API api = rpc.createReference!API(serviceID);
            writeln(api.appendBar("Hello from client "));
            rpc.kill();
        });

        client.connect("localhost", port);

        while (client.select() != 0) {}
            
    }

    static void runServer()
    {
        auto server = new RPCLoop;
        server.listen(port);
        
        server.onConnected.connectTo( (RPC rpc, bool incoming) {
            rpc.publish(new APIService(), serviceID);
            server.stopListening();
        });

        while (server.select() != 0) {}
    }

    spawn(&runClient);
    runServer();
}

///
unittest
{
    import std.concurrency;
    import std.stdio;

    enum port = 54323u;

    static void runClient()
    {
        auto client = new RPCLoop;

        client.onConnected.connectTo( (RPC rpc, bool incoming) {
            writeln("client says: Connect ok " ~ rpc.transport.toString());

            // Expose our local service to the server with an ID of 42
            rpc.publish(new APIService(null), "42");

            // Lets also get a service running on the server that have an ID of 100
            API api = rpc.createReference!API("100");

            // Call remote object
            string resultFromRemoteService = api.appendBar("Hello from client " ~ rpc.transport.toString());
            writeln(resultFromRemoteService);

            // Shutdown the rpc connection
            rpc.kill();
        });

        client.connect("localhost", port);

        while (client.select() != 0) {}

    }

    static void runServer()
    {
        auto server = new RPCLoop;
        server.listen(port);

        API api = null;

        // Service shared between all clients connecting to this server
        auto service = new APIService();

        server.onConnected.connectTo( (RPC rpc, bool incoming) {
            writeln("server says: Client connected " ~ rpc.transport.toString());

            // Expose our local service to the server with an ID of 100
            rpc.publish(service, "100");

            // Lets also get a service running on the client that have an ID of 42
            api = rpc.createReference!API("42");

            // Call remote object
            string resultFromRemoteService = api.appendBar("Hello from server " ~ rpc.transport.toString());
            writeln(resultFromRemoteService);

            // Stop listning for connections
            server.stopListening();
        });

        while (server.select() != 0) {}
    }

    spawn(&runClient);
    runServer();
}


version (unittest)
{
    interface ITestClient
    {
        void f1(ITestClient c);
        void f2(ITestClient c);
        void f3();
    }

    class TestClient : ITestClient
    {
        void f1(ITestClient c)
        {
            c.f2(this);
        }
        
        void f2(ITestClient c)
        {
            c.f3();
        }

        void f3()
        {
            import std.stdio;
            writeln("Got f3");
        }
    }
}

unittest
{
    import std.concurrency;
    import std.stdio;

    enum port = 54324u;
    enum serviceClientID = "myClientService";
    enum serviceServerID = "myServerService";

    static void runClient()
    {
        auto client = new RPCLoop;

        client.onConnected.connectTo( (RPC rpc, bool incoming) {
            auto cl = new TestClient();
            rpc.publish(cl, serviceClientID);
            auto api = rpc.createReference!ITestClient(serviceServerID);
            
            api.f1(cl);

            //writeln(api.appendBar("Hello from client "));
            rpc.kill();
        });

        client.connect("localhost", port);

        while (client.select() != 0) {}
    }

    static void runServer()
    {
        auto server = new RPCLoop;
        server.listen(port);

        server.onConnected.connectTo( (RPC rpc, bool incoming) {
            auto cl = new TestClient();
            rpc.publish(cl, serviceServerID);
            // ITestClient api = rpc.createReference!ITestClient(serviceClientID);
            
            server.stopListening();
        });

        while (server.select() != 0) {}
    }

    writeln("TEST 3");
    spawn(&runClient);
    runServer();
}
