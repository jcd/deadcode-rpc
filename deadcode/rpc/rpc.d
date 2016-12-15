module deadcode.rpc.rpc;

import msgpack;

import core.thread : Fiber;
import std.conv;
import std.stdio;
import std.traits;
import std.typecons;
import std.typetuple;

import deadcode.core.future;
import deadcode.rpc.rpcproxy : RPCProxy, getAllMethods, generateMethodImplementations, CreateFunction, JoinStrings, ParameterTuple;
import deadcode.rpc.rpctransport : RPCTransport;

alias CallID = string;
alias RPCAsyncCallback = Promise!Unpacker;

class RPC
{
    enum CallStatus : byte
    {
        Success = 1,
        Exception = 2,
        Error = 3
    }

    import std.array;
    RPCTransport _transport; // null when this RPC is killed
    ubyte[64*1024] inBuffer;
    Appender!(ubyte[]) outBuffer;
    
    IRPCService[string] _services;

    private struct RPCCallbackInfo
    {
        RPCAsyncCallback promise;
        Fiber resumeFiber;
    }

    RPCCallbackInfo[CallID] rpcCallbacks;
    ulong sNextCallID = 1;

    @property bool waitForReceive() 
    {
        // We want data from network if we are either providing a service
        // or we have made a RPC to remote and is waiting for response
        return _services.length != 0 || rpcCallbacks.length != 0;
    }

    debug @property RPCTransport transport()
    {
        return _transport;
    }

    @property bool isAlive()
    {
        return _transport !is null;
    }

    this(RPCTransport transport)
    {
        _transport = transport;
        outBuffer = appender!(ubyte[]);
    }

    void kill()
    {
        if (isAlive)
        {
            auto t = _transport;
            _transport = null;
            _services = null;
            Exception e = new Exception("Aborting RPC because of rpc.kill()");
            foreach (k,v; rpcCallbacks)
                remoteCallAbort(k, e);
            t.kill();
        }
    }

    string newCallID()
    {
        return (sNextCallID++).to!string;
    }

    void registerCallID(CallID callID, RPCAsyncCallback promise, Fiber resumeFiber)
    {
        rpcCallbacks[callID] = RPCCallbackInfo(promise, resumeFiber);
    }

    void sendMessage()
    {
        assert(isAlive);
        ubyte[] packedData = outBuffer.data;
        _transport.send(encodeLen(packedData.length));
        _transport.send(packedData);
    }

    ptrdiff_t receiveMessage()
    {
        assert(isAlive);
        auto recvLength = _transport.receive(inBuffer[0..4]);
        if (recvLength == RPCTransport.ReceiveError || recvLength == 0)
        {
            _transport.kill();
            return recvLength;
        }

        auto l = decodeLen(inBuffer);
        auto recvLength2 = _transport.receive(inBuffer[0..l]);
        if (recvLength2 == RPCTransport.ReceiveError || recvLength2 == 0)
        {
            _transport.kill();
            return recvLength2;
        }

        recvLength += recvLength2;

        auto unpacker = Unpacker(inBuffer[0..l]);
        CallID callID;
        unpacker.unpack(callID);
        bool isRequest;
        unpacker.unpack(isRequest);
        
        if (isRequest)
            incomingCall(callID, unpacker);
        else
            remoteCallReturned(callID, unpacker);
        
        return recvLength;
    }

    private void remoteCallReturned(CallID callID, Unpacker unpacker)
    {
        auto info = callID in rpcCallbacks;
        rpcCallbacks.remove(callID);
        if (info.promise !is null)
            info.promise.setValue(unpacker);
        if (info.resumeFiber !is null)
            info.resumeFiber.call();
    }

    private void remoteCallAbort(CallID callID, Exception ex)
    {
        auto info = callID in rpcCallbacks;
        rpcCallbacks.remove(callID);
        if (info.promise !is null)
            info.promise.setException(ex);
        if (info.resumeFiber !is null)
            info.resumeFiber.call();
    }

    // TODO: Figure out if Unpacker keeps are reference to in incoming buffer and make a copy in that case
    private void incomingCall(CallID callID, Unpacker unpacker)
    {
        //if (rpcCallbacks.length != 0)
        //{
            auto f = new Fiber( () {
                incomingCallNewFiber(callID, unpacker);
            });
            f.call();
        //}
        //else
        //{
        //    incomingCallNewFiber(callID, unpacker);
        //}
    }
    
    private void incomingCallNewFiber(CallID callID, Unpacker unpacker)
    {
        string objectType;
        string objectID;
        unpacker.unpack(objectType, objectID);
        ubyte[] res = pack(callID) ~ pack(false); // response

        IRPCService* service = objectID in _services;
        if (service is null)
        {
            // Error cannot call non-existing service
            res ~= pack(CallStatus.Error) ~ pack("Service with id '" ~ callID ~ "' does not exist on host");
        }
        else
        {
            try
            {
                res ~= pack(CallStatus.Success);
                service.execute(unpacker, res);
            }
            catch (Exception e)
            {
                debug writeln("incomingCallNewFiber caught " ~ e.toString());
                res ~= pack(CallStatus.Exception) ~ pack(e.toString());
            }
        }

        uint len = cast(uint)res.length;
        ubyte* lenubyte = cast(ubyte*)&len;
        _transport.send(lenubyte[0..4]);
        _transport.send(res);
    }

    RPCProxy!I create(I)(Unpacker unpacker) if (is (I == interface))
    {
        string id;
        unpacker.unpack(id);
        auto i = new RPCProxy!I(this, pack(id));
        return i;
    }

    RPCProxy!I createReference(I)(string id) if (is (I == interface))
    {
        //pragma(msg, I);
        assert(isAlive);
        auto i = new RPCProxy!I(this, pack(id));
        return i;
    }

    auto publish(T)(T service, string id)
    {
        //pragma(msg, T);
        alias Interfaces = InterfacesTuple!T;
        static assert(Interfaces.length == 1, "RPC.publish can only publish classes deriving exactly one interface");
        return publish!(Interfaces[0], T)(service, id);
    }

    RPCService!(Interface) publish(Interface, T : Interface)(T service, string serviceID) if (is(Interface == interface))
    {
        // pragma(msg, Interface, T);
        assert(isAlive);
        auto i = new RPCService!(Interface)(this, service, pack(serviceID));
        _services[serviceID] = i;
        return i;
    }

    private ubyte[] encodeLen(uint l)
    {
        static uint r;
        r = l;
        ubyte* lenubyte = cast(ubyte*)&r;
        return lenubyte[0..4];
    }

    private uint decodeLen(ubyte[] b)
    {
        return *(cast(uint*)b.ptr);
    }
}

private interface IRPCService
{
    @property ubyte[] packedID();
    @property Object obj();
    void execute(Unpacker unpacker, ref ubyte[] resultData);
}

private template paramTypesToUnpackTypes(Types...)
{
    static if (Types.length == 0)
    {
        alias paramTypesToUnpackTypes = AliasSeq!();
    }
    else static if (Types.length == 1)
    {
        static if (is(Types[0] == interface))
            alias paramTypesToUnpackTypes = string;
        else
            alias paramTypesToUnpackTypes = Types[0];
    }
    else
    {
        alias paramTypesToUnpackTypes = AliasSeq!(paramTypesToUnpackTypes!(Types[0]), Types[1..$]);
    }
}

class RPCService(I) : IRPCService, I
{
    private 
    {
        RPC _rpc;
        Object _obj;
        ubyte[] _id;
    }

    this(RPC rpc, Object o, ubyte[] id)
    {
        _rpc = rpc;
        _obj = o;
        _id = id;
    }
    
    @property Object obj()
    {
        return _obj;
    }

    @property ubyte[] packedID() { return _id; }

    // Get all members of interface I and generate a method body that simply 
    // forwards as a rpc call.
    alias allMethods = getAllMethods!I;

    enum CallSuperMethodMixin = q{
        import std.array;
        import std.traits;
        import std.typetuple;
        alias Func = Identity!(%s);
        enum Name = __FUNCTION__.split(".")[$-1];
        alias ArgsIdents = ParameterIdentifierTuple!Func;

        static if (ArgsIdents.length == 0)
            mixin("return super." ~ Name ~ "()";);
        else static if (ArgsIdents.length == 1)
            mixin("return super." ~ Name ~ "(" ~ ArgsIdents[0] ~ ")";);
        else static if (ArgsIdents.length == 2)
            mixin("return super." ~ Name ~ "(" ~ ArgsIdents[0] ~ "," ~ ArgsIdents[1] ~ ")";);
        else
        {
            pragma(msg, "Error: add support for more arguments in CallSuperMethodMixin. " );
        }
    };

    enum code = generateMethodImplementations!allMethods(q{throw new Exception("%s");});
    mixin(code);

    private Tuple!Params lookupObjectParams(UnpackedParams, Params...)(UnpackedParams up)
    {
        Tuple!Params params;

        foreach (i, ParamType; AliasSeq!Params)
        {
            static if (is(ParamType == interface))
            {
                auto service = up[i] in _rpc._services;
                if (service is null)
                    params[i] = _rpc.createReference!ParamType(up[i]);
                else
                    params[i] = cast(ParamType) *service;
            }
            else
            {
                params[i] = up[i];
            }
        }
        return params;
    }

    override void execute(Unpacker unpacker, ref ubyte[] resultData)
    {
        I obj = cast(I)_obj;
        import std.range;

        string methodName;
        unpacker.unpack(methodName);

        foreach (Method; allMethods)
        {
            if (__traits(identifier, Method) == methodName)
            {
                enum MethodName = __traits(identifier, Method);
                alias ParamTypes = Parameters!(Method);
                alias ParamUnpackTypes = paramTypesToUnpackTypes!ParamTypes;
                // alias ParamIdents = ParameterIdentifierTuple!(Method);
                // enum ParamCount = ParamTypes.length;
                alias Returning = ReturnType!(Method);
                // alias Typ = genParams!(ParamTypes);
                
                version (OutputRPCAPI)
                {
                    //pragma(msg, "paramtypes ", ParamTypes);a
                    pragma(msg, RPCAPIPREFIX, "    ", Returning.stringof, " ", m.name, "(", Typ!(ParamIdents), ") { mixin(RPCProxyMethodMixin); }");
                }

                // pragma(msg, RPCAPIPREFIX, "    ", Returning.stringof, " ", m.name, "(");
                //foreach (i, ident; ParamIdents)
                //{
                //    pragma(msg, RPCAPIPREFIX, "        ", i == 0 ? "" : ", ", ParamTypes[i].stringof, " ", ident);
                //}
                //pragma(msg, RPCAPIPREFIX, "        ) { mixin(RPCProxyMethodMixin); }");

                static if (ParamTypes.length == 0)
                {
                    static if (is(Returning : void))
                        __traits(getMember, obj, MethodName)();
                    else
                        auto result = __traits(getMember, obj, MethodName)();
                }
                else static if (ParamTypes.length == 1)
                {
                    ParamUnpackTypes unpackParams;
                    unpacker.unpack(unpackParams);
                    auto up  = tuple(unpackParams);
                    Tuple!ParamTypes params = lookupObjectParams!(typeof(up),ParamTypes)(up);

                    static if (is(Returning : void))
                        __traits(getMember, obj, MethodName)(params[0]);
                    else
                        auto result = __traits(getMember, obj, MethodName)(params[0]);
                }
                else static if (ParamTypes.length == 2)
                {
                    ParamUnpackTypes unpackParams;
                    unpacker.unpack(unpackParams);
                    auto up  = tuple(unpackParams);
                    Tuple!ParamTypes params = lookupObjectParams!(typeof(up),ParamTypes)(up);
                    static if (is(Returning : void))
                        __traits(getMember, obj, MethodName)(params[0], params[1]);
                    else
                        auto result = __traits(getMember, obj,MethodName)(params[0], params[1]);
                }
                else static if (ParamTypes.length == 3)
                {
                    ParamUnpackTypes unpackParams;
                    unpacker.unpack(unpackParams);
                    auto up  = tuple(unpackParams);
                    Tuple!ParamTypes params = lookupObjectParams!(typeof(up), ParamTypes)(up);
                    static if (is(Returning : void))
                        __traits(getMember, obj, MethodName)(params[0], params[1], params[2]);
                    else
                        auto result = __traits(getMember, obj, MethodName)(params[0], params[1], params[2]);
                }

                // Serialize result. also convert know object types to ids.

                static if (!is(Returning : void))
                {
                    static if (is(Returning == interface))
                    {
                        //if (hasMember!(Returning, "id"))
                        //{
                        ubyte[] id = pack("");
                        foreach (_service; _rpc._services)
                        {
                            // auto returnObj = cast(typeof(result))_service._obj;
                            if ( result == _service.obj)
                            {
                                id = _service.packedID;
                                break;
                            }
                        }
                        resultData ~= id;
                        //}
                        //else
                        //{
                        //    pragma(msg, "Trying to RPC return class with no id property: ", Returning.stringof);
                        //}
                    }
                    else
                    {
                        resultData ~= pack(result);
                    }
                }
            }
        }
        version (OutputRPCAPI)
        {
            pragma(msg, RPCAPIPREFIX, "}");
            pragma(msg, RPCAPIPREFIX); // newline
        }
    }
}
