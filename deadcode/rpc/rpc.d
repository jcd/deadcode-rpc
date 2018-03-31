module deadcode.rpc.rpc;

import msgpack;

import core.thread : Fiber;
import std.array;
import std.conv;
import std.stdio;
import std.traits;
import std.typecons;
import std.typetuple;

import deadcode.core.future;
import deadcode.core.signals;
//import deadcode.core.weakref : WeakRef;
import deadcode.rpc.rpccallback;
import deadcode.rpc.rpcproxy : RPCProxy, getAllMethods, generateMethodImplementations, CreateFunction, JoinStrings, ParameterTuple;
import deadcode.rpc.rpctransport : RPCTransport;

import deadcode.util.weakref : WeakRef;

alias CallID = string;
alias RPCAsyncCallback = Promise!Unpacker;

struct RPCBuffer
{
	import core.memory;
	this(size_t initialSize)
	{
		data = cast(ubyte*)pureMalloc(initialSize);
		capacity = initialSize;
		used = 0;
	}
	~this()
	{
		if (data !is null)
		{
			import core.stdc.stdlib : free;
			//pureFree(data);
			free(data);
			data = null;
			used = 0;
			capacity = 0;
		}
	}
	void reserve(size_t sz)
	{
		if (capacity < sz)
		{
			data = cast(ubyte*)pureRealloc(data, sz);
			capacity = sz;
		}
	}
	ubyte* data;
	size_t capacity;
	size_t used;
}

class RPCSocketBuffer
{
	import std.socket;
	this(socket_t _handle, size_t initialSize)
	{
		handle = _handle;
		messageSize = 0;
		buffer = RPCBuffer(initialSize);
	}
	socket_t handle;
	size_t messageSize;
	RPCBuffer buffer;
}

class RPC
{
    enum CallStatus : byte
    {
        Success = 1,
        Exception = 2,
        Error = 3
    }

    import std.array;
    private RPCTransport _transport; // null when this RPC is killed
    RPCSocketBuffer inBuffer;
    Appender!(ubyte[]) outBuffer;
    
    // private Object[string] _publishedServices;      // to keep weakrefs alive for published services 
    package IRPCService[string] _services;  // used by rpcproxy therefore package

    private struct RPCCallbackInfo
    {
        RPCAsyncCallback promise;
        Fiber resumeFiber;
    }

    RPCCallbackInfo[CallID] rpcCallbacks;
    ulong sNextCallID = 1;

    mixin Signal!(RPC) onKilled;

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
		inBuffer =  new RPCSocketBuffer(_transport._socket.handle, 1024);
    }

	string getErrorText()
	{
		return _transport._socket.getErrorText();
	}

    void kill()
    {
        if (isAlive)
        {
            onKilled.emit(this);
            auto t = _transport;
            _transport = null;
            Exception e = new Exception("Aborting RPC because of rpc.kill()");
            foreach (k,v; rpcCallbacks)
                remoteCallAbort(k, e);
            t.kill();
            _services = null;
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

	final void processMessageBuffer()
    {
        assert(isAlive);
		
		if (inBuffer.buffer.used == 0)
			return;

        auto unpacker = Unpacker(inBuffer.buffer.data[0..inBuffer.messageSize]);
		
		inBuffer.buffer.data[0..inBuffer.buffer.used-inBuffer.messageSize] = 
			inBuffer.buffer.data[inBuffer.messageSize..inBuffer.buffer.used];
		inBuffer.buffer.used -= inBuffer.messageSize;
		inBuffer.messageSize = 0;

        CallID callID;
        unpacker.unpack(callID);
        bool isRequest;
        unpacker.unpack(isRequest);

        if (isRequest)
            incomingCall(callID, unpacker);
        else
            remoteCallReturned(callID, unpacker);
	}

    ptrdiff_t receiveMessage()
    {
        assert(isAlive);
        auto recvLength = _transport.receive(inBuffer.buffer.data[0..4]);
        if (recvLength == RPCTransport.ReceiveError || recvLength == 0)
        {
            kill();
            // _transport.kill();
            return recvLength;
        }

        auto l = decodeLen(inBuffer.buffer.data[0..4]);
		inBuffer.buffer.reserve(l);
		inBuffer.buffer.used = l;
		inBuffer.messageSize = l;
        auto recvLength2 = _transport.receive(inBuffer.buffer.data[0..l]);
        if (recvLength2 == RPCTransport.ReceiveError || recvLength2 == 0)
        {
            kill();
            //_transport.kill();
            return recvLength2;
        }

        recvLength += recvLength2;

		processMessageBuffer();
        
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
        if (id is null)
            return null;
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

    RPCProxy!I createReference(I)() if (is (I == interface))
    {
        enum serviceID = fullyQualifiedName!I;
        //pragma(msg, I);
        assert(isAlive);
        auto i = new RPCProxy!I(this, pack(serviceID));
        return i;
    }

    //auto publish(T)(T service, string id)
    //{
    //    //_publishedServices[id] = cast(Object)service;
    //    return weakPublish(service, id);
    //}

    auto publish(T)(T service, string id)
    {
        //pragma(msg, T);
        static if (is (T == interface))
        {
            return publish!(T, T)(service, id);
        }
        else
        {
            alias Interfaces = InterfacesTuple!T;
            static assert(Interfaces.length == 1, "RPC.publish can only publish classes deriving exactly one interface");
            return publish!(Interfaces[0], T)(service, id);
        }
    }

    //RPCService!(Interface) publish(Interface, T : Interface)(T service, string serviceID) if (is(Interface == interface))
    //{
    //    _publishedServices[serviceID] = service;
    //    return weakPublish!(Interface, T)(service, serviceID);
    //}

    RPCService!(Interface) publish(Interface, T : Interface)(T service, string serviceID) if (is(Interface == interface))
    {
        // pragma(msg, Interface, T);
        assert(isAlive);
        auto i = new RPCService!(Interface)(this, service, pack(serviceID));
        _services[serviceID] = i;
        //IRPCService ii = i;
        //_services[serviceID] = new WeakRef!IRPCService(ii);
        return i;
    }

    auto publish(T)(T service)
    {
        //pragma(msg, T);
        static if (is (T == interface))
        {
            return publish!(T, T)(service);
        }
        else
        {
            alias Interfaces = InterfacesTuple!T;
            static assert(Interfaces.length == 1, "RPC.publish can only publish classes deriving exactly one interface");
            return publish!(Interfaces[0], T)(service);
        }
    }

    RPCService!(Interface) publish(Interface, T : Interface)(T service) if (is(Interface == interface))
    {
        enum serviceID = fullyQualifiedName!Interface;
        return publish!(Interface, T)(service, serviceID);
    }

    void unpublish(Object o)
    {
        bool didRemove = true;
        while (didRemove)
        {
            didRemove = false;
            foreach (k, v; _services)
            {
                if (v.obj is o)
                {
                    _services.remove(k);
                    didRemove = true;
                }
            }
        }
        //foreach (k, v; _publishedServices)
        //{
        //    if (v is o)
        //    {
        //        _publishedServices.remove(k);
        //        _services.remove(k);
        //    }
        //}
    }

    void unpublish(string serviceID)
    {
        _services.remove(serviceID);

        //foreach (k, v; _publishedServices)
        //{
        //    if (serviceID == k)
        //    {
        //        _publishedServices.remove(k);
        //        _services.remove(k);
        //    }
        //}
    }

    // Remove weak references to deletes service objects.
    // Service objects that are not explicitly published but
    // automatically published because an object is passed as argument
    // to a function and that object is not already published are
    // only stored as weak object (ie. weak services). As soon as
    // the object is deleted is cannot be used as a service anymore.
    //void cleanupWeakRefs()
    //{
    //    foreach (i; _services.byKeyValue.array)
    //    {
    //        if (i.value.get() is null)
    //        {
    //            assert( (i.key in _publishedServices) is null); 
    //            _services.remove(i.key);
    //        }
    //    }
    //}

    private ubyte[] encodeLen(size_t l)
    {
        static uint r;
        import std.exception;
        enforce(l < uint.max);
        r = cast(uint)l;
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
        else static if (isDelegate!(Types[0]))
            alias paramTypesToUnpackTypes = string;
        else
            alias paramTypesToUnpackTypes = Types[0];
    }
    else
    {
        alias paramTypesToUnpackTypes = AliasSeq!(paramTypesToUnpackTypes!(Types[0]), paramTypesToUnpackTypes!(Types[1..$]));
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

    this(O)(RPC rpc, O o, ubyte[] id) if (is(O == class) || is (O == interface))
    {
        _rpc = rpc;
        _obj = cast(Object)o;
        _id = id;
    }
    
	@property RPC rpc()
	{
		return _rpc;
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
                {
                    if (up[i].empty)
                        params[i] = null;
                    else
                        params[i] = _rpc.createReference!ParamType(up[i]);
                }
                else
                {
                    params[i] = cast(ParamType) service.obj;
                }
            }
			else static if (isDelegate!ParamType)
            {
				// We should have received a RPCProxy!ICallback
                // TODO: Could actually stitch services together here
				//       by using the service.obj which is local to 
				//       use as paramter for a local function. ie. no overhead
				//       connection.
				if (up[i].empty)
				{
                    params[i] = null;
				}
                else
				{
					alias DlgParams = Parameters!ParamType;
					static if (is(ReturnType!ParamType == void))
						alias DlgProxy = ICallback!DlgParams;
					else
						alias DlgProxy = ICallbackReturn!(ReturnType!ParamType, DlgParams);
					auto remoteCallbackObject = _rpc.createReference!DlgProxy(up[i]);
                    params[i] = &remoteCallbackObject.call;
				}
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
        byte argCount;
        unpacker.unpack(argCount);

        static void appendServiceID(E)(E r, RPC __rpc, ref ubyte[] _resultData)
        {
            //if (hasMember!(Returning, "id"))
            //{
            ubyte[] id = null;
            foreach (_service; __rpc._services)
            {
                // auto returnObj = cast(typeof(result))_service._obj;
                auto srv = _service; // .get();
                if ( r == srv.obj)
                {
                    id = srv.packedID;
                    break;
                }
            }

            if (id.empty)
            {
                import std.conv;
                Object o = cast(Object)r;
                if (o is null)
                {
                    string nullString;
                    id = pack(nullString);
                }
                else
                {
                    auto ws = __rpc.publish(r, o.toHash().to!string);
                    id = ws.packedID;
                }
            }

            _resultData ~= id;
            //}
            //else
            //{
            //    pragma(msg, "Trying to RPC return class with no id property: ", Returning.stringof);
            //}                        
        }

        foreach (Method; allMethods)
        {
            alias ParamTypes = Parameters!(Method);
            if (__traits(identifier, Method) == methodName && ParamTypes.length == argCount)
            {
                enum MethodName = __traits(identifier, Method);
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
                else static if (ParamTypes.length == 4)
                {
                    ParamUnpackTypes unpackParams;
                    unpacker.unpack(unpackParams);
                    auto up  = tuple(unpackParams);
                    Tuple!ParamTypes params = lookupObjectParams!(typeof(up), ParamTypes)(up);
                    static if (is(Returning : void))
                        __traits(getMember, obj, MethodName)(params[0], params[1], params[2], params[3]);
                    else
                        auto result = __traits(getMember, obj, MethodName)(params[0], params[1], params[2], params[3]);
                }
                else static if (ParamTypes.length == 5)
                {
                    ParamUnpackTypes unpackParams;
                    unpacker.unpack(unpackParams);
                    auto up  = tuple(unpackParams);
                    Tuple!ParamTypes params = lookupObjectParams!(typeof(up), ParamTypes)(up);
                    static if (is(Returning : void))
                        __traits(getMember, obj, MethodName)(params[0], params[1], params[2], params[3], params[4]);
                    else
                        auto result = __traits(getMember, obj, MethodName)(params[0], params[1], params[2], params[3], params[4]);
                }

                // Serialize result. also convert know object types to ids.
                import std.range;
                static if (!is(Returning : void))
                {
                    static if (is(Returning == interface))
                    {
                        appendServiceID!Returning(result, _rpc, resultData);
                    }
                    else static if ( isRandomAccessRange!Returning && is(ElementType!Returning == interface))
                    {
                        alias ElmType = ElementType!Returning;
                        int sz = cast(int)result.length;
                        resultData ~= pack(sz);
                        foreach (v; result)
                            appendServiceID!ElmType(v, _rpc, resultData);
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
