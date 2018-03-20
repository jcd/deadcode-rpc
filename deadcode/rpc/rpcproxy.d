module deadcode.rpc.rpcproxy;

import std.traits;

import msgpack;

import deadcode.core.future;
import deadcode.rpc.rpc;
import deadcode.rpc.rpccallback;

//import deadcode.util.weakref : WeakRef;

class RPCProxyBase
{
    private
    {
        RPC _rpc;
        ubyte[] packedType;
        ubyte[] packedID;
    }

    final @property RPC rpc() { return _rpc; }

    private this(RPC _rpc, ubyte[] packedType, ubyte[] packedID)
    {
        this._rpc = _rpc;
        this.packedType = packedType;
        this.packedID = packedID;
    }

    private final string _setupRPCCall()
    {
        rpc.outBuffer.clear();
        string callID = rpc.newCallID();
        rpc.outBuffer ~= pack(callID);
        rpc.outBuffer ~= pack(true); // request
        rpc.outBuffer ~= packedType;
        rpc.outBuffer ~= packedID;
        return callID;
    }

    private final auto _asyncCallInternal(bool yieldCall = false, Args...)(string methodName, Args args)
    {
        import core.thread : Fiber;
        CallID callID = _setupRPCCall();
        rpc.outBuffer ~= pack(methodName);
        byte argCount = cast(byte) args.length;
        rpc.outBuffer ~= pack(argCount);

        foreach (a; args)
        {
            version (RPCTrace)
                writeln("Arg: ", a);
            alias ArgType = typeof(a);
			static if (is(ArgType == interface))
            {
				auto proxyCasted = cast(RPCProxy!ArgType) a;
                if (proxyCasted is null)
                {
                    alias ServiceType = RPCService!ArgType;
//                    alias WeakServiceType = WeakRef!ServiceType;

                    auto serviceCasted = cast(ServiceType) a; 
                    if (serviceCasted is null)
                    {
                        //auto weakServiceCasted = cast(WeakServiceType) a; 
                        //if (weakServiceCasted is null)
                        //{
                            // We have a plain object. Search for it in the services list
                            bool ok = false;
                            foreach (service; rpc._services)
                            {
                                auto so = service; // .get();
                                if (/*so !is null && */ so.obj == a)
                                {
                                    rpc.outBuffer ~= so.packedID;
                                    ok = true;
                                    break;
                                }
                            }
                            if (!ok)
                            {
                                if (a is null)
								{
									rpc.outBuffer ~= pack("");
								}
								else
								{
									// Wrap the object in a weak reference and make it a service automatically
									import std.conv;
									Object o = cast(Object)a;
									auto ws = rpc.publish(a, o.toHash().to!string);
									rpc.outBuffer ~= ws.packedID;
								}
                                //import std.stdio;
                                //writeln("RPC method interface arg is not proxy, service or object in a service but : ", methodName, " ", a);
                            }
                    //    }
                    //    else
                    //    {
                    //        rpc.outBuffer ~= weakServiceCasted.get().packedID;
                    //    }
                    }
                    else
                    {
                        rpc.outBuffer ~= serviceCasted.packedID;
                    }
                }
                else
                {
                    rpc.outBuffer ~= proxyCasted.packedID;
                }
            }
			else static if (isDelegate!ArgType)
			{
				if (a is null)
				{
					rpc.outBuffer ~= pack("");
				}
				else
				{
					import std.conv;
					static if (is(ReturnType!ArgType == void))
						auto cb = createCallback(a);
					else
						auto cb = createCallbackReturn(a);

					Object o = cast(Object) cb;
					auto ws = rpc.publish(cb, o.toHash().to!string);
					rpc.outBuffer ~= ws.packedID;
				}
			}
			else
            {
	            rpc.outBuffer ~= pack(a);
            }
        }

        rpc.sendMessage();

        auto promise = new RPCAsyncCallback;
        rpc.registerCallID(callID, promise, yieldCall ? Fiber.getThis() : null);

        static if (yieldCall)
		{
			if (Fiber.getThis() is null)
				throw new Exception("Async RPC yielded call made but there is no active Fiber");
			Fiber.yield();
		}
        auto future = promise.getFuture();

        if (!rpc.isAlive && !future.isException)
            promise.setException(new Exception("RPC killed during call to " ~ methodName));

        return future;
    }

    final FutureVoid _asyncCall(bool yieldCall = false, Args...)(string methodName, Args args)
    {
        auto future = _asyncCallInternal!(yieldCall)(methodName, args);
        return future.then((Unpacker unpacker) {
            RPC.CallStatus callStatus;
            unpacker.unpack(callStatus);

            version (RPCTrace)
                writeln(callStatuc);

            import std.exception;
            enforce(callStatus == RPC.CallStatus.Success);
        });
    }

    final void _call(Args...)(string methodName, Args args)
    {
        FutureVoid future = _asyncCall!(true)(methodName, args);
        assert(future.isValid);
        future.get();
    }

    final Future!Result _asyncCall(Result, bool yieldCall = false, Args...)(string methodName, Args args)
    {
        auto future = _asyncCallInternal!(yieldCall)(methodName, args);
        return future.then((Unpacker unpacker) {
            RPC.CallStatus callStatus;
            unpacker.unpack(callStatus);

            version (RPCTrace)
                writeln(callStatus);

            import std.exception;
            enforce(callStatus == RPC.CallStatus.Success);

            Result ret;
            import std.range;
            static if ( is(Result == interface) )
            {
                version (RPCTrace)
                    writeln("unpackingA ", Result.stringof, " ", methodName);
                ret = rpc.create!Result(unpacker);
            }
            else static if ( isRandomAccessRange!Result && is(ElementType!Result == interface))
            {
                version (RPCTrace)
                    writeln("unpackingA ", Result.stringof, " ", methodName);
                alias ElmType = ElementType!Result;
                int sz = 0;
                unpacker.unpack(sz);
                ret.length = sz;
                foreach (i; 0..sz)
                    ret[i] = rpc.create!ElmType(unpacker);
            }
            else
            {
                version (RPCTrace)
                    writeln("unpackingB ", Result.stringof);
                unpacker.unpack(ret);
            }
            return ret;
        });
    }

    final Result _call(Result, Args...)(string methodName, Args args)
    {
        auto future = _asyncCall!(Result, true)(methodName, args);
        assert(future.isValid);
        return future.get();
    }
}

template ParameterTuple(alias Func)
{
    import std.traits : FunctionTypeOf;
    static if (is(FunctionTypeOf!Func Params == __parameters))
    {
        alias ParameterTuple = Params;
    } 
    else
    {
        static assert("Error in template ParameterTuple");
    }
}

template JoinStrings(int index, ARGS...)
{
    //pragma (msg, ARGS.length);
    //pragma (msg, ARGS[0]);
    static if (index != ARGS.length)
        enum JoinStrings = ARGS[index] ~ " " ~ JoinStrings!(index+1, ARGS);
    else
        enum JoinStrings = "";
}

mixin template CreateFunction(alias TemplateFunc, string functionBody)
{
    import std.string : format;
    import std.traits : ReturnType, fullyQualifiedName;

//    pragma(msg, __traits(getFunctionAttributes, TemplateFunc));
    enum funcAttrs = JoinStrings!(0, __traits(getFunctionAttributes, TemplateFunc));
    //pragma(msg, "FuncAttrs ", funcAttrs);
    //pragma(msg, fullyQualifiedName!TemplateFunc);
    enum code = q{ReturnType!(TemplateFunc) %s(ParameterTuple!TemplateFunc) %s { 
        %s
    }}.format(__traits(identifier, TemplateFunc), funcAttrs, functionBody.format("TemplateFunc" /*fullyQualifiedName!TemplateFunc*/));
	// pragma(msg, code);
	mixin(code);
}

template getAllMethods(I)
{
    import std.typetuple;
    import deadcode.core.traits : isMemberAccessible;

    enum allMembers = [ __traits(allMembers, I) ];

    template Impl(int idx)
    {
        static if (allMembers.length == idx)
            alias Impl = TypeTuple!();
        else static if (isMemberAccessible!(I, allMembers[idx]) && isCallable!(mixin("I."~allMembers[idx])))
            alias Impl = TypeTuple!(MemberFunctionsTuple!(I, allMembers[idx]), Impl!(idx + 1));
        else
            alias Impl = Impl!(idx + 1); // skip template methods

    }
    alias getAllMethods = Impl!0;
}

string generateMethodImplementations(allMethods...)(string methodBody)
{
    import std.string :  format;
    string res = "import std.traits : Identity;";
    foreach (idx, f; allMethods)
    {
        res ~= q{ 
            mixin CreateFunction!(allMethods[%s], q{%s});
        }.format(idx, methodBody);
    }
    return res;
}

class RPCProxy(I) : RPCProxyBase, I 
{
    alias ThisType = RPCProxy!I;

    this(RPC rpc, ubyte[] id)
    {
        super(rpc, pack(I.stringof), id);
    }

    // Get all members of interface I and generate a method body that simply 
    // forwards as a rpc call. Skip interface template methods though, because that
    // doesn't make sense to proxy
    alias allMethods = getAllMethods!I;
    
	import std.array;
    import std.typetuple;

    enum code = generateMethodImplementations!allMethods(RPCProxyMethodMixin);
   //	pragma(msg, code);

	mixin(code);
}

mixin template IdentifiersToArgs(T...)
{
	import std.typetuple;
	template Helper(int idx)
	{
		static if (idx == 0)
			alias Helper = AliasSeq!();
		else
			alias Helper = AliasSeq!(Helper!(idx-1), mixin(T[idx-1]));
	}
	alias Args = Helper!(T.length);
}

 // Identity!(%s);
enum RPCProxyMethodMixin = q{

    alias Func = %s;
    enum Name = __FUNCTION__.split(".")[$-1];
    alias ArgsIdents = ParameterIdentifierTuple!Func;
	mixin IdentifiersToArgs!ArgsIdents;

    alias RT = ReturnType!(Func);
    ThisType t = cast(ThisType) this; // cast away const for this
    static if(is (RT == void) )
        t._call(Name, Args);
    else
        return t._call!RT(Name, Args);
};
