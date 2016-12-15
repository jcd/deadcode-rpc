module deadcode.rpc.rpcproxy;

import msgpack;

import deadcode.core.future;
import deadcode.rpc.rpc;

class RPCProxyBase
{
    private
    {
        RPC rpc;
        ubyte[] packedType;
        ubyte[] packedID;
    }

    private this(RPC rpc, ubyte[] packedType, ubyte[] packedID)
    {
        this.rpc = rpc;
        this.packedType = packedType;
        this.packedID = packedID;
    }

    private final string _setupRPCCall(string method)
    {
        rpc.outBuffer.clear();
        string callID = rpc.newCallID();
        rpc.outBuffer ~= pack(callID);
        rpc.outBuffer ~= pack(true); // request
        rpc.outBuffer ~= packedType;
        rpc.outBuffer ~= packedID;
        rpc.outBuffer ~= pack(method);
        return callID;
    }

    private final auto asyncCallInternal(bool yieldCall = false, Args...)(string methodName, Args args)
    {
        CallID callID = _setupRPCCall(methodName);

        foreach (a; args)
        {
            version (RPCTrace)
                writeln("Arg: ", a);
			static if (is(typeof(a) == interface))
            {
				auto proxyCasted = cast(RPCProxy!(typeof(a))) a;
                if (proxyCasted is null)
                {
                    auto serviceCasted = cast(RPCService!(typeof(a))) a; 
                    if (serviceCasted is null)
                    {
                        bool ok = false;
                        foreach (service; rpc._services)
                        {
                            if (a == service.obj)
                            {
                                rpc.outBuffer ~= service.packedID;
                                ok = true;
                                break;
                            }
                        }
                        if (!ok)
                        {
                            import std.stdio;
                            writeln("RPC method interface arg is not proxy, service or object in a service but : ", methodName, " ", a);
                        }
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
			else
            {
	            rpc.outBuffer ~= pack(a);
            }
        }

        rpc.sendMessage();

        auto promise = new RPCAsyncCallback;
        rpc.registerCallID(callID, promise, yieldCall ? Fiber.getThis() : null);

        static if (yieldCall)
            Fiber.yield();

        auto future = promise.getFuture();

        if (!rpc.isAlive && !future.isException)
            promise.setException(new Exception("RPC killed during call to " ~ methodName));

        return future;
    }

    final FutureVoid asyncCall(bool yieldCall = false, Args...)(string methodName, Args args)
    {
        auto future = asyncCallInternal!(yieldCall)(methodName, args);
        return future.then((Unpacker unpacker) {
            RPC.CallStatus callStatus;
            unpacker.unpack(callStatus);

            version (RPCTrace)
                writeln(callStatuc);

            import std.exception;
            enforce(callStatus == RPC.CallStatus.Success);
        });
    }

    final void call(Args...)(string methodName, Args args)
    {
        FutureVoid future = asyncCall!(true)(methodName, args);
        assert(future.isValid);
        future.get();
    }

    final Future!Result asyncCall(Result, bool yieldCall = false, Args...)(string methodName, Args args)
    {
        auto future = asyncCallInternal!(yieldCall)(methodName, args);
        return future.then((Unpacker unpacker) {
            RPC.CallStatus callStatus;
            unpacker.unpack(callStatus);

            version (RPCTrace)
                writeln(callStatus);

            import std.exception;
            enforce(callStatus == RPC.CallStatus.Success);

            Result ret;
            import std.traits;
            static if ( is(Result == interface) )
            {
                version (RPCTrace)
                    writeln("unpackingA ", Result.stringof, " ", methodName);
                ret = rpc.create!Result(unpacker);
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

    final Result call(Result, Args...)(string methodName, Args args)
    {
        auto future = asyncCall!(Result, true)(methodName, args);
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

    mixin(q{ReturnType!(TemplateFunc) %s(ParameterTuple!TemplateFunc) %s { 
        %s
    }}.format(__traits(identifier, TemplateFunc), funcAttrs, functionBody.format(fullyQualifiedName!TemplateFunc)));
}

template getAllMethods(I)
{
    import std.traits;
    import std.typetuple;
    enum allMembers = [ __traits(allMembers, I) ];

    template Impl(int idx)
    {
        static if (allMembers.length != idx)
            alias Impl = TypeTuple!(MemberFunctionsTuple!(I, allMembers[idx]), Impl!(idx + 1));
        else
            alias Impl = TypeTuple!();
        
    }
    alias getAllMethods = Impl!0;
}

string generateMethodImplementations(allMethods...)(string methodBody)
{
    import std.string :  format;
    string res;
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
    // forwards as a rpc call.
    alias allMethods = getAllMethods!I;

    enum code = generateMethodImplementations!allMethods(RPCProxyMethodMixin);
    mixin(code);
}

enum RPCProxyMethodMixin = q{
    import std.array;
    import std.traits;
    import std.typetuple;
    alias Func = Identity!(%s);
    enum Name = __FUNCTION__.split(".")[$-1];
    alias ArgsIdents = ParameterIdentifierTuple!Func;

    static if (ArgsIdents.length == 0)
        alias Args = ArgsIdents;
    else static if (ArgsIdents.length == 1)
        alias Args =  AliasSeq!(mixin(ArgsIdents[0]));
    else static if (ArgsIdents.length == 2)
        alias Args = AliasSeq!(mixin(ArgsIdents[0]), mixin(ArgsIdents[1]));
    else
	{
        pragma(msg, "Error: add support for more arguments in RPCProxyMethodMixin. " );
	}
    alias RT = ReturnType!(Func);
    ThisType t = cast(ThisType) this; // cast away const for this
    static if(is (RT == void) )
        t.call(Name, Args);
    else
        return t.call!RT(Name, Args);
};
