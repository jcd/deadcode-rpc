module deadcode.rpc.rpccallback;

import std.traits : Parameters, ReturnType;

// NOTE: Don't use vararg templates since those do not have parameter names
//       and that will break rpcproxy. 
interface ICallback(A1)
{
	void call(A1 a1);
}

interface ICallback(A1, A2)
{
	void call(A1 a1, A2 a2);
}

interface ICallback(A1, A2, A3)
{
	void call(A1 a1, A2 a2, A3 a3);
}

interface ICallback(A1, A2, A3, A4)
{
	void call(A1 a1, A2 a2, A3 a3, A4 a4);
}

interface ICallback(A1, A2, A3, A4, A5)
{
	void call(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5);
}

class Callback(Args...) : ICallback!Args
{
	void delegate(Args) dlg;
	this(void delegate(Args) d)
	{
		dlg = d;
	}

	void call(Args args)
	{
		dlg(args);
	}
}

auto createCallback(Dlg)(Dlg dlg)
{
	alias Args = Parameters!Dlg;
	return new Callback!Args(dlg);
}

// NOTE: Don't use vararg templates since those do not have parameter names
//       and that will break rpcproxy. 
interface ICallbackReturn(RetType)
{
	RetType call();
}

interface ICallbackReturn(RetType, A1)
{
	RetType call(A1 a1);
}

interface ICallbackReturn(RetType, A1, A2)
{
	RetType call(A1 a1, A2 a2);
}

interface ICallbackReturn(RetType, A1, A2, A3)
{
	RetType call(A1 a1, A2 a2, A3 a3);
}

interface ICallbackReturn(RetType, A1, A2, A3, A4)
{
	RetType call(A1 a1, A2 a2, A3 a3, A4 a4);
}

interface ICallbackReturn(RetType, A1, A2, A3, A4, A5)
{
	RetType call(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5);
}

class CallbackReturn(RetType, Args...) : ICallbackReturn!(RetType, Args)
{
	RetType delegate(Args) dlg;
	this(RetType delegate(Args) d)
	{
		dlg = d;
	}

	RetType call(Args args)
	{
		return dlg(args);
	}
}

auto createCallbackReturn(Dlg)(Dlg dlg)
{
	alias Args = Parameters!Dlg;
	alias RetType = ReturnType!Dlg;
	return new CallbackReturn!(RetType, Args)(dlg);
}
