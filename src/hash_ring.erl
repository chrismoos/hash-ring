-module(hash_ring).
-behaviour(gen_server).

%% gen_server
-export([
         start_link/0,
         start_link/2,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
]).

%% API
-export([
         create_ring/2,
         create_ring/3,
         delete_ring/1,
         add_node/2,
         remove_node/2,
         find_node/2,
         set_mode/2,
         stop/0
]).

-define(SERVER, ?MODULE).

-include("hash_ring.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% @doc Starts the hash_ring driver
start_link() ->
  case code:priv_dir(hash_ring) of
       {error, bad_name} ->
         Path = filename:join([filename:dirname(code:which(hash_ring)),"..","priv"]);
       Path ->
         ok
  end,
  start_link(Path, "hash_ring_drv").


%%
%% @doc Starts the hash_ring driver.
%% This function should be called with the Path to the shared library and the name
%% of the shared library. SharedLib should be <strong>hash_ring_drv</strong>.
%%
start_link(Path, SharedLib) ->
    case erl_ddll:load_driver(Path, SharedLib) of
        ok -> 
            gen_server:start_link({local, ?SERVER}, ?MODULE, "hash_ring_drv", []);
        {error, already_loaded} -> {error, already_loaded};
        Other -> exit({error, Other})
    end.
    
stop() ->
    (catch gen_server:call(?SERVER, stop)).
    
create_ring(Ring, NumReplicas, HashFunction) when is_integer(NumReplicas), is_integer(HashFunction) ->
    gen_server:call(?SERVER, {create_ring, {Ring, NumReplicas, HashFunction}}).

%% Defaults to SHA1 for backwards compatibility.
create_ring(Ring, NumReplicas) ->
    gen_server:call(?SERVER, {create_ring, {Ring, NumReplicas, ?HASH_RING_FUNCTION_SHA1}}).
    
delete_ring(Ring) ->
    gen_server:call(?SERVER, {delete_ring, Ring}).
    
add_node(Ring, Node) when is_list(Node) ->
    add_node(Ring, list_to_binary(Node));
    
add_node(Ring, Node) when is_binary(Node) ->
    gen_server:call(?SERVER, {add_node, {Ring, Node}}).
    
remove_node(Ring, Node) when is_list(Node) ->
    remove_node(Ring, list_to_binary(Node));
    
remove_node(Ring, Node) when is_binary(Node) ->
    gen_server:call(?SERVER, {remove_node, {Ring, Node}}).

find_node(Ring, Key) when is_list(Key) ->
    find_node(Ring, list_to_binary(Key));

find_node(Ring, Key) when is_binary(Key) ->
    gen_server:call(?SERVER, {find_node, {Ring, Key}}).

set_mode(Ring, Mode) when is_integer(Mode) ->
    gen_server:call(?SERVER, {set_mode, {Ring, Mode}}).
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(state, {
          port,
          rings,
          queue
}).

init(Drv) ->
    Port = open_port({spawn, Drv}, [binary]),
    {ok, #state{
       port = Port,
       rings = dict:new(),
       queue = queue:new() }}.

    
    
handle_call({create_ring, {Ring, NumReplicas, HashFunction}}, _From, #state{ port = Port, rings = Rings } = State) ->
    Port ! {self(), {command, <<1:8, NumReplicas:32, HashFunction:8>>}},
    receive 
        {Port, {data, <<Index:32>>}} ->
            {reply, ok, 
             State#state{rings = dict:store(Ring, Index, Rings)}}
    end;

handle_call({delete_ring, Ring}, _From, #state{ port = Port, rings = Rings } = State) ->
    case dict:find(Ring, Rings) of
        {ok, Index} ->
            Port ! {self(), {command, <<2:8, Index:32>>}},
            receive
                {Port, {data, <<0:8>>}} ->
                    {reply, ok, State#state{rings = dict:erase(Ring, Rings)}};
                {Port, {data, <<1:8>>}} ->
                    {reply, {error, ring_not_found}, State}
            end;
        _ ->
            {reply, {error, ring_not_found}, State}
    end;

handle_call({add_node, {Ring, Node}}, _From, #state{ port = Port, rings = Rings } = State) ->
    case dict:find(Ring, Rings) of
        {ok, Index} ->
            NodeSize = size(Node),
            Port ! {self(), {command, <<3:8, Index:32, NodeSize:32, Node/binary>>}},
            receive
                {Port, {data, <<0:8>>}} ->
                    {reply, ok, State};
                {Port, {data, <<1:8>>}} ->
                    {reply, {error, unknown_error}, State}
            end;
        _ ->
            {reply, {error, ring_not_found}, State}
    end;

handle_call({remove_node, {Ring, Node}}, _From, #state{ port = Port, rings = Rings } = State) ->
    case dict:find(Ring, Rings) of
        {ok, Index} ->
            NodeSize = size(Node),
            Port ! {self(), {command, <<4:8, Index:32, NodeSize:32, Node/binary>>}},
            receive
                {Port, {data, <<0:8>>}} ->
                    {reply, ok, State};
                {Port, {data, <<1:8>>}} ->
                    {reply, {error, unknown_error}, State}
            end;
        _ ->
            {reply, {error, ring_not_found}, State}
    end;

handle_call({find_node, {Ring, Key}}, From, #state{ port = Port, rings = Rings, queue = Queue } = State) ->
    case dict:find(Ring, Rings) of
        {ok, Index} ->
            KeySize = size(Key),
            Port ! {self(), {command, <<5:8, Index:32, KeySize:32, Key/binary>>}},
            {noreply, State#state{queue = queue:in(From, Queue)}};
        _ ->
            {reply, {error, ring_not_found}, State}
    end;

handle_call({set_mode, {Ring, Mode}}, _From, #state{port = Port, rings = Rings} = State) ->
    case dict:find(Ring, Rings) of
        {ok, Index} ->
            Port ! {self(), {command, <<6:8, Index:32, Mode:8>>}},
            receive 
                {Port, {data, <<0:8>>}} ->
                    {reply, ok, State};
                {Port, {data, <<1:8>>}} ->
                    {reply, {error, cant_set_mode}, State}
            end;
        _ -> 
            {reply, {error, ring_not_found}, State}
    end;

handle_call(stop, _From, State) ->
    {stop, normal, State}.

handle_cast(_, State) ->
    {noreply, State}.
handle_info({Port, {data, Data}}, #state{port = Port, queue = Queue} = State) ->
    R =
    case Data of
        <<3:8>> ->
            {error, invalid_ring};
        <<2:8>> ->
            {error, node_not_found};
        <<1:8>> ->
            {error, unknown_error};
        <<Node/binary>> ->
            {ok, Node}
    end,
    {{value, Pid}, QTail} = queue:out(Queue),
    safe_reply(Pid, R),
    {noreply, State#state{queue = QTail}};

handle_info({'EXIT', Port, _Reason} = PortExit, #state{ port = Port } = State ) ->
    {stop, PortExit, State}.

terminate({'EXIT', Port, _Reason}, #state{ port = Port }) ->
    ok;
terminate(_, #state{ port = Port }) ->
    Port ! {self(), close},
    receive
        {Port, closed} ->
            ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

safe_reply(undefined, _Value) ->
    ok;
safe_reply(From, Value) ->
    gen_server:reply(From, Value).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup_driver() ->
    case whereis(hash_ring) of
        undefined -> ok;
        _ -> stop()
    end,
    hash_ring:start_link().

driver_starts_test() ->
    setup_driver(),
    ?assert(whereis(?MODULE) =/= undefined).
    
create_ring_test() ->
    setup_driver(),
    Ring = "myring",
    ?assert(create_ring(Ring, 8) == ok),
    ?assert(delete_ring(Ring) == ok).
    
create_and_delete_many_rings_test() ->
    setup_driver(),
    lists:foreach(fun(X) ->
        ?assert(create_ring(io_lib:format("ring_~p", [X]), 8) == ok)
    end, lists:seq(1, 5000)),
    lists:foreach(fun(X) ->
        ?assert(delete_ring(io_lib:format("ring_~p", [X])) == ok)
    end, lists:seq(1, 2000)),
    lists:foreach(fun(X) ->
        ?assert(create_ring(io_lib:format("ring_~p", [X]), 8) == ok)
    end, lists:seq(5000, 6000)),
    lists:foreach(fun(X) ->
        ?assert(delete_ring(io_lib:format("ring_~p", [X])) == ok)
    end, lists:seq(2001, 6000)).

delete_invalid_ring_test() ->
    setup_driver(),
    ?assert(delete_ring("notvalid") == {error, ring_not_found}).

find_known_keys_in_ring_test() ->
    setup_driver(),
    Ring = "myring",
    ?assert(create_ring(Ring, 8) == ok),
    ?assert(add_node(Ring, <<"slotA">>) == ok),
    ?assert(add_node(Ring, <<"slotB">>) == ok),
    
    ?assert(find_node(Ring, <<"keyA">>) == {ok, <<"slotA">>}),
    ?assert(find_node(Ring, <<"keyBBBB">>) == {ok, <<"slotA">>}),
    ?assert(find_node(Ring, <<"keyB_">>) == {ok, <<"slotB">>}).
    
remove_node_test() ->
    setup_driver(),
    Ring = "myring",
    ?assert(create_ring(Ring, 8) == ok),
    ?assert(add_node(Ring, <<"slotA">>) == ok),
    ?assert(add_node(Ring, <<"slotB">>) == ok),
    
    ?assert(find_node(Ring, <<"keyA">>) == {ok, <<"slotA">>}),
    ?assert(find_node(Ring, <<"keyBBBB">>) == {ok, <<"slotA">>}),
    ?assert(find_node(Ring, <<"keyB_">>) == {ok, <<"slotB">>}),
    
    ?assert(remove_node(Ring, <<"slotA">>) == ok),
    ?assert(find_node(Ring, <<"keyA">>) == {ok, <<"slotB">>}),
    ?assert(find_node(Ring, <<"keyBBBB">>) == {ok, <<"slotB">>}).

set_mode_libmemcached_test() ->
    setup_driver(),
    Ring = "myring",
    ?assert(create_ring(Ring, 1, ?HASH_RING_FUNCTION_MD5) == ok),
    ?assertEqual(ok, set_mode(Ring, ?HASH_RING_MODE_LIBMEMCACHED_COMPAT)).

-endif.
