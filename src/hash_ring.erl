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
         delete_ring/1,
         add_node/2,
         remove_node/2,
         find_node/2,
         stop/0
]).

-define(SERVER, ?MODULE).

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
    
create_ring(Ring, NumReplicas) ->
    gen_server:call(?SERVER, {create_ring, {Ring, NumReplicas}}).
    
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
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(state, {
          port,
          rings
}).

init(Drv) ->
    Port = open_port({spawn, Drv}, [binary]),
    {ok, #state{
       port = Port,
       rings = dict:new() }}.

    
    
handle_call({create_ring, {Ring, NumReplicas}}, _From, #state{ port = Port, rings = Rings } = State) ->
    Port ! {self(), {command, <<1:8, NumReplicas:32>>}},
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

handle_call({find_node, {Ring, Key}}, _From, #state{ port = Port, rings = Rings } = State) ->
    case dict:find(Ring, Rings) of
        {ok, Index} ->
            KeySize = size(Key),
            Port ! {self(), {command, <<5:8, Index:32, KeySize:32, Key/binary>>}},
            receive
                {Port, {data, <<3:8>>}} ->
                    {reply, {error, invalid_ring}, State};
                {Port, {data, <<2:8>>}} ->
                    {reply, {error, node_not_found}, State};
                {Port, {data, <<1:8>>}} ->
                    {reply, {error, unknown_error}, State};
                {Port, {data, <<Node/binary>>}} ->
                    {reply, {ok, Node}, State}
            end;
        _ ->
            {reply, {error, ring_not_found}, State}
    end;

handle_call(stop, _From, State) ->
    {stop, normal, State}.

handle_cast(_, State) ->
    {noreply, State}.

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

-endif.
