-module(hash_ring).

-export([
    start/0,
    start/2,
    stop/0,
    init/1
]).

-export([
    create_ring/2,
    delete_ring/1,
    add_node/2,
    remove_node/2,
    find_node/2
]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% @doc Starts the hash_ring driver
start() ->
  case code:priv_dir(hash_ring) of
       {error, bad_name} ->
         Path = filename:join([filename:dirname(code:which(hash_ring)),"..","priv"]);
       Path ->
         ok
  end,
  start(Path, "hash_ring_drv").


%%
%% @doc Starts the hash_ring driver.
%% This function should be called with the Path to the shared library and the name
%% of the shared library. SharedLib should be <strong>hash_ring_drv</strong>.
%%
start(Path, SharedLib) ->
    case erl_ddll:load_driver(Path, SharedLib) of
        ok -> 
            Pid = spawn(?MODULE, init, ["hash_ring_drv"]),
            register(hash_ring, Pid),
            ok;
        {error, already_loaded} -> {error, already_loaded};
        Other -> exit({error, Other})
    end.
    
stop() ->
    hash_ring ! stop,
    unregister(hash_ring).
    
create_ring(Ring, NumReplicas) ->
    hash_ring ! {create_ring, self(), {Ring, NumReplicas}},
    receive
        ok -> ok
    end.
    
delete_ring(Ring) ->
    hash_ring ! {delete_ring, self(), Ring},
    receive
        ok -> ok;
        {error, Reason} -> {error, Reason}
    end.
    
add_node(Ring, Node) when is_list(Node) ->
    add_node(Ring, list_to_binary(Node));
    
add_node(Ring, Node) when is_binary(Node) ->
    hash_ring ! {add_node, self(), {Ring, Node}},
    receive 
        ok -> ok;
        {error, Error} -> {error, Error}
    end.
    
remove_node(Ring, Node) when is_list(Node) ->
    remove_node(Ring, list_to_binary(Node));
    
remove_node(Ring, Node) when is_binary(Node) ->
    hash_ring ! {remove_node, self(), {Ring, Node}},
    receive 
        ok -> ok;
        {error, Error} -> {error, Error}
    end.

find_node(Ring, Key) when is_list(Key) ->
    find_node(Ring, list_to_binary(Key));

find_node(Ring, Key) when is_binary(Key) ->
    hash_ring ! {find_node, self(), {Ring, Key}},
    receive
        {ok, Node} -> {ok, Node};
        {error, Reason} -> {error, Reason}
    end.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(state, {
port,rings
}).

init(Drv) ->
    Port = open_port({spawn, Drv}, [binary]),
    loop(#state{port=Port,rings = dict:new()}).
    

    
    
loop(#state{port=Port,rings=Rings} = State) ->
    receive
        {create_ring, Pid, {Ring, NumReplicas}} ->
            Port ! {self(), {command, <<1:8, NumReplicas:32>>}},
            NewState = receive 
                {Port, {data, <<Index:32>>}} ->
                    Pid ! ok,
                    State#state{rings = dict:store(Ring, Index, Rings)}
            end,
            loop(NewState);
        {delete_ring, Pid, Ring} ->
            NewState = case dict:find(Ring, Rings) of
                {ok, Index} ->
                    Port ! {self(), {command, <<2:8, Index:32>>}},
                    receive
                        {Port, {data, <<0:8>>}} ->
                            Pid ! ok,
                            State#state{rings = dict:erase(Ring, Rings)};
                        {Port, {data, <<1:8>>}} ->
                            Pid ! {error, ring_not_found},
                            State
                    end;
                _ ->
                    Pid ! {error, ring_not_found},
                    State
            end,
            loop(NewState);
        {add_node, Pid, {Ring, Node}} when is_binary(Node) ->
             case dict:find(Ring, Rings) of
                {ok, Index} ->
                    NodeSize = size(Node),
                    Port ! {self(), {command, <<3:8, Index:32, NodeSize:32, Node/binary>>}},
                    receive
                        {Port, {data, <<0:8>>}} ->
                            Pid ! ok;
                        {Port, {data, <<1:8>>}} ->
                            Pid ! {error, unknown_error}
                    end;
                _ ->
                    Pid ! {error, ring_not_found}
            end,
            loop(State);
        {remove_node, Pid, {Ring, Node}} when is_binary(Node) ->
             case dict:find(Ring, Rings) of
                {ok, Index} ->
                    NodeSize = size(Node),
                    Port ! {self(), {command, <<4:8, Index:32, NodeSize:32, Node/binary>>}},
                    receive
                        {Port, {data, <<0:8>>}} ->
                            Pid ! ok;
                        {Port, {data, <<1:8>>}} ->
                            Pid ! {error, unknown_error}
                    end;
                _ ->
                    Pid ! {error, ring_not_found}
            end,
            loop(State);
        {find_node, Pid, {Ring, Key}} when is_binary(Key) ->
             case dict:find(Ring, Rings) of
                {ok, Index} ->
                    KeySize = size(Key),
                    Port ! {self(), {command, <<5:8, Index:32, KeySize:32, Key/binary>>}},
                    receive
                        {Port, {data, <<3:8>>}} ->
                            Pid ! {error, invalid_ring};
                        {Port, {data, <<2:8>>}} ->
                            Pid ! {error, node_not_found};
                        {Port, {data, <<1:8>>}} ->
                            Pid ! {error, unknown_error};
                        {Port, {data, <<Node/binary>>}} ->
                            Pid ! {ok, Node}
                    end;
                _ ->
                    Pid ! {error, ring_not_found}
            end,
            loop(State);
        stop ->
            Port ! {self(), close},
            receive
                {Port, closed} ->
                    exit(normal)
            end;
        {'EXIT', Port, Reason} ->
            error_logger:error_msg("hash_ring port exited: ~p~n", [Reason]);
        _ ->
            loop(State)
    end.

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
    hash_ring:start().

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
