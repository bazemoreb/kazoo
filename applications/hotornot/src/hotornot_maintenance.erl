%%%-------------------------------------------------------------------
%%% @copyright (C) 2011-2017, 2600Hz
%%% @doc
%%% Helper functions for users to inspect how HotOrNot is running
%%% @end
%%% @contributors
%%%   James Aimonetti
%%%-------------------------------------------------------------------
-module(hotornot_maintenance).

-export([local_summary/0
        ,rates_for_did/1, rates_for_did/2, rates_for_did/3, rates_for_did/4
        ,rates_between/2
        ,trie_rebuild/0
        ,get_rate_version/0, set_rate_version/1
        ]).

-include("hotornot.hrl").

-define(LOCAL_SUMMARY_ROW_FORMAT,
        " ~45.s | ~6.s | ~9.s | ~9.s | ~9.s | ~9.s | ~15.s | ~15.s | ~15.s |~n").
-define(LOCAL_SUMMARY_HEADER,
        io:format(?LOCAL_SUMMARY_ROW_FORMAT
                 ,[<<"RATE NAME">>, <<"COST">>, <<"INCREMENT">>, <<"MINIMUM">>
                  ,<<"SURCHARGE">>, <<"WEIGHT">>, <<"PREFIX">>, <<"RATEDECK NAME">>
                  ,<<"VERSION">>
                  ]
                 )
       ).

-spec local_summary() -> 'ok'.
local_summary() ->
    io:format("use rates_for_did/1 to see what rates would be used for a DID~n").

-spec trie_rebuild() -> 'ok'.
-spec wait_for_rebuild(pid(), reference()) -> 'ok'.
trie_rebuild() ->
    case hotornot_config:should_use_trie() of
        'true' ->
            {'ok', Pid} = hon_trie:rebuild(),
            wait_for_rebuild(Pid, erlang:monitor('process', Pid));
        'false' ->
            io:format("trie usage is not configured~n")
    end.

wait_for_rebuild(Pid, Ref) ->
    Timeout = hotornot_config:trie_build_timeout_ms() + 500,
    receive
        {'DOWN', Ref, 'process', Pid, 'normal'} ->
            io:format("trie has been rebuilt successfully~n");
        {'DOWN', Ref, 'process', Pid, _Reason} ->
            io:format("trie failed to be rebuilt in ~p: ~p~n", [Pid, _Reason])
    after
        Timeout ->
            io:format("trie failed to build in ~p under ~p ms~n", [Pid, Timeout])
    end.

-spec rates_for_did(ne_binary()) -> 'ok'.
-spec rates_for_did(ne_binary(), ne_binary()) -> 'ok'.
-spec rates_for_did(ne_binary(), api_ne_binary(), trunking_options()) -> 'ok'.
-spec rates_for_did(ne_binary(), api_ne_binary(), api_ne_binary(), trunking_options()) -> 'ok'.
rates_for_did(DID) ->
    rates_for_did(DID, 'undefined', 'undefined', []).
rates_for_did(DID, AccountId) ->
    rates_for_did(DID, 'undefined', AccountId, []).
rates_for_did(DID, Direction, RouteOptions) ->
    rates_for_did(DID, Direction, 'undefined', RouteOptions).
rates_for_did(DID, Direction, AccountId, RouteOptions) when is_list(RouteOptions) ->
    case hon_util:candidate_rates(DID, AccountId) of
        {'ok', []} -> io:format("rate lookup had no results~n");
        {'error', _E} -> io:format("rate lookup error: ~p~n", [_E]);
        {'ok', Rates} ->
            ReqJObj = kz_json:from_list(
                        props:filter_undefined(
                          [{<<"Account-ID">>, AccountId}
                          ,{<<"Direction">>, Direction}
                          ,{<<"Options">>, RouteOptions}
                          ,{<<"To-DID">>, DID}
                          ])),
            io:format("Candidates:~n"),
            ?LOCAL_SUMMARY_HEADER,
            lists:foreach(fun print_rate/1, Rates),
            print_matching(hon_util:matching_rates(Rates, ReqJObj))
    end;
rates_for_did(DID, Direction, AccountId, Opt) ->
    rates_for_did(DID, Direction, AccountId, [Opt]).

-spec print_matching(kzd_rate:docs()) -> 'ok'.
print_matching([]) ->
    io:format("no rates matched~n");
print_matching(Matching) ->
    io:format("Matching:~n"),
    ?LOCAL_SUMMARY_HEADER,

    [Winning|Sorted] = hon_util:sort_rates(Matching),
    Name = kzd_rate:name(Winning),

    lists:foreach(fun print_rate/1
                 ,[kzd_rate:set_name(Winning, <<"* ", Name/binary>>)
                   | Sorted
                  ]).

-spec rates_between(ne_binary(), ne_binary()) -> 'ok'.
rates_between(Pre, Post) ->
    ViewOpts = [{'startkey', kz_term:to_binary(Pre)}
               ,{'endkey', kz_term:to_binary(Post)}
               ],
    case kz_datamgr:get_results(?KZ_RATES_DB, <<"rates/lookup">>, ViewOpts) of
        {'ok', []} -> io:format("rate lookup had no results~n");
        {'error', _E} -> io:format("rate lookup error: ~p~n", [_E]);
        {'ok', Rates} ->
            io:format("Rates between:~n"),
            ?LOCAL_SUMMARY_HEADER,
            _ = [print_rate(kz_json:get_value(<<"value">>, R)) || R <- Rates],
            'ok'
    end.

-spec print_rate(kzd_rate:doc()) -> 'ok'.
print_rate(Rate) ->
    io:format(?LOCAL_SUMMARY_ROW_FORMAT
             ,[kz_term:to_binary(kzd_rate:name(Rate))
              ,kz_term:to_binary(kzd_rate:rate_cost(Rate))
              ,kz_term:to_binary(kzd_rate:increment(Rate))
              ,kz_term:to_binary(kzd_rate:minimum(Rate))
              ,kz_term:to_binary(kzd_rate:surcharge(Rate))
              ,kz_term:to_binary(kzd_rate:weight(Rate))
              ,kz_term:to_binary(kzd_rate:prefix(Rate))
              ,kz_term:to_binary(kzd_rate:ratedeck(Rate, <<>>))
              ,kz_term:to_binary(kzd_rate:version(Rate, <<>>))
              ]).

-spec get_rate_version() -> api_binary().
get_rate_version() -> hotornot_config:rate_version().

-spec set_rate_version(ne_binary()) -> 'ok'.
set_rate_version(Version) ->
    hotornot_config:set_rate_version(Version).
