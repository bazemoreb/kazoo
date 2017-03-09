%%%-------------------------------------------------------------------
%%% @copyright (C) 2011-2017, 2600Hz INC
%%% @doc
%%% (Words * Bytes/Word) div (Prefixes) ~= Bytes per Prefix
%%% (1127494 * 8) div 78009 = 115
%%%
%%% Processing:
%%% timer:tc(hon_trie, match_did, [<<"53341341354">>]).
%%%  {132,...}
%%% timer:tc(hon_util, candidate_rates, [<<"53341341354">>]).
%%%  {16989,...}
%%%
%%% Progressively load rates instead of seeding from the database
%%%
%%% @end
%%% @contributors
%%%   James Aimonetti
%%%-------------------------------------------------------------------
-module(hon_trie_lru).
-behaviour(gen_server).

-export([start_link/1
        ,match_did/2, match_did/3

        ,cache_rates/2
        ]).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3
        ]).

-export([handle_db_update/2]).

-include("hotornot.hrl").

-define(STATE_READY(Trie, RatedeckDb), {'ready', Trie, RatedeckDb}).

-type state() :: ?STATE_READY(trie:trie(), ne_binary()).
-type rate_entry() :: {ne_binary(), gregorian_seconds()}.
-type rate_entries() :: [rate_entry()].

-spec start_link(ne_binary()) -> {'ok', pid()}.
start_link(RatedeckDb) ->
    ProcName = hon_trie:trie_proc_name(RatedeckDb),
    gen_server:start_link({'local', ProcName}, ?MODULE, [RatedeckDb], []).

-type match_return() :: {'error', any()} |
                        {'ok', kzd_rate:docs()}.
-spec match_did(ne_binary(), api_ne_binary()) -> match_return().
-spec match_did(ne_binary(), api_ne_binary(), api_ne_binary()) -> match_return().
match_did(ToDID, AccountId) ->
    match_did(ToDID, AccountId, 'undefined').
match_did(ToDID, AccountId, RatedeckId) ->
    RatedeckId = hon_util:account_ratedeck(AccountId, RatedeckId),
    ProcName = hon_trie:trie_proc_name(RatedeckId),

    case gen_server:call(ProcName, {'match_did', kz_term:to_list(ToDID)}) of
        {'error', 'not_found'} ->
            lager:debug("failed to find rate for ~s", [ToDID]),
            {'ok', []};
        {'error', E} ->
            lager:warning("failed to find rate for ~s, got error ~p", [ToDID, E]),
            {'error', E};
        {'ok', {_Prefix, RateIds}} ->
            lager:info("candidate rates for ~s: ~s ~p", [ToDID, _Prefix, RateIds]),
            load_rates(RatedeckId, RateIds)
    end.

-spec load_rates(ne_binary(), ne_binaries()) -> {'ok', kzd_rate:docs()}.
load_rates(Ratedeck, RateIds) ->
    RatedeckDb = kzd_ratedeck:format_ratedeck_db(Ratedeck),
    {'ok', lists:foldl(fun(R, Acc) -> load_rate(R, Acc, RatedeckDb) end, [], RateIds)}.

-spec load_rate(ne_binary(), kz_json:objects(), ne_binary()) -> kzd_rate:docs().
load_rate(RateId, Acc, RatedeckDb) ->
    case kz_datamgr:open_cache_doc(RatedeckDb, RateId) of
        {'error', _} -> Acc;
        {'ok', RateDoc} ->
            [kzd_rate:set_ratedeck(RateDoc, kzd_ratedeck:format_ratedeck_id(RatedeckDb)) | Acc]
    end.

-spec cache_rates(ne_binary(), kzd_rate:docs()) -> 'ok'.
cache_rates(RatedeckId, Rates) ->
    ProcName = hon_trie:trie_proc_name(RatedeckId),
    gen_server:cast(ProcName, {'cache_rates', Rates}).

-spec init([ne_binary()]) -> {'ok', state()}.
init([RatedeckDb]) ->
    kz_util:put_callid(hon_trie:trie_proc_name(RatedeckDb)),
    {'ok', ?STATE_READY(trie:new(), RatedeckDb)}.

-spec handle_call(any(), pid_ref(), state()) ->
                         {'noreply', state()} |
                         {'reply', match_return(), state()}.
handle_call({'match_did', DID}, _From, ?STATE_READY(Trie, RatedeckDb)) ->
    {UpdatedTrie, Resp} = match_did_in_trie(DID, Trie),
    {'reply', Resp, ?STATE_READY(UpdatedTrie, RatedeckDb)};
handle_call(_Req, _From, State) ->
    {'noreply', State}.

-spec handle_cast(any(), state()) -> {'noreply', state()}.
handle_cast({'cache_rates', Rates}, ?STATE_READY(Trie, RatedeckDb)) ->
    UpdatedTrie = handle_caching_of_rates(Trie, Rates),
    {'noreply', ?STATE_READY(UpdatedTrie, RatedeckDb)};
handle_cast(_Req, State) ->
    lager:info("unhandled cast ~p", [_Req]),
    {'noreply', State}.

-spec handle_info(any(), state()) -> {'noreply', state()}.
handle_info(Msg, State) ->
    lager:debug("unhandled message ~p",[Msg]),
    {'noreply', State}.

-spec terminate(any(), state()) -> 'ok'.
terminate(_Reason, ?STATE_READY(_, _)) ->
    lager:info("terminating: ~p", [_Reason]).

-spec code_change(any(), state(), any()) -> {'ok', state()}.
code_change(_Vsn, State, _Extra) ->
    {'ok', State}.

-spec handle_db_update(kz_json:object(), kz_proplist()) -> 'ok'.
handle_db_update(ConfUpdate, _Props) ->
    'true' = kapi_conf:doc_update_v(ConfUpdate),

    lager:debug("conf update: ~p", [ConfUpdate]),
    process_conf_update(ConfUpdate, kapi_conf:get_type(ConfUpdate)).

process_conf_update(ConfUpdate, <<"database">>) ->
    process_db_update(kapi_conf:get_database(ConfUpdate)
                     ,kz_api:event_name(ConfUpdate)
                     );
process_conf_update(_ConfUpdate, _Type) ->
    'ok'.

-spec process_db_update(ne_binary(), ne_binary()) -> 'ok'.
process_db_update(?KZ_RATES_DB=RatedeckId, ?DB_EDITED) ->
    {'ok', Pid} = gen_server:call(hon_trie:trie_proc_name(RatedeckId), 'rebuild'),
    lager:info("ratedeck ~s changed, rebuilding trie in ~p", [Pid]);
process_db_update(?KZ_RATES_DB=RatedeckId, ?DB_DELETED) ->
    Proc = hon_trie:trie_proc_name(RatedeckId),
    hon_tries_sup:stop_trie(Proc),
    lager:info("ratedeck ~s deleted, stopping the trie at ~p", [RatedeckId, Proc]);
process_db_update(?MATCH_RATEDECK_DB_ENCODED(_)=RatedeckDb, ?DB_CREATED) ->
    maybe_start_trie_server(RatedeckDb);
process_db_update(?MATCH_RATEDECK_DB_ENCODED(_)=RatedeckDb, ?DB_EDITED) ->
    {'ok', Pid} = gen_server:call(hon_trie:trie_proc_name(RatedeckDb), 'rebuild'),
    lager:info("ratedeck ~s changed, rebuiding trie in ~p", [RatedeckDb, Pid]);
process_db_update(?MATCH_RATEDECK_DB_ENCODED(_)=RatedeckDb, ?DB_DELETED) ->
    Pid = hon_trie:trie_proc_name(RatedeckDb),
    hon_tries_sup:stop_trie(Pid),
    lager:info("ratedeck ~s deleted, stopping the trie at ~p", [RatedeckDb, Pid]);
process_db_update(_Db, _Action) ->
    lager:debug("ignoring ~s: ~s", [_Db, _Action]).

-spec maybe_start_trie_server(ne_binary()) -> 'ok'.
maybe_start_trie_server(RatedeckDb) ->
    case hon_tries_sup:start_trie(RatedeckDb) of
        {'ok', Pid} ->
            lager:info("starting new trie for ratedeck ~s in ~p", [RatedeckDb, Pid]);
        {'error', {'already_started', Pid}} ->
            lager:info("trie running for ratedeck ~s in ~p", [RatedeckDb, Pid]);
        {'error', 'already_present'} ->
            {'ok', Pid} = hon_tries_sup:restart_trie(RatedeckDb),
            lager:info("restarted trie for ratedeck ~s in ~p", [RatedeckDb, Pid]);
        {'error', _E} ->
            lager:debug("failed to start trie for ~s: ~p", [RatedeckDb, _E])
    end.

-spec match_did_in_trie(string(), trie:trie()) -> {trie:tie(), match_return()}.
match_did_in_trie(DID, Trie) ->
    case trie:find_prefix_longest(DID, Trie) of
        'error' -> {Trie, {'error', 'not_found'}};
        {'ok', Prefix, RateIds} ->
            UpdatedTrie = bump_prefix_timestamp(Trie, Prefix, RateIds),
            {UpdatedTrie, {'ok', {Prefix, [Id || {Id, _Created} <- RateIds]}}}
    end.

-spec bump_prefix_timestamp(trie:trie(), string(), rate_entries()) -> trie:trie().
bump_prefix_timestamp(Trie, Prefix, RateIds) ->
    BumpedRateIds = [{Id, kz_time:current_tstamp()}
                     || {Id, _LastAccessed} <- RateIds
                    ],
    trie:store(Prefix, BumpedRateIds, Trie).

-spec handle_caching_of_rates(trie:trie(), kzd_rate:docs()) -> trie:trie().
handle_caching_of_rates(Trie, Rates) ->
    lists:foldl(fun cache_rate/2, Trie, Rates).

-spec cache_rate(kzd_rate:doc(), trie:trie()) -> trie:trie().
cache_rate(Rate, Trie) ->
    Id = kz_doc:id(Rate),
    Prefix = kz_term:to_list(kzd_rate:prefix(Rate)),
    lager:debug("caching ~s for prefix ~s", [Id, Prefix]),
    trie:append(Prefix, {Id, kz_time:current_tstamp()}, Trie).
