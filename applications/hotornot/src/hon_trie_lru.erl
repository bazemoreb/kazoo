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

-export([start_link/1, start_link/2
        ,stop/1
        ,cache_rates/2
        ]).

-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3
        ]).

-include("hotornot.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(DEBUG(_Fmt), 'ok'). %% ?debugFmt(Fmt, [])).
-define(DEBUG(_Fmt, _Msg), 'ok'). %%  ?debugFmt(Fmt, Msg)).
-else.
-define(DEBUG(Fmt), lager:debug(Fmt)).
-define(DEBUG(Fmt, Msg), lager:debug(Fmt, Msg)).
-endif.

-define(STATE_READY(Trie, RatedeckDb, CheckRef), {'ready', Trie, RatedeckDb, CheckRef}).

-define(CHECK_MSG(ExpiresS), {'check_trie', ExpiresS}).

-type state() :: ?STATE_READY(trie:trie(), ne_binary(), reference()).
-type rate_entry() :: {ne_binary(), gregorian_seconds()}.
-type rate_entries() :: [rate_entry()].

-spec start_link(ne_binary()) -> {'ok', pid()}.
-spec start_link(ne_binary(), pos_integer()) -> {'ok', pid()}.
start_link(RatedeckDb) ->
    start_link(RatedeckDb, hotornot_config:lru_expires_s()).

start_link(RatedeckDb, ExpiresS) ->
    ProcName = hon_trie:trie_proc_name(RatedeckDb),
    gen_server:start_link({'local', ProcName}, ?MODULE, [RatedeckDb, ExpiresS], []).

-spec stop(ne_binary()) -> 'ok'.
stop(<<_/binary>>=RatedeckId) ->
    ProcName = hon_trie:trie_proc_name(RatedeckId),
    case whereis(ProcName) of
        'undefined' -> 'ok';
        Pid -> catch gen_server:call(Pid, 'stop')
    end.

-spec cache_rates(ne_binary(), kzd_rate:docs()) -> 'ok'.
cache_rates(RatedeckId, Rates) ->
    ProcName = hon_trie:trie_proc_name(RatedeckId),
    gen_server:call(ProcName, {'cache_rates', Rates}).

-spec init([ne_binary() | pos_integer()]) -> {'ok', state()}.
init([RatedeckDb, ExpiresS]) ->
    kz_util:put_callid(hon_trie:trie_proc_name(RatedeckDb)),
    ExpiresCheckRef = start_expires_check_timer(ExpiresS),
    {'ok', ?STATE_READY(trie:new(), RatedeckDb, ExpiresCheckRef)}.

-spec start_expires_check_timer(pos_integer()) -> reference().
-ifdef(PROPER).
start_expires_check_timer(ExpiresS) ->
    erlang:start_timer(1 * ?MILLISECONDS_IN_SECOND, self(), ?CHECK_MSG(ExpiresS)).
-else.
start_expires_check_timer(ExpiresS) ->
    Check = (ExpiresS div 2) * ?MILLISECONDS_IN_SECOND,
    erlang:start_timer(Check, self(), ?CHECK_MSG(ExpiresS)).
-endif.

-spec handle_call(any(), pid_ref(), state()) ->
                         {'noreply', state()} |
                         {'reply', match_return(), state()}.
handle_call({'match_did', DID}, _From, ?STATE_READY(Trie, RatedeckDb, CheckRef)) ->
    {UpdatedTrie, Resp} = match_did_in_trie(DID, Trie),
    {'reply', Resp, ?STATE_READY(UpdatedTrie, RatedeckDb, CheckRef)};
handle_call({'cache_rates', Rates}, _From, ?STATE_READY(Trie, RatedeckDb, CheckRef)) ->
    UpdatedTrie = handle_caching_of_rates(Trie, Rates),
    {'reply', 'ok', ?STATE_READY(UpdatedTrie, RatedeckDb, CheckRef)};
handle_call('stop', _From, State) ->
    lager:info("requested to stop by ~p", [_From]),
    {'stop', 'normal', State};
handle_call(_Req, _From, State) ->
    {'noreply', State}.

-spec handle_cast(any(), state()) -> {'noreply', state()}.
handle_cast(_Req, State) ->
    lager:info("unhandled cast ~p", [_Req]),
    {'noreply', State}.

-spec handle_info(any(), state()) -> {'noreply', state()}.
handle_info({'timeout', CheckRef, ?CHECK_MSG(ExpiresS)}, ?STATE_READY(Trie, RatedeckDb, CheckRef)) ->
    _ = check_expired_entries(Trie, ExpiresS),
    {'noreply', ?STATE_READY(Trie, RatedeckDb, start_expires_check_timer(ExpiresS))};
handle_info(_Msg, State) ->
    ?DEBUG("unhandled message ~p",[_Msg]),
    {'noreply', State}.

-spec check_expired_entries(trie:trie(), pos_integer()) -> trie:trie().
check_expired_entries(Trie, ExpiresS) ->
    Oldest = kz_time:current_tstamp() - ExpiresS,
    {UpdatedTrie, Oldest} =
        trie:foldl(fun check_if_expired/3
                  ,{Trie, Oldest}
                  ,Trie
                  ),
    UpdatedTrie.

-spec check_if_expired(prefix(), [{ne_binary(), gregorian_seconds()}], {pid(), gregorian_seconds()}) ->
                              {pid(), gregorian_seconds()}.
check_if_expired(Prefix, Rates, {Trie, OldestTimestamp}=Acc) ->
    case [RateId
          || {RateId, LastUsed} <- Rates,
             LastUsed < OldestTimestamp
         ]
    of
        [] -> Acc;
        [_|_]=_OldRates -> {trie:erase(Prefix, Trie), OldestTimestamp}
    end.

-spec terminate(any(), state()) -> 'ok'.
terminate(_Reason, ?STATE_READY(_, _, _)) ->
    lager:info("terminating: ~p", [_Reason]).

-spec code_change(any(), state(), any()) -> {'ok', state()}.
code_change(_Vsn, State, _Extra) ->
    {'ok', State}.

-spec match_did_in_trie(string(), trie:trie()) -> {trie:trie(), match_return()}.
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
    NowMs = kz_time:current_tstamp(),
    ?DEBUG("caching ~s for prefix ~s into ~p~n", [Id, Prefix, Trie]),
    trie:update(Prefix
               ,fun(Rates) -> props:insert_value(Id, NowMs, Rates) end
               ,[{Id, NowMs}]
               ,Trie
               ).
