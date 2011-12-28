%%% @doc Log4erl appender sending messages to an AMQP broker.
-module(amqp_appender).
-behaviour(gen_event).

%%%_* Exports ==========================================================
%% gen_event callbacks
-export([ init/1
        , handle_event/2
        , handle_call/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%%%_* Includes =========================================================
-include_lib("log4erl/include/log4erl.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%%_* Defines ==========================================================
-define(DEFAULT_USER,     "guest").
-define(DEFAULT_PASS,     "guest").
-define(DEFAULT_VHOST,    "/").
-define(DEFAULT_HOST,     "localhost").
-define(DEFAULT_PORT,     5672).
-define(DEFAULT_EXCHANGE, ?MODULE).

%%%_* Records ==========================================================
-record(state, { level
               , format
               , exchange
               , params
               }).

%%%_* Code =============================================================
%%%_* Callbacks --------------------------------------------------------
init({conf, Conf}) ->
  Level    = kf(level,      Conf, ?DEFAULT_LEVEL),
  Format   = kf(format,     Conf, ?DEFAULT_FORMAT),

  Username = kf(amqp_user,  Conf, ?DEFAULT_USER),
  Password = kf(amqp_pass,  Conf, ?DEFAULT_PASS),
  VHost    = kf(amqp_vhost, Conf, ?DEFAULT_VHOST),
  Host     = kf(host,       Conf, ?DEFAULT_HOST),
  Port     = kf(port,       Conf, ?DEFAULT_PORT),
  Exchange = kf(exchange,   Conf, ?DEFAULT_EXCHANGE),

  AmqpParams = #amqp_params_network{ username     = Username
                                   , password     = Password
                                   , virtual_host = VHost
                                   , host         = Host
                                   , port         = Port
                                   },
  {ok, Channel} = amqp_channel(AmqpParams),
  ExchangeDecl = #'exchange.declare'{ exchange = b(Exchange)
                                    , type = <<"topic">>
                                    },
  #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDecl),
  {ok, #state{ level    = Level
             , format   = Format
             , exchange = Exchange
             , params   = AmqpParams
             }}.

handle_call({change_format, Format}, State0) ->
  {ok, Tokens} = log_formatter:parse(Format),
  State = State0#state{format=Tokens},
  {ok, ok, State};
handle_call({change_level, Level}, State0) ->
  State = State0#state{level = Level},
  {ok, ok, State};
handle_call(_Request, State) ->
  {ok, ok, State}.

handle_event({change_level, Level}, State0) ->
  State = State0#state{level = Level},
  {ok, State};
handle_event({log, #log{level=Level}=Log}, #state{level=CurrentLevel}=State) ->
  case log4erl:to_log(CurrentLevel, Level) of
    true  -> do_log(Log, State);
    false -> {ok, State}
  end;
handle_event(_Request, State) ->
  {ok, State}.

handle_info(_Info, State) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internals --------------------------------------------------------
do_log(#log{level=Level}=Log, #state{format=Format, params=AmqpParams}=State) ->
  case amqp_channel(AmqpParams) of
    {ok, Channel} ->
      Message = log_formatter:format(Log, Format),
      {ok, send(State, Level, Message, Channel)};
    _ ->
      {ok, State}
  end.

send(#state{exchange=Exchange}=State, Level, Message, Channel) ->
  RoutingKey = b(Level),
  Publish    = #'basic.publish'{ exchange    = Exchange
                               , routing_key = RoutingKey
                               },
  Props      = #'P_basic'{ content_type = <<"text/plain">> },
  Body       = b(Message),
  Msg        = #amqp_msg{ payload = Body
                        , props   = Props
                        },
  amqp_channel:cast(Channel, Publish, Msg), 
  State.

amqp_channel(AmqpParams) ->
  case pg2:get_closest_pid(AmqpParams) of
    {error, {no_such_group, _}} -> 
      pg2:create(AmqpParams),
      amqp_channel(AmqpParams);
    {error, {no_process, _}} -> 
      case amqp_connection:start(AmqpParams) of
        {ok, Client} ->
          case amqp_connection:open_channel(Client) of
            {ok, Channel} ->
              pg2:join(AmqpParams, Channel),
              {ok, Channel};
            {error, Reason} -> {error, Reason}
          end;
        {error, Reason} -> 
          {error, Reason}
      end;
    Channel -> 
      {ok, Channel}
  end.

%%%_* Internals --------------------------------------------------------
kf(Key, List, Default) ->
  case lists:keyfind(Key, 1, List) of
    {Key, Value} -> Value;
    false        -> Default
  end.

b(L) when is_list(L)    -> erlang:list_to_binary(L);
b(A) when is_atom(A)    -> erlang:atom_to_binary(A);
b(I) when is_integer(I) -> b(erlang:integer_to_list(I));
b(B) when is_binary(B)  -> B.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
