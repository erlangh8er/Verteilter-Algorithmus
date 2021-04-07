%%%-------------------------------------------------------------------
%%% Created : 09. Dez 2020 09:26
%%%-------------------------------------------------------------------
-module(starter).
-import(string, [concat/2]).
-import(ggTProzess, [start/5]).
%% API
-export([go/2]).

go(0, _StarterNummer) ->
  io:format("Alle Starter gestartet. \n");
go(Anzahl, StarterNummer) ->
  {ok, Config} = file:consult("ggt.cfg"),
  {ok, Praktikumsgruppe} = vsutil:get_config_value(praktikumsgruppe, Config),
  {ok, Teamnummer} = vsutil:get_config_value(teamnummer, Config),
  {ok, Nameservicenode} = vsutil:get_config_value(nameservicenode, Config),
  {ok, Koordinatorname} = vsutil:get_config_value(koordinatorname, Config),
  {ok, HostName} = inet:gethostname(),
  LogFile = string:concat(string:concat(string:concat("ggtSTARTER_", util:to_String(StarterNummer)), string:concat("@",HostName)), ".log"),
  util:logging(LogFile, "ggt.cfg ausgelesen.\n"),
  init(Nameservicenode, Koordinatorname, Praktikumsgruppe, Teamnummer, StarterNummer, LogFile),
  go(Anzahl - 1, StarterNummer + 1).


init(Nameservicenode, Koordinatorname, Praktikumsgruppe, Teamnummer, StarterNummer, LogFile) ->
  case net_adm:ping(Nameservicenode) of
    pong ->
      timer:sleep(1000),
      util:logging(LogFile, "starter pingt an Nameservice war erfolgreich.\n"),
      spawn(fun() ->
        loop(Praktikumsgruppe, Teamnummer, Nameservicenode, Koordinatorname, StarterNummer, LogFile) end),
      util:logging(LogFile, "Starter ist bereit.\n");
    pang -> % logge einen fehler hier
      util:logging(LogFile, "Starter wurde nicht gestartet.\n")
  end.

loop(Praktikumsgruppe, Teamnummer, Nameservicenode, Koordinatorname, StarterNummer, LogFile) ->
  Nameservice = global:whereis_name(nameservice),
  Nameservice ! {self(), {lookup, Koordinatorname}},
  receive
    not_found -> util:logging(LogFile, "..Koordinator..not_found.\n");
    {pin, {Name, Node}} ->
      util:logging(LogFile, io:format("...ok: {~p,~p}.\n", [Name, Node])),
      % getsteeringval vom koordinator erfragen
      {Name, Node} ! {self(), getsteeringval},
      receive
      % Entwurf 2.2.1
        {steeringval, ArbeitsZeit, TermZeit, Quota, GGTProzessnummer} ->
          startggTProzess(GGTProzessnummer, ArbeitsZeit, TermZeit, Praktikumsgruppe, Teamnummer, Quota, Koordinatorname, Nameservicenode, Node, StarterNummer, LogFile)
      end
  end.


startggTProzess(1, ArbeitsZeit, TermZeit, Praktikumsgruppe, Teamnummer, Quota, Koordinatorname, Nameservice, KoordinatorNode, StarterNummer, LogFile) ->
  Clientname = string:concat(string:concat(util:to_String(Praktikumsgruppe), util:to_String(Teamnummer)), string:concat(util:to_String(1), util:to_String(StarterNummer))),
  spawn(fun() ->
    ggTProzess:start(ArbeitsZeit, TermZeit, Clientname, Koordinatorname, Nameservice, KoordinatorNode, Quota) end),
  util:logging(LogFile, io_lib:format("Starte ggT-Prozess: ~p \n", [Clientname])),
  util:logging(LogFile, "Alle ggT-Prozesse gestartet.\n");

startggTProzess(GGTProzessnummer, ArbeitsZeit, TermZeit, Praktikumsgruppe, Teamnummer, Quota, Koordinatorname, Nameservice, KoordinatorNode, StarterNummer, LogFile) ->
  Clientname = string:concat(string:concat(util:to_String(Praktikumsgruppe), util:to_String(Teamnummer)), string:concat(util:to_String(GGTProzessnummer), util:to_String(StarterNummer))),
  util:logging(LogFile, io_lib:format("Starte ggT-Prozess: ~p \n", [Clientname])),
  spawn(fun() ->
    ggTProzess:start(ArbeitsZeit, TermZeit, Clientname, Koordinatorname, Nameservice, KoordinatorNode, Quota) end),
  TmpGGTProzessnummer = GGTProzessnummer - 1,
  startggTProzess(TmpGGTProzessnummer, ArbeitsZeit, TermZeit, Praktikumsgruppe, Teamnummer, Quota, Koordinatorname, Nameservice, KoordinatorNode, StarterNummer, LogFile).