%%%-------------------------------------------------------------------
%%% Created : 09. Dez 2020 09:26
%%%-------------------------------------------------------------------
-module(ggTProzess).
-import(string, [concat/2]).

%% API
-export([start/7]).

start(ArbeitsZeit, TermZeit, Clientname, Koordinatorname, Nameservice, KoordinatorNode, Quota) ->
  {ok, HostName} = inet:gethostname(),
  LogFile = string:concat(string:concat(string:concat("GGTP", Clientname), string:concat("@", HostName)), ".log"),
  case net_adm:ping(Nameservice) of
    pong ->
      timer:sleep(1000),
      ClientnameAtom = list_to_atom(Clientname),
      util:logging(LogFile, io_lib:format("~p pingt an Nameservice: ~p war erfolgreich.\n", [ClientnameAtom, Nameservice])),
      ActualNameservice = global:whereis_name(nameservice),
      init(ClientnameAtom, Koordinatorname, KoordinatorNode, ActualNameservice, LogFile),
      Timer = vsutil:reset_timer(0, TermZeit, {self(), timeout}),
      Clock = vsutil:getUTC(),
      GGTPPID = spawn(fun() ->
        loop(ArbeitsZeit, TermZeit, ClientnameAtom, Quota, ActualNameservice, KoordinatorNode, Koordinatorname, Clock, nil, nil, 0, Timer, 0, false, LogFile) end),
      register(ClientnameAtom, GGTPPID);

    pang -> % logge einen fehler hier
      util:logging(LogFile, io_lib:format("~p wurde nicht gestartet.\n", [Clientname]))
  end.

% Entwurf 3.1
init(Clientname, Koordinatorname, KoordinatorNode, ActualNameservice, LogFile) ->
  util:logging(LogFile, io_lib:format("~p hat mit der Initphase angefangen.\n", [Clientname])),
  % Nameservicenode = auskunft@IT000249
  ActualNameservice ! {self(), {rebind, Clientname, node()}},
  receive
    ok ->
      util:logging(LogFile, io_lib:format("ggT-Prozess: ~p an Namensdienst: ~p rebind.done.\n", [Clientname, ActualNameservice])),
      % Look Up Koordinator
      ActualNameservice ! {self(), {lookup, Koordinatorname}},
      receive
        not_found ->
          util:logging(LogFile, io_lib:format("GGTProzess: ~p lookup über Namenservice, Koordinator ~p not_found.\n", [Clientname, Koordinatorname]));
        {pin, {Name, _Node}} ->
          % hello an Koordinator senden
          {Name, KoordinatorNode} ! {hello, Clientname}
      end
  end,
  util:logging(LogFile, io_lib:format("~p hat Initphase abgeschlossen.\n", [Clientname])).

loop(ArbeitsZeit, TermZeit, Clientname, Quota, Nameservice, KoordinatorNode, Koordinatorname, Clock, LeftN, RightN, Mi, Timer, Votes, SingleTerm, LogFile) ->
  receive
  % Entwurf 2.3.1
    {setneighbors, CLeftN, CRightN} ->
      util:logging(LogFile, io_lib:format("GGTProzess: ~p setzt seinen linken Nachbarn= ~p, und seinen Rechten Nachbarn= ~p \n", [Clientname, CLeftN, CRightN])),
      loop(ArbeitsZeit, TermZeit, Clientname, Quota, Nameservice, KoordinatorNode, Koordinatorname, Clock, CLeftN, CRightN, Mi, Timer, Votes, SingleTerm, LogFile);
  % Entwurf 2.3.2
    {setpm, MiNeu} ->
      NewTimer = vsutil:reset_timer(Timer, TermZeit, {self(), timeout}),
      NewClock = vsutil:getUTC(),
      util:logging(LogFile, io_lib:format("GGTProzess: ~p erhaelt vom Koordinator neues Mi: ~p \n", [Clientname, MiNeu])),
      % Entwurf 4.4.1
      loop(ArbeitsZeit, TermZeit, Clientname, Quota, Nameservice, KoordinatorNode, Koordinatorname, NewClock, LeftN, RightN, MiNeu, NewTimer, Votes, false, LogFile);
  % Entwurf 2.3.3
    {sendy, Y} ->
      NewTimer = vsutil:reset_timer(Timer, TermZeit, {self(), timeout}),
      NewClock = vsutil:getUTC(),
      util:logging(LogFile, io_lib:format("GGTProzess: ~p vergleicht Y < Mi. ~p < ~p \n", [Clientname, Y, Mi])),
      if
        Y < Mi ->
          timer:sleep(ArbeitsZeit * 1000),
          NewMi = calcGGT(Mi, Y),

          if
            Mi == NewMi ->
              % Ändert sich seine Zahl dadurch nicht, macht der ggT-Prozess gar nichts und erwartet die nächste Nachricht.
              loop(ArbeitsZeit, TermZeit, Clientname, Quota, Nameservice, KoordinatorNode, Koordinatorname, NewClock, LeftN, RightN, Mi, NewTimer, Votes, false, LogFile);
            true ->
              util:logging(LogFile, io_lib:format("GGTProzess: ~p sendet an seinen linken, rechten Nachbarn und Koordinator: ~p. \n", [Clientname, NewMi])),

              % Look Up Koordinator
              Nameservice ! {self(), {lookup, Koordinatorname}},
              receive
                not_found ->
                  util:logging(LogFile, io_lib:format("GGTProzess: ~p lookup über Namenservice, Koordinator ~p not_found.\n", [Clientname, Koordinatorname]));
                {pin, {Name, _Node}} ->
                  util:logging(LogFile, io_lib:format("GGTProzess: ~p briefmi an Koordinator ~p mit Mi: ~p.\n", [Clientname, Koordinatorname, NewMi])),
                  {Name, KoordinatorNode} ! {briefmi, {Clientname, NewMi, util:timeMilliSecond()}}
              end,
              util:logging(LogFile, io_lib:format("GGTProzess: ~p sendy an Nachbarn mit Mi: ~p.\n", [Clientname, NewMi])),

              Nameservice ! {self(), {lookup, RightN}},
              receive
                not_found ->
                  util:logging(LogFile, io_lib:format("GGTProzess: ~p lookup über Namenservice, RightN ~p not_found.\n", [Clientname, RightN]));
                {pin, {NameRightN, NodeRightN}} ->
                  util:logging(LogFile, io_lib:format("GGTProzess: ~p sendy an RightN ~p mit Mi: ~p.\n", [Clientname, RightN, NewMi])),
                  {NameRightN, NodeRightN} ! {sendy, NewMi}
              end,

              Nameservice ! {self(), {lookup, LeftN}},
              receive
                not_found ->
                  util:logging(LogFile, io_lib:format("GGTProzess: ~p lookup über Namenservice, LeftN ~p not_found.\n", [Clientname, LeftN]));
                {pin, {NameLeftN, NodeLeftN}} ->
                  util:logging(LogFile, io_lib:format("GGTProzess: ~p sendy an LeftN ~p mit Mi: ~p.\n", [Clientname, LeftN, NewMi])),
                  {NameLeftN, NodeLeftN} ! {sendy, NewMi}
              end,
              % Entwurf 4.4.1
              loop(ArbeitsZeit, TermZeit, Clientname, Quota, Nameservice, KoordinatorNode, Koordinatorname, Clock, LeftN, RightN, NewMi, NewTimer, Votes, false, LogFile)
          end;
        true ->
          % Entwurf 4.4.1
          loop(ArbeitsZeit, TermZeit, Clientname, Quota, Nameservice, KoordinatorNode, Koordinatorname, Clock, LeftN, RightN, Mi, NewTimer, Votes, false, LogFile)
      end;
  % Entwurf 2.3.4
    {From, {vote, Initiator}} ->
      CurClock = vsutil:getUTC(),
      VoteDecision = (CurClock - Clock) / 1000,
      TermzeitHalbe = TermZeit / 2,
%%      util:logging(LogFile, io_lib:format("~p GGTProzess: ~p entscheidet ob mit Ja gewaehlt wird: VoteDecision > TermzeitHalbe | ~p > ~p \n", [From, Initiator, VoteDecision, TermzeitHalbe])),
      if
        VoteDecision > TermzeitHalbe ->
          % waehle yes und mach weiter
          util:logging(LogFile, io_lib:format("GGTProzess: ~p stimmt für JA und sendet es: ~p \n", [Clientname, Initiator])),
          From ! {voteYes, Clientname},
          loop(ArbeitsZeit, TermZeit, Clientname, Quota, Nameservice, KoordinatorNode, Koordinatorname, Clock, LeftN, RightN, Mi, Timer, Votes, SingleTerm, LogFile);
        true ->
          util:logging(LogFile, io_lib:format("GGTProzess: ~p macht einfach weiter. Stimmt für Nein. \n", [Clientname])),
          loop(ArbeitsZeit, TermZeit, Clientname, Quota, Nameservice, KoordinatorNode, Koordinatorname, Clock, LeftN, RightN, Mi, Timer, Votes, SingleTerm, LogFile)
      end;

  % Entwurf 2.3.5
    {voteYes, _Name} ->
      if
        SingleTerm ->
          TmpVotes = Votes + 1,
%%          util:logging(LogFile, io_lib:format("GGTProzess: ~p Ja Stimme von: ~p erhalten. AktuelleVotes: ~p und Quota: ~p \n", [Clientname, Name, TmpVotes, Quota])),
          if
            TmpVotes == Quota ->
              % briefterm
              util:logging(LogFile, io_lib:format("GGTProzess: ~p Votes(~p)==Quota(~p). Also briefterm. \n", [Clientname, TmpVotes, Quota])),
              % Look Up Koordinator
              Nameservice ! {self(), {lookup, Koordinatorname}},
              receive
                not_found ->
                  util:logging(LogFile, io_lib:format("GGTProzess: ~p lookup über Namenservice, Koordinator ~p not_found.\n", [Clientname, Koordinatorname]));
                {pin, KoordiPID} ->
                  util:logging(LogFile, io_lib:format("GGTProzess: ~p briefterm an Koordinator ~p.\n", [Clientname, Koordinatorname])),
                  KoordiPID ! {self(), briefterm, {Clientname, Mi, util:timeMilliSecond()}},
                  loop(ArbeitsZeit, TermZeit, Clientname, Quota, Nameservice, KoordinatorNode, Koordinatorname, Clock, LeftN, RightN, Mi, Timer, TmpVotes, SingleTerm, LogFile)
              end;
            true ->
              % Verwerfe alle Nachrichten, die nach der Votes==Quota Nachricht eintreffen, da ein briefterm ausgeloest wurde.
              loop(ArbeitsZeit, TermZeit, Clientname, Quota, Nameservice, KoordinatorNode, Koordinatorname, Clock, LeftN, RightN, Mi, Timer, TmpVotes, SingleTerm, LogFile)
          end;
        true ->
          loop(ArbeitsZeit, TermZeit, Clientname, Quota, Nameservice, KoordinatorNode, Koordinatorname, Clock, LeftN, RightN, Mi, Timer, Votes, SingleTerm, LogFile)
      end;
  % Entwurf 2.3.6
    {From, tellmi} ->
      util:logging(LogFile, io_lib:format("GGTProzess: ~p teilt ~p sein Mi mit: ~p \n", [Clientname, From, Mi])),
      From ! {mi, Mi},
      loop(ArbeitsZeit, TermZeit, Clientname, Quota, Nameservice, KoordinatorNode, Koordinatorname, Clock, LeftN, RightN, Mi, Timer, Votes, SingleTerm, LogFile);
  % Entwurf 2.3.7
    {From, pingGGT} ->
      From ! {pongGGT, Clientname},
      util:logging(LogFile, io_lib:format("GGTProzess: ~p sendet pong an: ~p \n", [Clientname, From])),
      loop(ArbeitsZeit, TermZeit, Clientname, Quota, Nameservice, KoordinatorNode, Koordinatorname, Clock, LeftN, RightN, Mi, Timer, Votes, SingleTerm, LogFile);
  % Entwurf 2.3.8
    kill ->
      util:logging(LogFile, io_lib:format("GGTProzess: ~p beendet sich jetzt. \n", [Clientname])),
      % Entwurf 3.5
      spawn(fun() -> killProcess(Clientname, LogFile) end);
  % Entwurf 3.4
    {_ggTProzess, timeout} ->
      util:logging(LogFile, io_lib:format("GGTProzess: ~p timeout \n", [Clientname])),
      Nameservice ! {self(), {multicast, vote, Clientname}},
% Entwurf 4.4.1
      loop(ArbeitsZeit, TermZeit, Clientname, Quota, Nameservice, KoordinatorNode, Koordinatorname, Clock, LeftN, RightN, Mi, Timer, Votes, true, LogFile)
  end.

% Entwurf 4.1
calcGGT(X, Y) ->
  XTmp = X - 1,
  ModRes = XTmp rem Y,
  YNeu = ModRes + 1,
  YNeu.

killProcess(Clientname, LogFile) ->
  Nameservice = global:whereis_name(nameservice),
  Nameservice ! {self(), {unbind, Clientname}},
  receive
    ok ->
      util:logging(LogFile, io_lib:format("GGTProzess: ~p ..unbind..done.\n", [Clientname]))
  end,
  erlang:exit("GGTProzess killed.").