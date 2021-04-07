%%%-------------------------------------------------------------------
%%% Created : 02. Dez 2020 09:26
%%%-------------------------------------------------------------------
-module(koordinator).
-import(string, [concat/2]).

%% API
-export([start/0]).

start() ->
  %Entwurf 3.3.1
  {ok, HostName} = inet:gethostname(),
  LogFile = string:concat(string:concat("Koordinator@", HostName), ".log"),
  {ok, Config} = file:consult("koordinator.cfg"),
  util:logging(LogFile, "koordinator.cfg geöffnet.\n"),
  {ok, SleepTime} = vsutil:get_config_value(arbeitszeit, Config),
  {ok, XTime} = vsutil:get_config_value(termzeit, Config),
  {ok, GgtprozessAnzahl} = vsutil:get_config_value(ggtprozessnummer, Config),
  {ok, Nameservicenode} = vsutil:get_config_value(nameservicenode, Config),
  {ok, Koordinatorname} = vsutil:get_config_value(koordinatorname, Config),
  {ok, Quote} = vsutil:get_config_value(quote, Config),
  {ok, KorrigierungsFlag} = vsutil:get_config_value(korrigieren, Config),
  util:logging(LogFile, "koordinator.cfg ausgelesen.\n"),
  init(Nameservicenode, Koordinatorname, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile).

%Entwurf 3.3.1
init(Nameservicenode, Koordinatorname, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile) ->
  util:logging(LogFile, "Es wurde mit Initphase angefangen.\n"),
  %Nameservice anpingen
  case net_adm:ping(Nameservicenode) of
    pong ->
      vsutil:meinSleep(1000),
      util:logging(LogFile, "ping an Nameservice war erfolgreich.\n"),
      Nameservice = global:whereis_name(nameservice),
      %Koordinator erstellen
      KPID = spawn(fun() ->
        loop([], [], Nameservice, nil, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,0) end),
      register(Koordinatorname, KPID),
      util:logging(LogFile, io_lib:format("Koordinatorprozess wurde gestartet, ProzessID von Koordinator ist ~p, benutzen sie dies für senden der Nachrichten an Koordinator, wie z.B step.\n",[KPID])),
      %Verbindung zum Nameservice aufbauen
      Nameservice ! {self(), {rebind, Koordinatorname, node()}},
      receive ok -> util:logging(LogFile, "Koordinator an Namensdienst rebind.done.\n")
      end;
    pang -> % logge einen fehler hier
      util:logging(LogFile, "Koordinator wurde nicht gestartet.\n")
  end.

loop(GgtProzesseNamen, GgtProzessePID, Nameservice, AktuelleKleinsteZahl, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,AktuelleGGtProzessAnzahl) ->
  receive
    % Entwurf 2.1.1
    {StarterPID, getsteeringval} ->
      util:logging(LogFile, io_lib:format("getsteeringval Anfrage von ~p.\n", [StarterPID])),
      %Rechne die Quota aus
      NewAktuelleGGtProzessAnzahl = AktuelleGGtProzessAnzahl + GgtprozessAnzahl,
      StarterPID ! {steeringval, SleepTime, XTime, round(NewAktuelleGGtProzessAnzahl * Quote / 100), GgtprozessAnzahl},
      util:logging(LogFile, io_lib:format("getsteeringval Reponse gesendet an ~p.\n", [StarterPID])),
      loop(GgtProzesseNamen, GgtProzessePID, Nameservice, AktuelleKleinsteZahl, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,NewAktuelleGGtProzessAnzahl);
    %Entwurf 2.1.2
    {hello, GGTProzessName} ->
      util:logging(LogFile, io_lib:format("hello Anfrage von ~p.\n", [GGTProzessName])),
      NewGgtProzessenNamen = [GGTProzessName | GgtProzesseNamen],
      util:logging(LogFile, io_lib:format("hello Anfrage von ~p wurde erfolgreich bearbeitet.\n", [GGTProzessName])),
      loop(NewGgtProzessenNamen, GgtProzessePID, Nameservice, AktuelleKleinsteZahl, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,AktuelleGGtProzessAnzahl);
    %Entwurf 2.1.3
    {briefmi, {Clientname, CMi, CZeit}} when AktuelleKleinsteZahl == nil ->
      util:logging(LogFile, io_lib:format("briefmi Anfrage von ~p.\n", [Clientname])),
      util:logging(LogFile, io_lib:format("ggTProzess ~p meldet ein neues Mi ~p um ~p, dies ist die erste Zahl und somit auch die kleinste Zahl. \n", [Clientname,CMi,CZeit])),
      util:logging(LogFile, io_lib:format("briefmi Anfrage von ~p wurde erfolgreich bearbeitet.\n", [Clientname])),
      loop(GgtProzesseNamen, GgtProzessePID, Nameservice, CMi, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,AktuelleGGtProzessAnzahl);
    %Entwurf 2.1.3
    {briefmi, {Clientname, CMi, CZeit}} when AktuelleKleinsteZahl > CMi ->
      util:logging(LogFile, io_lib:format("briefmi Anfrage von ~p.\n", [Clientname])),
      util:logging(LogFile, io_lib:format("ggTProzess ~p meldet ein neues Mi ~p um ~p, dies ist auch die neue kleinste Zahl. \n", [Clientname,CMi,CZeit])),
      util:logging(LogFile, io_lib:format("briefmi Anfrage von ~p wurde erfolgreich bearbeitet.\n", [Clientname])),
      loop(GgtProzesseNamen, GgtProzessePID, Nameservice, CMi, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,AktuelleGGtProzessAnzahl);
    %Entwurf 2.1.3
    {briefmi, {Clientname, CMi, CZeit}} when AktuelleKleinsteZahl =< CMi ->
      util:logging(LogFile, io_lib:format("briefmi Anfrage von ~p.\n", [Clientname])),
      util:logging(LogFile, io_lib:format("ggTProzess ~p meldet ein neues Mi ~p um ~p. \n", [Clientname,CMi,CZeit])),
      util:logging(LogFile, io_lib:format("briefmi Anfrage von ~p wurde erfolgreich bearbeitet.\n", [Clientname])),
      loop(GgtProzesseNamen, GgtProzessePID, Nameservice, AktuelleKleinsteZahl, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,AktuelleGGtProzessAnzahl);
    %Entwurf 2.1.4
    {From, briefterm, {_Clientname, CMi, CZeit}} when KorrigierungsFlag == 1, CMi > AktuelleKleinsteZahl ->
      util:logging(LogFile, io_lib:format("briefterm Anfrage von ~p, um ~p. \n", [From,CZeit])),
      util:logging(LogFile, "Koorrigierungsflag ist 1 und die gewünschte Zahl ist nicht gleich CMI, eine Korrektur wird ausgeführt, {sendy,).\n"),
      From ! {sendy,AktuelleKleinsteZahl},
      util:logging(LogFile, io_lib:format("briefterm Anfrage von ~p wurde erfolgreich bearbeitet.\n", [From])),
      loop(GgtProzesseNamen, GgtProzessePID, Nameservice, AktuelleKleinsteZahl, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,AktuelleGGtProzessAnzahl);
    %Entwurf 2.1.4
    {From, briefterm, {_Clientname, CMi, CZeit}} when CMi == AktuelleKleinsteZahl ->
      util:logging(LogFile, io_lib:format("briefterm Anfrage von ~p, um ~p. \n", [From,CZeit])),
      util:logging(LogFile, "Die gewünschte Zahl ist gleich der CMI.\n"),
      util:logging(LogFile, io_lib:format("briefterm Anfrage von ~p wurde erfolgreich bearbeitet.\n", [From])),
      loop(GgtProzesseNamen, GgtProzessePID, Nameservice, AktuelleKleinsteZahl, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,AktuelleGGtProzessAnzahl);
    %Entwurf 2.1.4
    {From, briefterm, {_Clientname, CMi, CZeit}} when CMi < AktuelleKleinsteZahl ->
      util:logging(LogFile, io_lib:format("briefterm Anfrage von ~p, um ~p. \n", [From,CZeit])),
      util:logging(LogFile, "Die gewünschte Zahl ist größer als die CMI, somit wird die CMI die neue kleinste Zahl sein.\n"),
      util:logging(LogFile, io_lib:format("briefterm Anfrage von ~p wurde erfolgreich bearbeitet.\n", [From])),
      loop(GgtProzesseNamen, GgtProzessePID, Nameservice, CMi, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,AktuelleGGtProzessAnzahl);
    %Entwurf 2.1.5
    reset ->
      util:logging(LogFile, "reset Anforderung von Client.\n"),
      %Sendet kill an alle Prozesse
      sendeKillAnGgtS(GgtProzessePID,LogFile),
      Nameservice ! {self(),reset},
      receive
        ok -> resetNamensdienstVerbindung(Nameservicenode,Koordinatorname,LogFile)
      end,
      loop([], [], Nameservice, nil, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile, Koordinatorname,Nameservicenode,0);
    %Entwurf 2.1.6
    step ->
      util:logging(LogFile, "step Anforderung von Client.\n"),
      NewGgtProzessePID = getProzessIDs(GgtProzesseNamen, GgtProzessePID, Nameservice, LogFile),
      %Baut den Ring auf
      ringAufbau(NewGgtProzessePID, getSize(NewGgtProzessePID), LogFile),
      util:logging(LogFile, "step Anforderung von Client wurde erfolgreich bearbeitet, Koordinator wartet auf Start einer ggT-Berechnung.\n"),
      loop(GgtProzesseNamen, NewGgtProzessePID, Nameservice, AktuelleKleinsteZahl, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,AktuelleGGtProzessAnzahl);
    %Entwurf 2.1.7
    prompt ->
      util:logging(LogFile, "promp Anforderung von Client.\n"),
      spawn(fun() -> getAllMi(GgtProzessePID,LogFile) end),
      loop(GgtProzesseNamen, GgtProzessePID, Nameservice, AktuelleKleinsteZahl, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,AktuelleGGtProzessAnzahl);
    %Entwurf 2.1.8
    nudge ->
      util:logging(LogFile, "nudge Anforderung von Client.\n"),
      spawn(fun() -> getAllLebenszustand(GgtProzessePID,LogFile) end),
      loop(GgtProzesseNamen, GgtProzessePID, Nameservice, AktuelleKleinsteZahl, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,AktuelleGGtProzessAnzahl);
    %Entwurf 2.1.9
    toggle ->
      util:logging(LogFile, "toggle Anforderung von Client.\n"),
      case KorrigierungsFlag of
        1 -> NewKorrigierungsFlag = 0,
          util:logging(LogFile, "toggle wurde ausgeführt und Korrigierungsflag wurde von 1 zu 0 gesetzt.\n");
        0 -> NewKorrigierungsFlag = 1,
          util:logging(LogFile, "toggle wurde ausgeführt und Korrigierungsflag wurde von 0 zu 1 gesetzt.\n")
      end,
      util:logging(LogFile, "toggle Anforderung von Client wurde erfolgreich bearbeitet.\n"),
      loop(GgtProzesseNamen, GgtProzessePID, Nameservice, AktuelleKleinsteZahl, NewKorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,AktuelleGGtProzessAnzahl);
    %Entwurf 2.1.10
    {calc, WggT} ->
      util:logging(LogFile, "calc Anforderung von Client.\n"),
      %Bestimmt die Mis liste für die Prozesse
      MisListe = vsutil:bestimme_mis(WggT, getSize(GgtProzessePID)),
      sendAllSetpm(GgtProzessePID, MisListe, LogFile),
      %Wählt n Prozesse aus
      AusgewaelteGGtProzessen = waehleGGTaus(GgtProzessePID, getSize(GgtProzessePID), LogFile),
      util:logging(LogFile, io_lib:format("Folgende ggTProzesse wurden ausgewählt ~p.\n", [AusgewaelteGGtProzessen])),
      sendy(AusgewaelteGGtProzessen, util:shuffle(MisListe), LogFile),
      util:logging(LogFile, "calc Anforderung von Client wurde erfolgreich bearbeitet.\n"),
      loop(GgtProzesseNamen, GgtProzessePID, Nameservice, AktuelleKleinsteZahl, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,AktuelleGGtProzessAnzahl);
    %Entwurf 2.1.11
    kill ->
      util:logging(LogFile, "kill Anforderung von Client.\n"),
      %Sendet eine kill Anforderung an alle Prozesse
      sendeKillAnGgtS(GgtProzessePID,LogFile),
      Nameservice ! {self(),{unbind,Koordinatorname}},
      receive
        ok -> util:logging(LogFile,"Koordinator unbind von Nameservice..done.\n")
      end,
      unregister(Koordinatorname),
      util:logging(LogFile, "Koordinator shutdown!.\n");
    _X ->
      loop(GgtProzesseNamen, GgtProzessePID, Nameservice, AktuelleKleinsteZahl, KorrigierungsFlag, GgtprozessAnzahl, SleepTime, XTime, Quote, LogFile,Koordinatorname,Nameservicenode,AktuelleGGtProzessAnzahl)
  end.


%Holt die ProzessId über Nameservice
getProzessIDs([], GgtProzessePID, _Nameservice, _LogFile) ->
  GgtProzessePID;

getProzessIDs([GGtName | Rest], GgtProzessePID, Nameservice, LogFile) ->
  Nameservice ! {self(), {lookup, GGtName}},
  receive
    not_found ->
      util:logging(LogFile, io_lib:format("Koordinator lookup über Namenservice, ggTProzess ~p not_found.\n", [GGtName]));
    {pin, {Name, Node}} ->
      util:logging(LogFile, io_lib:format("Koordinator lookup über Namenservice, ggTProzess {~p,~p} gefunden.\n", [Name, Node])),
      getProzessIDs(Rest, [{Name, Node} | GgtProzessePID], Nameservice, LogFile)
  end.

%Baut den Ring auf wenn es zwei oder mehr Prozesse gibt
ringAufbau(_GgtProzessePID, Size, LogFile) when Size < 2 ->
  util:logging(LogFile, "Ring kann nicht aufgebaut werden, da die Anzahl der Prozesse zu klein ist (weniger als zwei).");

ringAufbau(GgtProzessePID, _Size, LogFile) ->
  [{HeadName,HeadNode} | [{HeadNameSec,HeadNodeSec} | Tail]] = util:shuffle(GgtProzessePID),
  {LastHead,LastNode} = getLastElement([{HeadName,HeadNode} | [{HeadNameSec,HeadNodeSec} | Tail]]),
  setNeighbor({HeadName,HeadNode}, {LastHead,LastNode}, {HeadNameSec,HeadNodeSec}),
  util:logging(LogFile, io_lib:format("ggTProzess ~p wurde über seinen linken Nachbar ~p und rechten Nachbar ~p informiert. \n", [HeadName, LastHead, HeadNameSec])),
  aufbau({HeadName,HeadNode}, {HeadName,HeadNode}, [{HeadNameSec,HeadNodeSec} | Tail], LogFile).

aufbau({FirstElementName,FirstElementNode}, {LetztesElementName,LetztesElementNode}, [{HeadName,HeadNode} | []], LogFile) ->
  setNeighbor({HeadName,HeadNode}, {LetztesElementName,LetztesElementNode}, {FirstElementName,FirstElementNode}),
  util:logging(LogFile, io_lib:format("ggTProzess ~p wurde über seinen linken Nachbar ~p und rechten Nachbar ~p informiert. \n", [HeadName, LetztesElementName, FirstElementName])),
  util:logging(LogFile, "Ring wurde aufgebaut.\n");

aufbau({FirstElementName,FirstElementNode}, {LetztesElementName,LetztesElementNode}, [{HeadName,HeadNode} | [{HeadNameSec,HeadNodeSec} | Tail]], LogFile) ->
  setNeighbor({HeadName,HeadNode}, {LetztesElementName,LetztesElementNode}, {HeadNameSec,HeadNodeSec}),
  util:logging(LogFile, io_lib:format("ggTProzess ~p wurde über seinen linken Nachbar ~p und rechten Nachbar ~p informiert. \n", [HeadName, LetztesElementName, HeadNameSec])),
  aufbau({FirstElementName,FirstElementNode}, {HeadName,HeadNode}, [{HeadNameSec,HeadNodeSec} | Tail], LogFile).

getSize(List) ->
  getSize(List, 0).

getSize([], Size) ->
  Size;

getSize([_Head | Tail], Size) ->
  getSize(Tail, Size + 1).

%Setzt die Nachbarn von Prozessen
setNeighbor(PID, {NameL, _NodeL}, {NameR, _NodeR}) ->
  PID ! {setneighbors, NameL, NameR}.

getLastElement([Head | []]) ->
  Head;

getLastElement([_Head | Tail]) ->
  getLastElement(Tail).

%Sendet an alle Prozesse setpm
sendAllSetpm([], [], Logfile) ->
  util:logging(Logfile, "Alle setpms wurden an alle Prozesse gesendet.\n");

sendAllSetpm([], [_HeadMis | _TailMis], Logfile) ->
  util:logging(Logfile, "Alle setpms wurden an alle Prozesse gesendet.\n");

sendAllSetpm([_Head | _Tail], [], Logfile) ->
  util:logging(Logfile, "Nicht alle Prozesse haben ein setpm erhalten, da zu wenig MiNeu erstellt worden sind.\n");

sendAllSetpm([Head | Tail], [HeadMis | TailMis], Logfile) ->
  Head ! {setpm, HeadMis},
  util:logging(Logfile, io_lib:format("An ggTProzess ~p wurde ein Mi gesendet ~p mittels {setpm,MiNeu}. \n", [Head, HeadMis])),
  sendAllSetpm(Tail, TailMis, Logfile).

waehleGGTaus(_GgtProzessePID, Size, LogFile) when Size < 2 ->
  util:logging(LogFile, "Weniger als zwei ggT Prozesse vorhanden. Die ggT Prozesse können nicht ausgewählt werden. \n");

waehleGGTaus(GgtProzessePID, Size, LogFile) ->
  Anzahl = util:ceil(0.2 * Size),
  util:logging(LogFile, io_lib:format("Es werden ~p Prozesse ausgewaehlt. \n", [Anzahl])),
  getNProzesses(GgtProzessePID, Anzahl, []).

getNProzesses(_GgtProzessePID, 0, Rest) ->
  Rest;

getNProzesses([Head | Tail], Anzahl, Rest) ->
  getNProzesses(Tail, Anzahl - 1, [Head | Rest]).

%Sendet an ausgewählte Prozesse ein sendy
sendy([], _MisListe, LogFile) ->
  util:logging(LogFile, "Alle Y wurden an die Prozesse gesendet Mittels {sendy,Y}. \n");

sendy([Head | Tail], [MisHead | MisTail], LogFile) ->
  Head ! {sendy, MisHead},
  util:logging(LogFile, io_lib:format("An ggTProzess ~p wurde Mi ~p gesendet mittels  {sendy, Y}. \n", [Head, MisHead])),
  sendy(Tail, MisTail, LogFile).

%Sendet eine kill Anforderung an alle Prozesse
sendeKillAnGgtS([],LogFile) ->
  util:logging(LogFile, "An alle ggTProzesse wurde ein kill gesendet. \n");

sendeKillAnGgtS([Head|Tail],LogFile) ->
  util:logging(LogFile, io_lib:format("An ggTProzess ~p wurde eine kill Anfrage gesendet. \n", [Head])),
    Head ! kill,
    sendeKillAnGgtS(Tail,LogFile).

%Holt sich die Mi aller Prozesse
getAllMi([], LogFile) ->
  util:logging(LogFile, "Alle Mis wurden gelogt. \n");

getAllMi([{Name,Node}|Tail], LogFile) ->
  {Name,Node} ! {self(),tellmi},
  receive
    {mi,Mi} ->
      util:logging(LogFile, io_lib:format("ggTProzess ~p hat die Mi ~p. \n", [Name, Mi]))
    after 2000 ->
      util:logging(LogFile, io_lib:format("ggTProzess ~p hat nicht auf tellmi geantwortet. \n", [Name]))
  end,
  getAllMi(Tail,LogFile).

%Holt sich den Lebenszustand der Prozesse
getAllLebenszustand([],LogFile) ->
  util:logging(LogFile, "Lebenszustand aller ggTProzesses wurde gelogt. \n");

getAllLebenszustand([{Name, Node}|Tail], LogFile) ->
  {Name, Node} ! {self(),pingGGT},
  receive
    {pongGGT,GGTname} ->
      util:logging(LogFile, io_lib:format("Im Lebenszustand ~p. \n", [GGTname]))
  after 2000 ->
    util:logging(LogFile, io_lib:format("GgtProzess ~p lebt nicht mehr. \n", [Name]))
  end,
  getAllLebenszustand(Tail,LogFile).

%Entwurf 3.6
resetNamensdienstVerbindung(Nameservicenode,Koordinatorname,LogFile) ->
  case net_adm:ping(Nameservicenode) of
    pong ->
      vsutil:meinSleep(1000),
      util:logging(LogFile, "Reset: ping an Nameservice war erfolgreich.\n"),
      Nameservice = global:whereis_name(nameservice),
      Nameservice ! {self(), {rebind, Koordinatorname, node()}},
      receive ok -> util:logging(LogFile, "Reset: Koordinator an Namensdienst rebind.done.\n")
      end;
    pang -> % logge einen fehler hier
      util:logging(LogFile, "Reset: Koordinator wurde nicht gestartet.\n")
  end.