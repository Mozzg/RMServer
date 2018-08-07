unit SrvClientUnit;

interface

uses ScktComp, Classes, WinSock, SysUtils, QueueTypeUnit;

type
  TClientThread = class (TServerClientThread)
  private
    FOut:TStrings;  //указатель на мемо дл€ вывода лога
    FOut_level:integer;  //уровень логировани€ дл€ уменьшени€ кол-ва информации в логе
    FOut_path:string;  //путь к файлу логов
    FQue:TQueue;  //указатель на очередь (дл€ доступа к критической секции)
    FSocketHandle:integer;  //хендл клиентского сокета
    FMainDelay:cardinal;   //задержка основного цикла

    FWasDisconnect:boolean;    //определение, был ли вызван эвент дисконекта

    function SocketRead:integer;  //функци€ проверки сокета на готовность к чтению
    function SocketWrite:integer;  //функци€ проверки сокета на готовность к записи

    procedure ClientConnect;  //процедура-событие дл€ коннекта
    procedure ClientDisconnect;  //процедура-событие дл€ дисконекта
    procedure ClientRead;  //процедура-событие дл€ чтени€
    procedure ClientWrite;  //процедура-событие дл€ записи
  public
    constructor Create(ASocket: TServerClientWinSocket; pPriority:TThreadPriority; mainDelay:cardinal; output:TStrings; output_level:integer; output_path:string; Que:TQueue);
    destructor Destroy; override;

    procedure ClientExecute; override;

    procedure Log(mess:string);   //процедура логировани€ с использованием уровн€ логов
    procedure LogErr(mess:string);   //процедура логировани€ без использованием уровн€ логов (выводитс€ всегда)
    function LogServerMess(path,Mess:string; time:boolean):boolean;
  end;

implementation

//----------------------TClientThread-------------------

constructor TClientThread.Create(ASocket: TServerClientWinSocket; pPriority:TThreadPriority; mainDelay:cardinal; output:TStrings; output_level:integer; output_path:string; Que:TQueue);
begin
  //создаЄм поток и выставл€ем ему приоритет
  inherited Create(true,ASocket);
  priority:=pPriority;

  FOut:=output;
  FOut_level:=output_level;
  FOut_path:=output_path;

  LogErr('OnThreadCreate   ExistingHandle='+inttostr(FSocketHandle)+'   NewHandle='+inttostr(ClientSocket.SocketHandle));

  //переписываем хендл сокета, чтоб было удобнее и быстрее орбащатс€ к нему
  FSocketHandle:=ClientSocket.SocketHandle;

  //инициализаци€ переменных
  FMainDelay:=mainDelay;
  FQue:=Que;
  FWasDisconnect:=false;

  //запускаем поток
  Resume;
end;

destructor TClientThread.Destroy;
begin
  FOut:=nil;

  inherited Destroy;
end;

procedure TClientThread.ClientConnect;
begin
  //Log('ClientThread#'+inttostr(FSocketHandle)+'/'+inttostr(ClientSocket.SocketHandle)+' OnConnect');

  FQue.SocketConnect(nil,ClientSocket);
end;

procedure TClientThread.ClientDisconnect;
begin
  //Log('ClientThread#'+inttostr(FSocketHandle)+'/'+inttostr(ClientSocket.SocketHandle)+' OnDisconnect');

  if ClientSocket.SocketHandle=INVALID_SOCKET then
    FQue.SocketDisconnect(nil,FSocketHandle)
  else
    FQue.SocketDisconnect(nil,ClientSocket.SocketHandle);

  ClientSocket.Disconnect(ClientSocket.SocketHandle);

  FWasDisconnect:=true;
end;

procedure TClientThread.ClientRead;
//var i:integer;
begin
  //i:=ClientSocket.ReceiveBuf(FReadBuffer,1024);
  //i:=-1;

  //Log('ClientThread#'+inttostr(FSocketHandle)+' OnRead with result='+inttostr(i));

  FQue.SocketRead(nil,ClientSocket);
end;

procedure TClientThread.ClientWrite;
begin
  //Log('ClientThread#'+inttostr(FSocketHandle)+'/'+inttostr(ClientSocket.SocketHandle)+' OnWrite');
end;


//return values:
//-1 Error
//-2 Disconnect/ThreadTerminate
//0 Timeout
//1 ReadyToRead
function TClientThread.SocketRead:integer;
var FDSet: TFDSet;
TimeVal: TTimeVal;
begin
  FD_ZERO(FDSet);
  FD_SET(ClientSocket.SocketHandle, FDSet);
  TimeVal.tv_sec := 0;
  TimeVal.tv_usec := 500;
  result:=select(0, @FDSet, nil, nil, @TimeVal);

  //проверка на ошибку
  if result=SOCKET_ERROR then exit;

  //проверка на отсоединение и уничтожение потока
  if result>0 then
    if (ClientSocket.ReceiveBuf(FDSet, -1) = 0)or(Terminated) then
    begin
      result:=-2;
      exit;
    end;

  if result>0 then result:=1;
end;


//return values:
//-1 Error
//-2 ThreadTerminate
//0 Timeout
//1 ReadyToWrite
function TClientThread.SocketWrite:integer;
var FDSet: TFDSet;
TimeVal: TTimeVal;
begin
  FD_ZERO(FDSet);
  FD_SET(ClientSocket.SocketHandle, FDSet);
  TimeVal.tv_sec := 0;
  TimeVal.tv_usec := 500;
  result:=select(0, nil, @FDSet, nil, @TimeVal);

  //проверка на ошибку
  if result=SOCKET_ERROR then exit;

  //проверка на уничтожение потока
  if (Terminated) then
  begin
    result:=-2;
    exit;
  end;

  if result>0 then result:=1;
end;

procedure TClientThread.ClientExecute;
begin
  //переписываем хендл сокета снова, т.к. этот поток может использоватьс€ дл€ разных сокетов поочереди
  FSocketHandle:=ClientSocket.SocketHandle;

  LogErr('ClientThread#'+inttostr(FSocketHandle)+' execute enter');

  //делаем своЄ событие на коннект
  if ClientSocket.Connected then ClientConnect;

  while (not(Terminated)) and (ClientSocket.Connected) do
  begin
    //проверка на правильный сокет
    if ClientSocket.SocketHandle=INVALID_SOCKET then
    begin
      Log('Invalid socket before read');
      ClientDisconnect;
      break;
    end;

    //провер€ем сокет на чтение
    case SocketRead of
      -1:begin
           LogErr('ClientThread#'+inttostr(FSocketHandle)+' Error on attempting to read with errorcode='+inttostr(WSAGetLastError));
         end;
      -2:begin
           ClientDisconnect;
           break;
         end;
      0:begin
        end;
      1:begin
          ClientRead;
        end;
    end;

    //проверка на правильный сокет
    if ClientSocket.SocketHandle=INVALID_SOCKET then
    begin
      Log('Invalid socket before write');
      ClientDisconnect;
      break;
    end;

    //провер€ем сокет на запись
    case SocketWrite of
      -1:begin
           LogErr('ClientThread#'+inttostr(FSocketHandle)+' Error on attempting to read with errorcode='+inttostr(WSAGetLastError));
         end;
      -2:begin
           ClientDisconnect;
           break;
         end;
      0:begin
        end;
      1:begin
          ClientWrite;
        end;
    end;


    sleep(FMainDelay);
  end;  //while (not(Terminated)) and (ClientSocket.Connected) do

  LogErr('ClientThread#'+inttostr(FSocketHandle)+'/'+inttostr(ClientSocket.SocketHandle)+' execute exit');

  //проверка на закрытый сокет
  if ClientSocket.SocketHandle<>INVALID_SOCKET then
  begin
    LogErr('WARNING! ClientThread#'+inttostr(FSocketHandle)+'/'+inttostr(ClientSocket.SocketHandle)+' has open socket, closing');

    ClientDisconnect;
  end;

  //проверка на вызов событи€ на дисконект
  if FWasDisconnect=false then
  begin
    LogErr('There was no disconnect event, executing event');

    ClientDisconnect;
  end;
end;

procedure TClientThread.Log(mess:string);
begin
  if (FOut_level>=0) then
  begin
    LogServerMess(FOut_path,mess,true);
    if FOut<>nil then
      FOut.Add(FormatDateTime('hh:nn:ss.zzz',now)+'  '+mess);
  end;
end;

procedure TClientThread.LogErr(mess:string);
begin
  LogServerMess(FOut_path,mess,true);
  if (FOut<>nil) then
    FOut.Add(FormatDateTime('hh:nn:ss.zzz',now)+'  '+mess);
end;

function TClientThread.LogServerMess(path,Mess:string; time:boolean):boolean;
var handl:integer;
temp_mess:string;
begin
  result:=false;
  if time=true then temp_mess:=FormatDateTime('dd.mm.yyyy hh:nn:ss.zzz',now)+'  '+Mess+#13+#10
  else temp_mess:=Mess+#13+#10;
  
  if path<>'' then
  begin
    if FileExists(path) then
      handl:=FileOpen(path,fmOpenReadWrite or fmShareDenyNone)
    else
    begin
      handl:=FileCreate(path);
      temp_mess:=datetimetostr(now)+#13+#10+temp_mess;
    end;

    if handl<0 then exit;
    if FileSeek(handl,0,2)=-1 then exit;
    if FileWrite(handl,temp_mess[1],length(temp_mess))=-1 then exit;
    FileClose(handl);
  end
  else
  begin
    temp_mess:='';
    exit;
  end;

  temp_mess:='';
  result:=true;
end;

//======================TClientThread===================

end.
