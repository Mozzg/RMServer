unit SrvClientUnit;

interface

uses ScktComp, Classes, WinSock, SysUtils, QueueTypeUnit;

type
  TClientThread = class (TServerClientThread)
  private
    FOut:TStrings;  //��������� �� ���� ��� ������ ����
    FOut_level:integer;  //������� ����������� ��� ���������� ���-�� ���������� � ����
    FOut_path:string;  //���� � ����� �����
    FQue:TQueue;  //��������� �� ������� (��� ������� � ����������� ������)
    FSocketHandle:integer;  //����� ����������� ������
    FMainDelay:cardinal;   //�������� ��������� �����

    FWasDisconnect:boolean;    //�����������, ��� �� ������ ����� ����������

    function SocketRead:integer;  //������� �������� ������ �� ���������� � ������
    function SocketWrite:integer;  //������� �������� ������ �� ���������� � ������

    procedure ClientConnect;  //���������-������� ��� ��������
    procedure ClientDisconnect;  //���������-������� ��� ����������
    procedure ClientRead;  //���������-������� ��� ������
    procedure ClientWrite;  //���������-������� ��� ������
  public
    constructor Create(ASocket: TServerClientWinSocket; pPriority:TThreadPriority; mainDelay:cardinal; output:TStrings; output_level:integer; output_path:string; Que:TQueue);
    destructor Destroy; override;

    procedure ClientExecute; override;

    procedure Log(mess:string);   //��������� ����������� � �������������� ������ �����
    procedure LogErr(mess:string);   //��������� ����������� ��� �������������� ������ ����� (��������� ������)
    function LogServerMess(path,Mess:string; time:boolean):boolean;
  end;

implementation

//----------------------TClientThread-------------------

constructor TClientThread.Create(ASocket: TServerClientWinSocket; pPriority:TThreadPriority; mainDelay:cardinal; output:TStrings; output_level:integer; output_path:string; Que:TQueue);
begin
  //������ ����� � ���������� ��� ���������
  inherited Create(true,ASocket);
  priority:=pPriority;

  FOut:=output;
  FOut_level:=output_level;
  FOut_path:=output_path;

  LogErr('OnThreadCreate   ExistingHandle='+inttostr(FSocketHandle)+'   NewHandle='+inttostr(ClientSocket.SocketHandle));

  //������������ ����� ������, ���� ���� ������� � ������� ��������� � ����
  FSocketHandle:=ClientSocket.SocketHandle;

  //������������� ����������
  FMainDelay:=mainDelay;
  FQue:=Que;
  FWasDisconnect:=false;

  //��������� �����
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

  //�������� �� ������
  if result=SOCKET_ERROR then exit;

  //�������� �� ������������ � ����������� ������
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

  //�������� �� ������
  if result=SOCKET_ERROR then exit;

  //�������� �� ����������� ������
  if (Terminated) then
  begin
    result:=-2;
    exit;
  end;

  if result>0 then result:=1;
end;

procedure TClientThread.ClientExecute;
begin
  //������������ ����� ������ �����, �.�. ���� ����� ����� �������������� ��� ������ ������� ���������
  FSocketHandle:=ClientSocket.SocketHandle;

  LogErr('ClientThread#'+inttostr(FSocketHandle)+' execute enter');

  //������ ��� ������� �� �������
  if ClientSocket.Connected then ClientConnect;

  while (not(Terminated)) and (ClientSocket.Connected) do
  begin
    //�������� �� ���������� �����
    if ClientSocket.SocketHandle=INVALID_SOCKET then
    begin
      Log('Invalid socket before read');
      ClientDisconnect;
      break;
    end;

    //��������� ����� �� ������
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

    //�������� �� ���������� �����
    if ClientSocket.SocketHandle=INVALID_SOCKET then
    begin
      Log('Invalid socket before write');
      ClientDisconnect;
      break;
    end;

    //��������� ����� �� ������
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

  //�������� �� �������� �����
  if ClientSocket.SocketHandle<>INVALID_SOCKET then
  begin
    LogErr('WARNING! ClientThread#'+inttostr(FSocketHandle)+'/'+inttostr(ClientSocket.SocketHandle)+' has open socket, closing');

    ClientDisconnect;
  end;

  //�������� �� ����� ������� �� ���������
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
