unit QueueTypeUnit;

interface

uses ScktComp, Classes, Windows, typesUnit, TransferServerTypeUnit;

type  
//QUEUE
  TQueue = class(TThread)
  private
    FServer:TServerSocket;  //������ �����
    FServerShutdown:boolean;   //����������, ������������ ���������� �� ������� � ��������� ����������
    FOut:TStrings;   //���� ��� ������ ����������
    FOut_path:string;    //���� � ����� �����
    FCritSection:TRTLCriticalSection;  //����������� ������ ��� ������� � �������
    FRegisterThread_arr:array of TRegisterThreadElement;   //������ ������������������ ������� ���������, ����� ��� �������� ������ (��������� �����-������)
    FRegisterSocket_arr:array of integer;    //������ ��� ������������������ �������
    FCoresp_arr:array of TCorespElement;    //����� ��� �������� ����������� �������� �����-����� ��� �������, ������ ������
    FTransferPort:integer;   //������������ ���� ��� �������� �� �� �������� ��� �������� ������

    //���������� ��� ���������� �������� ��� ������������� ��������
    //FSemaphore:TConnectSemaphore;

    //���������� ��� �������� ��������
    FirstTrans:PTTransferElement;   //��������� �� ������ ������� �������
    FTransCount:integer;    //������� ���-�� �������� ��������
    FID_counter:cardinal;   //������� ��� ������������ ����������� ID �������� ��������
    TransferUpdateTime:int64;    //����� �������� �������� ��������

    //���������� ��� �������
    First,Last:PTQueueElement;  //��������� �� ������ � ��������� ������ �������
    FQueueCount:integer;    //������� ���-�� ��������� � �������
    //���������� ��� �������������� �������
    FirstExt,LastExt:PTQueueElement;  //��������� �� ������ � ��������� ������ �������
    //������� ��� �������� �������������� ������� ��� �������� ����������� ������
    procedure CheckAndLeaveCriticalSection;

    //��������� ������ ���������� �� ������� ����
    procedure Log(message:string);
    function LogServerMess(path,Mess:string; time:boolean):boolean;
  public
    constructor Create(port:integer; output:TStrings; OpenOnCreate:boolean; output_path:string);
    destructor Destroy; override;

    procedure Execute; override;

    procedure Shutdown;

    //��������� ��� ������ � �������� ���������
    function RegisterThread(ThrID:cardinal; op:boolean):boolean;
    function RemoveThread(ThrID:cardinal):boolean;
    function GetNewServerPort:integer;
    procedure LogThreads;

    //��������� ��� ������ � ��������
    procedure OpenServer;  //�������� ����� �������
    function RegisterSocket(SockID:integer; Check_CritSection:boolean; CreateStatic:boolean):boolean; overload; //�������� ������ � ������, � �������� ������ ����� �������
    function RegisterSocket(SockID:integer; ThrID:cardinal):boolean; overload; //�������� ������ ������ � ������������� ������
    function RegisterOPSocket(SockID:integer):boolean;  //�������� ������ � ������, ������� ������������ ����������
    function RemoveSocket(SockID:integer):boolean;
    function RegisterStaticSocket(SockID:integer; ThrID:cardinal):boolean;
    function RemoveStaticSocket(SockID:integer; ThrID:cardinal):boolean;
    function DisconnectSocket(SockID:integer):boolean;
    procedure CreateDisconnectElement(SockID:integer);
    function GetSocketIP(SockID:integer):string;
    function SendPacket(SockID:integer; buf:byte_arr):boolean;
    procedure LogSockets;
    procedure LogInetSockets;
    function GetISocketCount:integer;

    //��������� ���������
    function CleanSocketArrays:integer;
    procedure CreateTransferTest;

    //��������� ��� ������ � ��������
    function Push(buf:Byte_arr; SockID:integer; Time:TDateTime; Check_CritSection:boolean):boolean; overload;
    function Push(buf:Byte_arr; Size,SockID:integer; Time:TDateTime; Check_CritSection:boolean):boolean; overload;
    function Pop(ThrID:cardinal; var buf:Byte_arr; var SockID:integer; var Time:TDateTime):boolean;
    procedure LogQueue;
    function ElementCount:integer;

    //��������� ��� ������ � �������� �������� ��������
    procedure CreateTransferElement(Ins:TTransferServerInstructions; ptr:PTThreadSocketElement);
    procedure AddTransferElement(el:TTransferServer);
    function GetTransferServer(ID:cardinal):TTransferServer;
    function GetTransferServerCount:integer;
    procedure CheckTransferServers;

    //���������-������� ��� ������
    procedure SocketListen(Sender: TObject; Socket: TCustomWinSocket);
    procedure SocketConnect(Sender: TObject; Socket: TCustomWinSocket);
    procedure SocketDisconnect(Sender: TObject; Socket: integer);
    procedure SocketRead(Sender: TObject; Socket: TCustomWinSocket);
    procedure SocketError(Sender: TObject; Socket: TCustomWinSocket; ErrorEvent: TErrorEvent; var ErrorCode: Integer);
    procedure SocketAccept(Sender: TObject; Socket: TCustomWinSocket);
    procedure SocketGetThread(Sender: TObject; ClientSocket: TServerClientWinSocket; var SocketThread: TServerClientThread);
  end;


implementation

uses SysUtils, SrvClientUnit, DateUtils;

//----------------------TQueue-------------------
constructor TQueue.Create(port:integer; output:TStrings; OpenOnCreate:boolean; output_path:string);
begin
  inherited Create(true);

  FreeOnTerminate:=false;

  //����� ���� ��� ����
  FOut:=output;
  //����� ���� �����
  FOut_path:=output_path;
  Log('=====================================================================================');
  Log('Creating Queue begins at '+datetimetostr(now));
  //������� ���������
  First:=nil;
  Last:=nil;
  FirstExt:=nil;
  LastExt:=nil;
  FirstTrans:=nil;
  //������������� ����������
  FTransferPort:=2000;
  FServerShutdown:=false;
  setlength(FRegisterThread_arr,0);
  setlength(FRegisterSocket_arr,0);
  setlength(FCoresp_arr,0);
  FID_counter:=1;
  FTransCount:=0;
  FQueueCount:=0;
  TransferUpdateTime:=gettickcount64;
  //FSemaphore.Active:=false;
  //FSemaphore.SockID:=0;
  //FSemaphore.SavedAsyncStyles:=[asAccept];
  //������ �����
  FServer:=TServerSocket.Create(nil);
  FServer.Port:=port;
  FServer.ServerType:=stThreadBlocking;
  FServer.ThreadCacheSize:=0;
  FServer.OnListen:=SocketListen;
  {FServer.OnClientConnect:=SocketConnect;
  FServer.OnClientDisconnect:=SocketDisconnect;
  FServer.OnClientRead:=SocketRead; }
  FServer.OnClientError:=SocketError;
  FServer.OnGetThread:=SocketGetThread;
  //FServer.OnAccept:=SocketAccept;
  Log('Created socket with port:'+inttostr(port));
  if OpenOnCreate then
  begin
    FServer.Open;
    Resume;
    Log('Opened socket on port:'+inttostr(port));
  end;
  //������ ����������� ������
  InitializeCriticalSectionAndSpinCount(FCritSection,$1000);
  Log('Critical section created');
end;

//todo: ���������� � ����� � ������������ �������� � �������� ���� �������
destructor TQueue.Destroy;
var i:integer;
p:PTQueueElement;
pTrans:PTTransferElement;
begin
  Log('Starting to destroy queue ++++++++++++++++======================+++++++++++++++++++');

  FServer.Close;
  FServer.Free;

  //������� �������
  p:=First;
  while p<>nil do
  begin
    setlength(p^.data,0);
    First:=p^.NextElement;
    Dispose(p);
    p:=First;
  end;
  First:=nil;
  Last:=nil;
  //������� �������������� �������
  p:=FirstExt;
  while p<>nil do
  begin
    setlength(p^.data,0);
    FirstExt:=p^.NextElement;
    Dispose(p);
    p:=FirstExt;
  end;
  FirstExt:=nil;
  LastExt:=nil;
  //������� ������� �������� ��������
  pTrans:=FirstTrans;
  while pTrans<>nil do
  begin
    if pTrans.TransferServer<>nil then
    begin
      pTrans.TransferServer.Terminate;
      pTrans.TransferServer.WaitFor;
      pTrans.TransferServer.Free;
      pTrans.TransferServer:=nil;
    end;
    FirstTrans:=pTrans.NextElement;
    Dispose(pTrans);
    pTrans:=FirstTrans;
  end;
  FirstTrans:=nil;

  DeleteCriticalSection(FCritSection);

  for i:=0 to length(FRegisterThread_arr)-1 do
    setlength(FRegisterThread_arr[i].SocketID_arr,0);
  setlength(FRegisterThread_arr,0);
  setlength(FCoresp_arr,0);

  Log('Destroy queue complete ===================================================');
end;

procedure TQueue.Shutdown;
var i:integer;
buf:array of byte;
begin
  EnterCriticalSection(FCritSection);
  //��������� ����� ���������� �������
  setlength(buf,11);
  Fillchar(buf[0],11,0);
  buf[0]:=158;  //������ ������ ������
  buf[4]:=6;  //������ ������ ������
  buf[6]:=8;  //���� ������ (0008h)

  FServerShutdown:=true;

  //���������� ������
  for i:=0 to FServer.Socket.ActiveConnections-1 do
    FServer.Socket.Connections[i].SendBuf(buf[0],length(buf));

  Log('Shutdown packets send');

  setlength(buf,0);

  LeaveCriticalSection(FCritSection);   //���� ���, ����� ��������� �� �������, �.�. �� ���������� ������� ����� OnDisconnect ������, ������� ���� ����������� ������
  FServer.Close;
  
  Log('Server stopped');
end;

procedure TQueue.OpenServer;
begin
  if FServer.Active=false then
  begin
    FServer.Open;
    Resume;
    Log('Opened socket on port:'+inttostr(FServer.Port));
  end
  else
    Log('Trying to start a server when it is already started');
end;

procedure TQueue.CheckAndLeaveCriticalSection;
var p:PTQueueElement;
str:string;
i,j:integer;
begin
  LeaveCriticalSection(FCritSection);

  if FirstExt<>nil then
  begin
    Log('WARNING!WARNING!WARNING! Entered exception section when exiting critical section');

    Log('Logging backup elements');
    p:=FirstExt;
    i:=1;
    while p<>nil do
    begin
      str:='Element #'+inttostr(i)+'   ID='+inttostr(p^.SocketID)+#13+#10;
      for j:=0 to length(p^.data)-1 do
        str:=str+'   #'+inttostr(j)+'='+inttostr(p^.data[j])+#13+#10;

      Log(str);
      p:=p^.NextElement;
      inc(i);
    end;

    Log('Queue count before processing='+inttostr(FQueueCount));

    p:=FirstExt;
    while p<>nil do
    begin
      //���������, ��� �������� ������ ������ (SocketConnect) ��� ������ ����� (SocketDisconnect, SocketRead)
      if length(p^.data)>1 then
      begin  //��� ����� ������
        Push(p^.data,p^.SocketID,now,false);
      end
      else
      begin  //��� �������� ������ ������
        RegisterSocket(p^.SocketID,false,true);
      end;

      setlength(p^.data,0);
      FirstExt:=p^.NextElement;
      Dispose(p);
      p:=FirstExt;
    end;

    Log('Queue count after processing='+inttostr(FQueueCount));
    if FirstExt=nil then Log('FirstExt=nil')
    else Log('FirstExt<>nil');
  end;
end;

function TQueue.Push(buf:Byte_arr; SockID:integer; Time:TDateTime; Check_CritSection:boolean):boolean;
var p:PTQueueElement;
begin
  EnterCriticalSection(FCritSection);

  result:=false;
  try
    //������� ������� � ��������� ��� ����
    New(p);
    p^.SocketID:=SockID;
    p^.ReceiveTime:=Time;
    p^.NextElement:=nil;
    setlength(p^.data,length(buf));
    move(buf[0],p^.data[0],length(buf));

    //���� ��� ������ ������� � �������
    if First=nil then
    begin
      First:=p;
      Last:=p;
      Inc(FQueueCount);
    end
    else  //���� � ������� ��� ���-�� ����
    begin
      Last^.NextElement:=p;
      Last:=p;
      Inc(FQueueCount);
    end;
  except
    on e:exception do
    begin
      Log('Exception in push method with message:'+e.Message);
      if Check_CritSection then CheckAndLeaveCriticalSection
      else LeaveCriticalSection(FCritSection);
      exit;
    end;
  end;
  result:=true;
  if Check_CritSection then CheckAndLeaveCriticalSection
  else LeaveCriticalSection(FCritSection);
end;

function TQueue.Push(buf:Byte_arr; Size,SockID:integer; Time:TDateTime; Check_CritSection:boolean):boolean;
var p:PTQueueElement;
begin
  EnterCriticalSection(FCritSection);

  result:=false;
  try
    //������� ������� � ��������� ��� ����
    New(p);
    p^.SocketID:=SockID;
    p^.ReceiveTime:=Time;
    p^.NextElement:=nil;
    setlength(p^.data,Size);
    move(buf[0],p^.data[0],size);

    //���� ��� ������ ������� � �������
    if First=nil then
    begin
      First:=p;
      Last:=p;
      Inc(FQueueCount);
    end
    else  //���� � ������� ��� ���-�� ����
    begin
      Last^.NextElement:=p;
      Last:=p;
      Inc(FQueueCount);
    end;
  except
    on e:exception do
    begin
      Log('Exception in push method with message:'+e.Message);
      if Check_CritSection then CheckAndLeaveCriticalSection
      else LeaveCriticalSection(FCritSection);
      exit;
    end;
  end;
  result:=true;
  if Check_CritSection then CheckAndLeaveCriticalSection
  else LeaveCriticalSection(FCritSection);
end;

function TQueue.Pop(ThrID:cardinal; var buf:Byte_arr; var SockID:integer; var Time:TDateTime):boolean;
var DynInd,i:integer;
curSock:integer;
p,p_prev:PTQueueElement;
b:boolean;
label ex1;
begin
  EnterCriticalSection(FCritSection);
  result:=false;

  //���� ����� � ����������� �������
  DynInd:=-1;
  for i:=0 to length(FRegisterThread_arr)-1 do
    if FRegisterThread_arr[i].ThreadID=ThrID then
    begin
      DynInd:=i;
      break;
    end;

  if DynInd=-1 then  //���� ��� ������ ������, �� �� �� ���������������
  begin
    CheckAndLeaveCriticalSection;
    exit;
  end;

  //��� �� �������
  p:=First;
  if p=nil then goto ex1;  //������� �����
  p_prev:=nil;

  while p<>nil do
  begin
    curSock:=p^.SocketID;
    //���� ������� ����� � ����������� ������
    b:=false;
    for i:=0 to length(FCoresp_arr)-1 do
      if FCoresp_arr[i].SocketID=curSock then
        if FCoresp_arr[i].ThreadID=ThrID then  //������ ������� ����� ���������, �.�. �� ��� ����� � ����������� ������
        begin
          SockID:=curSock;
          Time:=p^.ReceiveTime;
          setlength(buf,length(p^.data));
          move(p^.data[0],buf[0],length(buf));
          //������� ������� ������� �������
          setlength(p^.data,0);
          if p<>First then p_prev^.NextElement:=p^.NextElement
          else First:=p^.NextElement;
          if p^.NextElement=nil then Last:=p_prev;
          Dispose(p);
          Dec(FQueueCount);
          result:=true;
          goto ex1;
        end
        else   //������ ������� ���� ����������, �.�. �� �����, ��� �� �������� � ������� ������
        begin
          b:=true;
          break;
        end;

    if b=true then  //���� ���������� �������
    begin
      p_prev:=p;
      p:=p^.NextElement;
      continue;
    end;

    //�� �������� ���� ������ ���� �� ����� ������� ����� � ����������� ������
    //��������� ������������ ����� ��� ������� ������
    for i:=0 to length(FRegisterThread_arr[DynInd].SocketID_arr)-1 do
      if FRegisterThread_arr[DynInd].SocketID_arr[i]=curSock then
      begin  //�� ����� ������� � ������������ �������, ��������� ���
        SockID:=curSock;
        Time:=p^.ReceiveTime;
        setlength(buf,length(p^.data));
        move(p^.data[0],buf[0],length(buf));
        //������� ������� ������� �������
        setlength(p^.data,0);
        if p<>First then p_prev^.NextElement:=p^.NextElement
        else First:=p^.NextElement;
        if p^.NextElement=nil then Last:=p_prev;
        Dispose(p);
        Dec(FQueueCount);
        result:=true;
        goto ex1;
      end;

    //�� ����� ������������, ��� ������ �� �������
    p_prev:=p;
    p:=p^.NextElement;
  end;

  //�������� ����, ���� �� ����� ����������� �������� ��� ��������, ������� �� ������� � ���������� false
ex1:
  CheckAndLeaveCriticalSection;
end;

procedure TQueue.LogQueue;
var p:PTQueueElement;
i,j:integer;
str_t,str:string;
begin
  EnterCriticalSection(FCritSection);

  Log('Number of elements='+inttostr(FQueueCount));
  p:=First;
  i:=1;
  while p<>nil do
  begin
    str:=#13+#10;
    for j:=0 to length(p^.data)-1 do
      str:=str+'#'+inttostr(j)+'='+inttostr(p^.data[j])+#13+#10;
    //Log('   #'+inttostr(j)+'='+inttostr(p^.data[j]));

    str_t:='';
    datetimetostring(str_t,'hh:nn:ss.zzz',p^.ReceiveTime);
    //Log('Element #'+inttostr(i)+'  ID='+inttostr(p^.SocketID)+'  BufLength='+inttostr(length(p^.data))+'    Buf='+str+'    Time='+str_t);
    Log('Element #'+inttostr(i)+'  ID='+inttostr(p^.SocketID)+'  BufLength='+inttostr(length(p^.data))+'    Time='+str_t);
    Log(str);
    p:=p^.NextElement;
    inc(i);
  end;

  CheckAndLeaveCriticalSection;
end;

function TQueue.ElementCount:integer;
begin
  result:=FQueueCount;
end;

function TQueue.RegisterThread(ThrID:cardinal; op:boolean):boolean;
var i,j:integer;
begin
  result:=false;

  EnterCriticalSection(FCritSection);
  Log('Entered register thread, with id='+inttostr(ThrID));
  if op then Log('New thread is an operator thread');

  j:=length(FRegisterThread_arr);
  if j=0 then
  begin
    setlength(FRegisterThread_arr,1);
    FRegisterThread_arr[0].ThreadID:=ThrID;
    FRegisterThread_arr[0].Operator:=op;
    setlength(FRegisterThread_arr[0].SocketID_arr,0);
  end
  else
  begin
    //���� ���������� � ������� �������
    for i:=0 to j-1 do
      if FRegisterThread_arr[i].ThreadID=ThrID then
      begin
        CheckAndLeaveCriticalSection;
        exit;
      end;

    //���� ������ ������ ���, �� ��������� ���
    setlength(FRegisterThread_arr,j+1);
    FRegisterThread_arr[j].ThreadID:=ThrID;
    FRegisterThread_arr[j].Operator:=op;
    setlength(FRegisterThread_arr[j].SocketID_arr,0);
  end;

  Log('Register thread sucsessful');

  CheckAndLeaveCriticalSection;

  result:=true;
end;

function TQueue.RemoveThread(ThrID:cardinal):boolean;
var i,j,k:integer;
b:boolean;
temp_buf:array of integer;
begin
  result:=false;

  EnterCriticalSection(FCritSection);

  j:=length(FRegisterThread_arr);
  if j<=1 then
  begin
    CheckAndLeaveCriticalSection;
    exit;
  end;

  //����������, ���� �� ����� �������
  b:=false;
  i:=0;
  while i<j do
  begin
    if FRegisterThread_arr[i].ThreadID=ThrID then
    begin
      b:=true;
      break;
    end;
    inc(i);
  end;

  if b=false then
  begin
    CheckAndLeaveCriticalSection;
    exit;
  end;

  //���� ����, �� �������� ��� ������ �������
  j:=length(FRegisterThread_arr[i].SocketID_arr);
  setlength(temp_buf,j);
  move(FRegisterThread_arr[i].SocketID_arr[0],temp_buf[0],j*sizeof(integer));

  //������� ����� �� ������� (�������� ���� ����������� ������)
  j:=length(FRegisterThread_arr);
  inc(i);
  while i<j do
  begin
    FRegisterThread_arr[i-1].ThreadID:=FRegisterThread_arr[i].ThreadID;
    setlength(FRegisterThread_arr[i-1].SocketID_arr,length(FRegisterThread_arr[i].SocketID_arr));
    for k:=0 to length(FRegisterThread_arr[i].SocketID_arr)-1 do
      FRegisterThread_arr[i-1].SocketID_arr[k]:=FRegisterThread_arr[i].SocketID_arr[k];
    inc(i);
  end;
  setlength(FRegisterThread_arr[j-1].SocketID_arr,0);
  setlength(FRegisterThread_arr,j-1);

  //����������������� �������
  j:=0;
  for i:=0 to length(temp_buf)-1 do
  begin
    if j>(length(FRegisterThread_arr)-1) then j:=0;

    k:=length(FRegisterThread_arr[j].SocketID_arr);
    setlength(FRegisterThread_arr[j].SocketID_arr,k+1);
    FRegisterThread_arr[j].SocketID_arr[k]:=temp_buf[i];

    inc(j);
  end;

  //������� ������ � ����������� ������� ���� ��� ����
  i:=0;
  while i<length(FCoresp_arr)-1 do
  begin
    if FCoresp_arr[i].ThreadID=ThrID then
    begin
      move(FCoresp_arr[i+1],FCoresp_arr[i],(length(FCoresp_arr)-i-1)*sizeof(TCorespElement));
      setlength(FCoresp_arr,length(FCoresp_arr)-1);
      continue;
    end;

    inc(i);
  end;

  CheckAndLeaveCriticalSection;

  result:=true;
end;

function TQueue.GetNewServerPort:integer;
var i:integer;
begin
  EnterCriticalSection(FCritSection);
  i:=FTransferPort;
  if i>=3000 then i:=2001
  else inc(i);

  FTransferPort:=i;
  result:=i;
  CheckAndLeaveCriticalSection;
end;

procedure TQueue.LogThreads;
var i,j:integer;
begin
  EnterCriticalSection(FCritSection);
  Log('====================================');
  Log('Thread array:');
  for i:=0 to length(FRegisterThread_arr)-1 do
  begin
    log(' #'+inttostr(i)+'  ThreadID='+inttostr(FRegisterThread_arr[i].ThreadID));
    if FRegisterThread_arr[i].Operator then log('   Operator Thread! *************');
    log('   SocketCount='+inttostr(length(FRegisterThread_arr[i].SocketID_arr)));
    log('     Sockets:');
    for j:=0 to length(FRegisterThread_arr[i].SocketID_arr)-1 do
      log('      '+inttostr(FRegisterThread_arr[i].socketid_arr[j]));
  end;
  Log('-------------------------------');
  Log('Static socket array:');
  for i:=0 to length(FCoresp_arr)-1 do
  begin
    log(' #'+inttostr(i)+'  ThreadID='+inttostr(FCoresp_arr[i].ThreadID)+'    SocketID='+inttostr(FCoresp_arr[i].SocketID)+'   Time='+timetostr(FCoresp_arr[i].Time));
  end;
  //Log('-------------------------------');
  //Log('Semaphore:  Active='+booltostr(FSemaphore.Active,true)+'   SockID='+inttostr(FSemaphore.SockID));
  Log('====================================');
  CheckAndLeaveCriticalSection;
end;

procedure TQueue.LogSockets;
var i:integer;
begin
  EnterCriticalSection(FCritSection);
  Log('************************************');
  Log('Registered sockets:');
  for i:=0 to length(FRegisterSocket_arr)-1 do
    log(' #'+inttostr(i)+'='+inttostr(FRegisterSocket_arr[i]));
  Log('************************************');
  CheckAndLeaveCriticalSection;
end;

procedure TQueue.LogInetSockets;
var i:integer;
begin
  EnterCriticalSection(FCritSection);
  Log('++++++++++++++++++++++++++++++++++++');
  Log('Connected sockets:');
  for i:=0 to FServer.Socket.ActiveConnections-1 do
    log(' #'+inttostr(i)+':  ID='+inttostr(FServer.Socket.Connections[i].SocketHandle)+'  IP='+FServer.Socket.Connections[i].RemoteAddress+':'+inttostr(FServer.Socket.Connections[i].RemotePort));
  Log('++++++++++++++++++++++++++++++++++++');
  CheckAndLeaveCriticalSection;
end;

function TQueue.GetISocketCount:integer;
begin
  result:=FServer.Socket.ActiveConnections;
end;

function TQueue.RegisterSocket(SockID:integer; Check_CritSection:boolean; CreateStatic:boolean):boolean;
var i,j,k,t:integer;
buf:byte_arr;
begin
  result:=false;

  EnterCriticalSection(FCritSection);
  Log('Entered register socket');

  //����, ��� �� ��� ������ ������
  j:=length(FRegisterSocket_arr);
  for i:=0 to j-1 do
    if FRegisterSocket_arr[i]=SockID then
    begin
      if Check_CritSection then CheckAndLeaveCriticalSection
      else LeaveCriticalSection(FCritSection);
      exit;
    end;

  //�������, ���� �� ����� �� �������� ��� ����������� ������
  i:=-1;
  for k:=0 to length(FRegisterThread_arr)-1 do
    if FRegisterThread_arr[k].Operator=false then
    begin
      i:=k;
      break;
    end;
  if i=-1 then
  begin
    Log('No non-Operator thread found, exiting register socket');
    if Check_CritSection then CheckAndLeaveCriticalSection
    else LeaveCriticalSection(FCritSection);
    exit;
  end;

  //��������� ����� ����� � ������ ������������������ �������
  setlength(FRegisterSocket_arr,j+1);
  FRegisterSocket_arr[j]:=SockID;

  //���� ����� � ���������� ������ �������
  i:=0;
  j:=length(FRegisterThread_arr[0].SocketID_arr);
  for k:=0 to length(FRegisterThread_arr)-1 do
  begin
    //���������� ������ ����������
    if FRegisterThread_arr[k].Operator=true then continue;

    t:=length(FRegisterThread_arr[k].SocketID_arr);
    if t<j then
    begin
      i:=k;
      j:=t;
    end;
  end;

  //��������� ������ ����� �����
  setlength(FRegisterThread_arr[i].SocketID_arr,j+1);
  FRegisterThread_arr[i].SocketID_arr[j]:=SockID;

  //������� ����� ����� � ����������� ����� ��� ���������� ������
  if CreateStatic=true then RegisterStaticSocket(SockID,FRegisterThread_arr[i].ThreadID);

  //������� � ������� �����, ��������������� ������ � ����� ����������
  setlength(buf,7);
  FillChar(buf[0],7,0);
  buf[0]:=158;
  buf[4]:=2;
  //��������� ����=158D=9Eh  - ������� ������ ������
  //����� 1-4  - ������ ������ ����� ����� ���� (� ������ ������ ������ 2 ����� - ��� ������)
  //����� 5-6  - ��� ������ (� ������ ������ 0000h - ����� ����������/�������������)
  Push(buf,SockID,now,true);
  setlength(buf,0);

  Log('Register socket completed sucsessfuly');
  if Check_CritSection then CheckAndLeaveCriticalSection
  else LeaveCriticalSection(FCritSection);

  result:=true;
end;

function TQueue.RegisterSocket(SockID:integer; ThrID:cardinal):boolean;
var i,j,k:integer;
buf:byte_arr;
begin
  result:=false;

  EnterCriticalSection(FCritSection);
  Log('Entered presice register socket');

  //����, ��� �� ��� ������ ������
  j:=length(FRegisterSocket_arr);
  for i:=0 to j-1 do
    if FRegisterSocket_arr[i]=SockID then
    begin
      CheckAndLeaveCriticalSection;
      exit;
    end;

  //��������� ����� ����� � ������ ������������������ �������
  setlength(FRegisterSocket_arr,j+1);
  FRegisterSocket_arr[j]:=SockID;

  //���� ������ �����
  i:=-1;
  for k:=0 to length(FRegisterThread_arr)-1 do
  begin
    if FRegisterThread_arr[k].ThreadID=ThrID then
    begin
      i:=k;
      j:=length(FRegisterThread_arr[k].SocketID_arr);
      break;
    end;
  end;

  //���������, ����� �� �� �����
  if i=-1 then
  begin
    CheckAndLeaveCriticalSection;
    exit;
  end;

  //��������� ������ ����� �����
  setlength(FRegisterThread_arr[i].SocketID_arr,j+1);
  FRegisterThread_arr[i].SocketID_arr[j]:=SockID;

  //������� ����� ����� � ����������� ����� ��� ���������� ������
  //RegisterStaticSocket(SockID,FRegisterThread_arr[i].ThreadID);

  //������� � ������� �����, ��������������� ������ � ����� ����������
  setlength(buf,7);
  FillChar(buf[0],7,0);
  buf[0]:=158;
  buf[4]:=2;
  //��������� ����=158D=9Eh  - ������� ������ ������
  //����� 1-4  - ������ ������ ����� ����� ���� (� ������ ������ ������ 2 ����� - ��� ������)
  //����� 5-6  - ��� ������ (� ������ ������ 0000h - ����� ����������/�������������)
  Push(buf,SockID,now,true);
  setlength(buf,0);

  Log('Register presice socket completed sucsessfuly');
  CheckAndLeaveCriticalSection;

  result:=true;
end;

function TQueue.RegisterOPSocket(SockID:integer):boolean;
var i,j,k:integer;
buf:byte_arr;
begin
  result:=false;

  EnterCriticalSection(FCritSection);
  Log('Entered presice register socket for operator');

  //����, ��� �� ��� ������ ������
  j:=length(FRegisterSocket_arr);
  for i:=0 to j-1 do
    if FRegisterSocket_arr[i]=SockID then
    begin
      CheckAndLeaveCriticalSection;
      exit;
    end;

  //��������� ����� ����� � ������ ������������������ �������
  setlength(FRegisterSocket_arr,j+1);
  FRegisterSocket_arr[j]:=SockID;

  //���� ������ ����� ��� ����������
  i:=-1;
  for k:=0 to length(FRegisterThread_arr)-1 do
  begin
    if FRegisterThread_arr[k].Operator=true then
    begin
      i:=k;
      j:=length(FRegisterThread_arr[k].SocketID_arr);
      break;
    end;
  end;

  //���������, ����� �� �� �����
  if i=-1 then
  begin
    CheckAndLeaveCriticalSection;
    exit;
  end;

  //��������� ������ ����� �����
  setlength(FRegisterThread_arr[i].SocketID_arr,j+1);
  FRegisterThread_arr[i].SocketID_arr[j]:=SockID;

  //������� � ������� �����, ��������������� ������ � ����� ����������
  setlength(buf,7);
  FillChar(buf[0],7,0);
  buf[0]:=158;
  buf[4]:=2;
  //��������� ����=158D=9Eh  - ������� ������ ������
  //����� 1-4  - ������ ������ ����� ����� ���� (� ������ ������ ������ 2 ����� - ��� ������)
  //����� 5-6  - ��� ������ (� ������ ������ 0000h - ����� ����������/�������������)
  Push(buf,SockID,now,true);
  setlength(buf,0);

  Log('Register presice socket for operator completed sucsessfuly');
  CheckAndLeaveCriticalSection;

  result:=true;
end;

function TQueue.RemoveSocket(SockID:integer):boolean;
var i,j,k:integer;
b:boolean;
begin
  result:=false;

  EnterCriticalSection(FCritSection);
  Log('Entered remove socket with SockID='+inttostr(SockID));

  //���� ����� � ������������������ �������
  b:=false;
  k:=length(FRegisterSocket_arr);
  for i:=0 to k-1 do
    if FRegisterSocket_arr[i]=SockID then
    begin
      b:=true;
      j:=i;
      break;
    end;

  if b=false then
  begin
    CheckAndLeaveCriticalSection;
    exit;
  end;
  //������� ����� �� ������������������ �������
  move(FRegisterSocket_arr[j+1],FRegisterSocket_arr[j],(k-j-1)*sizeof(integer));
  setlength(FRegisterSocket_arr,k-1);

  //���� ����� � �������� � �������      
  b:=false;
  for i:=0 to length(FRegisterThread_arr)-1 do
  begin
    if b=true then break;
    for j:=0 to length(FRegisterThread_arr[i].SocketID_arr)-1 do
      if FRegisterThread_arr[i].SocketID_arr[j]=SockID then
      begin
        move(FRegisterThread_arr[i].SocketID_arr[j+1],FRegisterThread_arr[i].SocketID_arr[j],(length(FRegisterThread_arr[i].SocketID_arr)-j-1)*sizeof(integer));
        setlength(FRegisterThread_arr[i].SocketID_arr,length(FRegisterThread_arr[i].SocketID_arr)-1);
        b:=true;
      end;
  end;

  //���� ����� � ����������� ������
  i:=0;
  while i<length(FCoresp_arr) do
  begin
    if FCoresp_arr[i].SocketID=SockID then
    begin
      move(FCoresp_arr[i+1],FCoresp_arr[i],(length(FCoresp_arr)-i-1)*sizeof(TCorespElement));
      setlength(FCoresp_arr,length(FCoresp_arr)-1);
      continue;
    end;

    inc(i);
  end;

  Log('Remove socket sucsessful');
  CheckAndLeaveCriticalSection;

  //LogThreads;

  result:=true;
end;

function TQueue.RegisterStaticSocket(SockID:integer; ThrID:cardinal):boolean;
var i:integer;
begin
  result:=false;

  EnterCriticalSection(FCritSection);
  Log('Enter registering static socket with SocketID='+inttostr(sockid)+'; ThreadID='+inttostr(ThrID));

  //��������� ������ � �����
  i:=length(FCoresp_arr);
  setlength(FCoresp_arr,i+1);
  FCoresp_arr[i].ThreadID:=ThrID;
  FCoresp_arr[i].SocketID:=SockID;
  FCoresp_arr[i].Time:=now;

  Log('Register static socket complete');
  CheckAndLeaveCriticalSection;

  result:=true;
end;

function TQueue.CleanSocketArrays:integer;
var ar:array of integer;
i,j,k:integer;
b:boolean;
p,p1,p_prev:PTQueueElement;
begin
  result:=-1;

  EnterCriticalSection(FCritSection);
  Log('Entered cleaning procedure');

  //���������� � ������ ��� ������, ������� ������������
  setlength(ar,FServer.Socket.ActiveConnections);

  for i:=0 to FServer.Socket.ActiveConnections-1 do
  //while i<FServer.Socket.ActiveConnections do
  begin
    ar[i]:=FServer.Socket.Connections[i].SocketHandle;
    {try
      FServer.Socket.Connections[i].SendText('123');
    except
      on e:exception do
      begin
        Log('Exception in test send with message:'+e.Message);
        FServer.Socket.Connections[i].Close;
        sleep(200);
        b:=true;
      end;
    end;   }
  end;

  //��������� ������ ������������������ �������
  Log('Checking registered socket array');
  i:=0;
  while i<length(FRegisterSocket_arr) do
  begin
    b:=false;
    for j:=0 to length(ar)-1 do
      if ar[j]=FRegisterSocket_arr[i] then
      begin
        b:=true;
        break;
      end;

    //���� ������� �����, �� ���� ������� ����������, ����������/ ���� �� ����� - �������
    if b=false then
    begin
      Log('Found wrong element, ID='+inttostr(FRegisterSocket_arr[i]));

      if i<>(length(FRegisterSocket_arr)-1) then
        move(FRegisterSocket_arr[i+1],FRegisterSocket_arr[i],(length(FRegisterSocket_arr)-i-1)*sizeof(integer));
      setlength(FRegisterSocket_arr,length(FRegisterSocket_arr)-1);

      result:=1;

      continue;
    end;

    inc(i);
  end;

  //��������� ������ ����������� �������� ������� �������
  Log('Checking static socket array');
  i:=0;
  while i<length(FCoresp_arr) do
  begin
    b:=false;
    for j:=0 to length(ar)-1 do
      if ar[j]=FCoresp_arr[i].SocketID then
      begin
        b:=true;
        break;
      end;

    //���� ������� �����, �� ���� ������� ����������, ����������/ ���� �� ����� - �������
    if b=false then
    begin
      Log('Found wrong element, ID='+inttostr(FCoresp_arr[i].SocketID));

      if i<>(length(FCoresp_arr)-1) then
        move(FCoresp_arr[i+1],FCoresp_arr[i],(length(FCoresp_arr)-i-1)*sizeof(TCorespElement));
      setlength(FCoresp_arr,length(FCoresp_arr)-1);

      result:=1;

      continue;
    end;

    inc(i);
  end;

  //��������� ������ �������
  Log('Checking thread array');
  for k:=0 to length(FRegisterThread_arr)-1 do
  begin
    i:=0;
    while i<length(FRegisterThread_arr[k].SocketID_arr) do
    begin
      b:=false;
      for j:=0 to length(ar)-1 do
        if ar[j]=FRegisterThread_arr[k].SocketID_arr[i] then
        begin
          b:=true;
          break;
        end;

      //���� ������� �����, �� ���� ������� ����������, ����������/ ���� �� ����� - �������
      if b=false then
      begin
        Log('Found wrong element, ID='+inttostr(FRegisterThread_arr[k].SocketID_arr[i]));

        if i<>(length(FRegisterThread_arr[k].SocketID_arr)-1) then
          move(FRegisterThread_arr[k].SocketID_arr[i+1],FRegisterThread_arr[k].SocketID_arr[i],(length(FRegisterThread_arr[k].SocketID_arr)-i-1)*sizeof(integer));
        setlength(FRegisterThread_arr[k].SocketID_arr,length(FRegisterThread_arr[k].SocketID_arr)-1);

        result:=1;

        continue;
      end;

      inc(i);
    end;
  end;

  //��������� �������
  Log('Checking Queue');
  p:=First;
  p_prev:=nil;
  while p<>nil do
  begin
    i:=p^.SocketID;
    //���� ����� �� ����� ���� � ������� ������������������ �������
    b:=false;
    for j:=0 to length(FRegisterSocket_arr)-1 do
      if FRegisterSocket_arr[j]=i then
      begin
        b:=true;
        break;
      end;

    if b=false then  //���� ������ �������� ���, ������ ��� ���� ������� �� �������
    begin
      Log('Found wrong element, ID='+inttostr(p^.SocketID)+'   Length='+inttostr(length(p^.data)));
      //������� ������� ������� �������
      setlength(p^.data,0);
      if p<>First then p_prev^.NextElement:=p^.NextElement
      else
      begin
        First:=p^.NextElement;
        p_prev:=First;
      end;
      if p^.NextElement=nil then Last:=p_prev;
      p1:=p^.NextElement;
      Dispose(p);
      Dec(FQueueCount);
      p:=p1;

      result:=1;
      continue;
    end;

    p_prev:=p;
    p:=p^.NextElement;
  end;

  if result<>1 then result:=0;

  setlength(ar,0);

  Log('Cleaning complete');
  CheckAndLeaveCriticalSection;
end;

procedure TQueue.CreateTransferTest;
var backup_log_packet:byte_arr;
str_file:string;
i,j,k:integer;
begin
  //EnterCriticalSection(FCritSection);

  Log('Test timer OnTimer');
  randomize;

  if length(FRegisterSocket_arr)>5 then
  begin
    for k:=0 to 4 do
    begin
    //��������� ����� ��������
    str_file:='1234567890_'+inttostr(gettickcount64)+'.log';   //�������� ����� �� FTP
    j:=length(str_file)+25;   //������ ������
    i:=j-5;   //������ ������ � ���������
    setlength(backup_log_packet,j);
    FillChar(backup_log_packet[0],length(backup_log_packet),0);
    backup_log_packet[0]:=158;  //��������� ������
    backup_log_packet[1]:=(i and $FF000000)shr 24;  //������
    backup_log_packet[2]:=(i and $00FF0000)shr 16;
    backup_log_packet[3]:=(i and $0000FF00)shr 8;
    backup_log_packet[4]:=(i and $000000FF);
    backup_log_packet[6]:=7;  //���
    i:=123321;
    backup_log_packet[7]:=(i and $FF000000)shr 24;  //������ �������������� �����
    backup_log_packet[8]:=(i and $00FF0000)shr 16;
    backup_log_packet[9]:=(i and $0000FF00)shr 8;
    backup_log_packet[10]:=(i and $000000FF);
    i:=123;
    backup_log_packet[11]:=(i and $FF000000)shr 24;  //������ ������������� �����
    backup_log_packet[12]:=(i and $00FF0000)shr 16;
    backup_log_packet[13]:=(i and $0000FF00)shr 8;
    backup_log_packet[14]:=(i and $000000FF);
    backup_log_packet[15]:=1;  //�� ���������� ����������
    i:=$ABCDEF01;   //������� ����������� ����� ������������� ���������
    backup_log_packet[16]:=(i and $FF000000)shr 24;  //crc32
    backup_log_packet[17]:=(i and $00FF0000)shr 16;
    backup_log_packet[18]:=(i and $0000FF00)shr 8;
    backup_log_packet[19]:=(i and $000000FF);
    i:=3000+random(k*3000);   //������� ��� ������� �� ���� ������ (2/3 ������ ��������)
    backup_log_packet[20]:=(i and $FF000000)shr 24;  //timeout
    backup_log_packet[21]:=(i and $00FF0000)shr 16;
    backup_log_packet[22]:=(i and $0000FF00)shr 8;
    backup_log_packet[23]:=(i and $000000FF);
    backup_log_packet[24]:=length(str_file);  //������ � ��������� �����
    for i:=25 to length(backup_log_packet)-1 do
    begin
      if (i-24)>length(str_file) then break;
      backup_log_packet[i]:=ord(str_file[i-24]);
    end;

    Push(backup_log_packet,FRegisterSocket_arr[k],now,true);
    end;
  end;
  
  //CheckAndLeaveCriticalSection;
end;

function TQueue.RemoveStaticSocket(SockID:integer; ThrID:cardinal):boolean;
var i,j:integer;
begin
  result:=false;

  EnterCriticalSection(FCritSection);
  Log('Entered remove static socket with SocketID='+inttostr(sockid)+'; ThreadID='+inttostr(ThrID));

  //���� ������ ������� �������
  j:=-1;
  for i:=0 to length(FCoresp_arr)-1 do
    if (FCoresp_arr[i].ThreadID=ThrID)and(FCoresp_arr[i].SocketID=SockID) then
    begin
      j:=i;
      break;
    end;

  if j=-1 then
  begin
    CheckAndLeaveCriticalSection;
    exit;
  end;

  //������� ������ �������
  i:=length(FCoresp_arr);
  move(FCoresp_arr[j+1],FCoresp_arr[j],(i-j-1)*sizeof(TCorespElement));
  setlength(FCoresp_arr,i-1);

  Log('Remove static socket complete');
  CheckAndLeaveCriticalSection;

  result:=true;
end;

function TQueue.DisconnectSocket(SockID:integer):boolean;
var i:integer;
begin
  Log('Entered DisconnectSocket with SockID='+inttostr(SockID));
  result:=false;

  //���� ����� � ����������
  for i:=0 to FServer.Socket.ActiveConnections-1 do
    if FServer.Socket.Connections[i].SocketHandle=SockID then
    begin
      FServer.Socket.Connections[i].Close;
      result:=true;
      break;
    end;

  Log('Exiting DisconnectSocket with result='+booltostr(result,true));
end;

procedure TQueue.CreateDisconnectElement(SockID:integer);
var buf:byte_arr;
b:boolean;
i,j:integer;
begin
  //���������, ���� �� ����� ����� � �������� �������� ��� �������
  b:=false;
  for i:=0 to length(FRegisterThread_arr)-1 do
  begin
    if b=true then break;

    for j:=0 to length(FRegisterThread_arr[i].SocketID_arr)-1 do
      if FRegisterThread_arr[i].SocketID_arr[j]=SockID then
      begin
        b:=true;
        break;
      end;
  end;

  //���� �����, ��� ������� ����� ������������ ���� �����, �� ������ ������� ��������������� ������������
  if b=true then
  begin
    setlength(buf,7);
    FillChar(buf[0],7,0);
    buf[0]:=158;
    buf[4]:=2;
    buf[5]:=$FF;
    buf[6]:=$FF;

    Push(buf,SockID,now,true);
    setlength(buf,0);
  end
  else
    Log('Disconnected socket, that is not registered');
end;

function TQueue.GetSocketIP(SockID:integer):string;
var i:integer;
begin
  result:='';

  for i:=0 to FServer.Socket.ActiveConnections-1 do
    if FServer.Socket.Connections[i].SocketHandle=SockID then
    begin
      result:=FServer.Socket.Connections[i].RemoteAddress;
      break;
    end;
end;

function TQueue.SendPacket(SockID:integer; buf:byte_arr):boolean;
var i:integer;
b:boolean;
begin
  result:=false;
  Log('Enter SendPacket for socket#'+inttostr(SockID)+'   BufLen='+inttostr(length(buf)));

  {if FSemaphore.Active=true then
  begin
    if FSemaphore.SockID=SockID then
    begin
      Log('Semaphore is active in SendPacket SockID='+inttostr(FSemaphore.SockID)+', disabling semaphore');
      FSemaphore.Active:=false;
      FSemaphore.SockID:=0;
    end;
  end;  }

  b:=false;
  for i:=0 to FServer.Socket.ActiveConnections-1 do
    if FServer.Socket.Connections[i].SocketHandle=SockID then
    begin
      FServer.Socket.Connections[i].SendBuf(buf[0],length(buf));
      b:=true;
      break;
    end;

  Log('Exiting SendPacket with result='+booltostr(b,true));
  result:=b;
end;

procedure TQueue.CreateTransferElement(Ins:TTransferServerInstructions; ptr:PTThreadSocketElement);
var p,p1:PTTransferElement;
begin
  EnterCriticalSection(FCritSection);
  Log('Enter CreateTransferElement with SockID='+inttostr(ptr^.SocketID)+', ElementID='+inttostr(FID_counter));

  try
    //������� ������� � ��������� ��� ����
    New(p);
    p^.id:=FID_counter;
    inc(FID_counter);
    p^.NextElement:=nil;
    p^.TransferServer:=nil;
    //todo: ����� ���������������� ��� ���������� ������
    p^.TransferServer:=TTransferServer.Create(Ins,ExtractFilePath(FOut_path)+'TransferLogs\',ptr^.SocketID,p^.id);
    //ptr^.FileServer:=p^.id;

    //���� ����� �������
    p1:=FirstTrans;
    if p1<>nil then
      while p1^.NextElement<>nil do p1:=p1^.NextElement;

    if p1=nil then
    begin  //���� � ������� ������ ���
      FirstTrans:=p;
      inc(FTransCount);
    end
    else
    begin  //���� �� ����� ����� �������
      p1^.NextElement:=p;
      inc(FTransCount);
    end;
  except
    on e:exception do
    begin
      Log('WARNING! Exception in CreateTransferElement method with message:'+e.Message);
      CheckAndLeaveCriticalSection;
      exit;
    end;
  end;

  Log('Exit CreateTransferElement');
  CheckAndLeaveCriticalSection;
end;

procedure TQueue.AddTransferElement(el:TTransferServer);
var p,p1:PTTransferElement;
begin
  EnterCriticalSection(FCritSection);
  Log('Enter AddTransferElement');

  if el=nil then
  begin
    Log('Exit AddTransferElement with element=nil');
    CheckAndLeaveCriticalSection;
    exit;
  end;

  try
    New(p);
    p^.id:=FID_counter;
    inc(FID_counter);
    p^.NextElement:=nil;
    p^.TransferServer:=nil;
    p^.TransferServer:=el;

    //���� ����� �������
    p1:=FirstTrans;
    if p1<>nil then
      while p1^.NextElement<>nil do p1:=p1^.NextElement;

    if p1=nil then
    begin  //���� � ������� ������ ���
      FirstTrans:=p;
      inc(FTransCount);
    end
    else
    begin  //���� �� ����� ����� �������
      p1^.NextElement:=p;
      inc(FTransCount);
    end;
  except
    on e:exception do
    begin
      Log('WARNING! Exception in AddTransferElement method with message:'+e.Message);
      CheckAndLeaveCriticalSection;
      exit;
    end;
  end;

  Log('Exit AddTransferElement');
  CheckAndLeaveCriticalSection;
end;

function TQueue.GetTransferServer(ID:cardinal):TTransferServer;
var p:PTTransferElement;
begin
  EnterCriticalSection(FCritSection);

  Log('GetTransferServer enter with ID='+inttostr(ID));
  result:=nil;
  //���� ����� 0, ������ ����� �������

  if ID<>0 then
  begin
    //���� ������ ������� �� ID
    p:=FirstTrans;
    while p<>nil do
    begin
      if p^.id=ID then
      begin
        result:=p^.TransferServer;
        break;
      end;
      p:=p^.NextElement;
    end;
  end;

  CheckAndLeaveCriticalSection;
end;

function TQueue.GetTransferServerCount:integer;
begin
  result:=FTransCount;
end;

procedure TQueue.CheckTransferServers;
var p,p1,prev:PTTransferElement;
b:boolean;
begin
  EnterCriticalSection(FCritSection);
  //Log('Check');

  p:=FirstTrans;
  prev:=nil;
  while p<>nil do
  begin
    b:=false;
    if p^.TransferServer<>nil then
    begin
      if p^.TransferServer.GetStatus=tssDoneConfirmed then
      begin
        Log('Found transfer server ready to destroy, ID='+inttostr(p^.id)+', reason=destroy was confirmed, destroing');
        b:=true;
      end;
      if IncSecond(p^.TransferServer.GetLastUpdateTime,60)<now then
      begin
        Log('Found transfer server ready to destroy, ID='+inttostr(p^.id)+', reason=timeout on last action, destroing');
        b:=true;
      end;
    end   //if p^.TransferServer<>nil then
    else
    begin
      Log('Found transfer server ready to destroy, ID='+inttostr(p^.id)+', reason=transfer server is nil');
      b:=true;
    end;

    if b=true then
    begin
      if p^.TransferServer<>nil then
      begin
        p^.TransferServer.Terminate;
        p^.TransferServer.WaitFor;
        p^.TransferServer.Free;
        p^.TransferServer:=nil;
        Log('Transfer server destroy complete');
      end
      else Log('Transfer server is not created');

      //������� �������
      if p=FirstTrans then
      begin  //���� ��� ������ �������, �� �� ���� ����������� � ������� �� ��������� ��������
        p1:=p^.NextElement;
        FirstTrans:=p1;
        prev:=nil;
        Dispose(p);
        Dec(FTransCount);
        p:=p1;
        continue;
      end
      else
      begin  //���� ��� �� ������ �������, �� ���� ��������� �������� ������ ����������� ��������
        p1:=p^.NextElement;
        prev^.NextElement:=p1;
        Dispose(p);
        Dec(FTransCount);
        p:=p1;
        continue;
      end;
    end;

    prev:=p;
    p:=p^.NextElement;
  end;

  CheckAndLeaveCriticalSection;
end;

procedure TQueue.SocketListen(Sender: TObject; Socket: TCustomWinSocket);
begin
  Log('OnListen');
end;

procedure TQueue.SocketConnect(Sender: TObject; Socket: TCustomWinSocket);
var
t:int64;
b:boolean;
p:PTQueueElement;
begin
  if length(FRegisterThread_arr)=0 then
  begin
    log('No thread registered to work with socket, disconnecting');
    Socket.Close;
  end
  else
  begin
    Log('OnConnect    ID='+inttostr(socket.SocketHandle)+'    Adress='+socket.RemoteAddress+':'+inttostr(socket.RemotePort));

    {if FSemaphore.Active=true then
    begin
      if FSemaphore.SockID<>Socket.SocketHandle then
      begin
        Log('Semaphore is active and socket is different in OnConnect, disconecting');
        socket.Close;
        exit;
      end;
    end;

    FSemaphore.Active:=true;
    FSemaphore.SockID:=socket.SocketHandle;
    Log('Semaphore set to socket='+inttostr(FSemaphore.SockID));  }

    //������� �������� ������ � ����������� ������
    t:=gettickcount64;

    while TryEnterCriticalSection(FCritSection)=false do
    begin
      //Log('WARNING! Collision in Connect');
      sleep(1);
    end;

    t:=gettickcount64-t;
    b:=true;

    if b=true then
    begin
      if t>100 then  Log('WARNING! Time spend on aquiring CriticalSection in Connect='+inttostr(t));

      if RegisterSocket(Socket.SocketHandle,true,true)=false then Socket.Close;

      //LogThreads;

      LeaveCriticalSection(FCritSection);
    end
    else
    begin
      Log('Unable to enter critical section in OnSocketConnect, creating backup');

      //������� ������� � ��������� ��� ����
      New(p);
      p^.SocketID:=socket.SocketHandle;
      p^.NextElement:=nil;
      setlength(p^.data,0);

      //���� ��� ������ ������� � �������
      if FirstExt=nil then
      begin
        FirstExt:=p;
        LastExt:=p;
      end
      else  //���� � ������� ��� ���-�� ����
      begin
        LastExt^.NextElement:=p;
        LastExt:=p;
      end;
    end;

  end;

  Log('End OnConnect with ID='+inttostr(socket.SocketHandle));
end;

procedure TQueue.SocketDisconnect(Sender: TObject; Socket: integer);
var
t:int64;
b:boolean;
p:PTQueueElement;
begin
  Log('OnDisconnect    ID='+inttostr(socket));
  //RemoveSocket(Socket.SocketHandle);

  {if FSemaphore.Active=true then
  begin
    Log('Semaphore is active in OnDisconnect, semaphore SockID='+inttostr(FSemaphore.SockID));

    if socket=FSemaphore.SockID then
    begin
      FSemaphore.Active:=false;
      FSemaphore.SockID:=0;
      Log('Semaphore is disabled');
    end;
  end;  }

  //������� �������� ������ � ����������� ������
  t:=gettickcount64;

  while TryEnterCriticalSection(FCritSection)=false do
  begin
    //Log('WARNING! Collision in Disconnect');
    sleep(1);
  end;

  t:=gettickcount64-t;
  b:=true;

  if b=true then
  begin
    if t>100 then  Log('WARNING! Time spend on aquiring CriticalSection in Disconnect='+inttostr(t));

    CreateDisconnectElement(Socket);

    LeaveCriticalSection(FCritSection);
  end
  else
  begin
    Log('Unable to enter critical section in OnSocketDisconnect, creating backup');

    //������� ������� � ��������� ��� ����
    New(p);
    p^.SocketID:=socket;
    p^.ReceiveTime:=now;
    p^.NextElement:=nil;
    setlength(p^.data,7);
    FillChar(p^.data[0],7,0);
    p^.data[0]:=158;
    p^.data[4]:=2;
    p^.data[5]:=$FF;
    p^.data[6]:=$FF;

    //���� ��� ������ ������� � �������
    if FirstExt=nil then
    begin
      FirstExt:=p;
      LastExt:=p;
    end
    else  //���� � ������� ��� ���-�� ����
    begin
      LastExt^.NextElement:=p;
      LastExt:=p;
    end;
  end;

  Log('End OnDisconnect with ID='+inttostr(socket));
end;

procedure TQueue.SocketRead(Sender: TObject; Socket: TCustomWinSocket);
var buf:byte_arr;
i,j:integer;
t:int64;
b:boolean;
p:PTQueueElement;
FReadBuffer:array[0..8191] of byte;  //����� ��� ����� ������� ����� ������ (8 ��)
begin
  Log('OnSocketRead    ID='+inttostr(socket.SocketHandle)+', bytes='+inttostr(socket.ReceiveLength));
  //buf:=socket.ReceiveText;

  {i:=socket.ReceiveLength;
  setlength(buf,i);
  socket.ReceiveBuf(buf[0],i); }
  //��������� ������
  try
    i:=socket.ReceiveBuf(FReadBuffer[0],8192);
  except
    on e:exception do
    begin
      Log('WARNING! Exception on receive buffer with message:'+e.Message);
      exit;
    end;
  end;

  if i<=-1 then
  begin
    Log('WARNING! Wrong receive length returned, value='+inttostr(i));
    exit;
  end;
  setlength(buf,i);
  move(FReadBuffer[0],buf[0],i);

  j:=socket.SocketHandle;
  
  {for i:=0 to length(buf)-1 do
    Log(' #'+inttostr(i)+'='+inttostr(buf[i]));     }

  //������� �������� ������ � ����������� ������
  t:=gettickcount64;

  while TryEnterCriticalSection(FCritSection)=false do
  begin
    //Log('WARNING! Collision in Read');
    sleep(1);
  end;

  t:=gettickcount64-t;
  b:=true;

  if b=true then
  begin
    if t>100 then  Log('WARNING! Time spend on aquiring CriticalSection in Read='+inttostr(t));

    Push(buf,j,now,true);

    LeaveCriticalSection(FCritSection);
  end
  else
  begin
    Log('Unable to enter critical section in OnSocketRead, creating backup');

    //������� ������� � ��������� ��� ����
    New(p);
    p^.SocketID:=j;
    p^.ReceiveTime:=now;
    p^.NextElement:=nil;
    setlength(p^.data,length(buf));
    move(buf[0],p^.data[0],length(buf));

    //���� ��� ������ ������� � �������
    if FirstExt=nil then
    begin
      FirstExt:=p;
      LastExt:=p;
    end
    else  //���� � ������� ��� ���-�� ����
    begin
      LastExt^.NextElement:=p;
      LastExt:=p;
    end;
  end;

  setlength(buf,0);
end;

procedure TQueue.SocketError(Sender: TObject; Socket: TCustomWinSocket; ErrorEvent: TErrorEvent; var ErrorCode: Integer);
begin
  Log('OnClientError,   ErrorCode='+inttostr(ErrorCode)+'    ErrorEvent='+inttostr(ord(ErrorEvent)));

  case ErrorEvent of
    eeDisconnect:begin
                   //RemoveSocket(Socket.SocketHandle);
                   Socket.Close;
                 end;
  end;

  ErrorCode:=0;
end;

procedure TQueue.SocketAccept(Sender: TObject; Socket: TCustomWinSocket);
begin
  Log('OnAccept    ID='+inttostr(socket.SocketHandle)+'    Adress='+socket.RemoteAddress+':'+inttostr(socket.RemotePort));

  //if FSemaphore.Active then Socket.ASyncStyles:=[];
end;

procedure TQueue.SocketGetThread(Sender: TObject; ClientSocket: TServerClientWinSocket; var SocketThread: TServerClientThread);
begin
  if FServerShutdown=true then
  begin
    SocketThread:=nil;

    clientsocket.Close;
  end
  else
  begin
    Log('OnGetThread    ID='+inttostr(ClientSocket.SocketHandle));

    SocketThread:=TClientThread.Create(ClientSocket,tpLower,10,FOut,1,FOut_path,Self);
  end;
end;

procedure TQueue.Execute;
var t:int64;
begin
  Log('Queue Execute enter');

  TransferUpdateTime:=gettickcount64;

  while not(Terminated) do
  begin
    //Log('Queue execute work');
    t:=gettickcount64;

    if TransferUpdateTime+5000<t then
    begin
      CheckTransferServers;
      TransferUpdateTime:=t;
    end;

    //tofo: ������� ������������� �������� ������������ �������� ���-�� ��������� �������� (������� ������� � �������� ��������)

    sleep(500);
  end;

  Log('Queue Execute exit');
end;

procedure TQueue.Log(message:string);
begin
  //����� � ���� �����
  LogServerMess(FOut_path,message,true);
  //����� � ����
  if FOut<>nil then FOut.Add(FormatDateTime('hh:nn:ss.zzz',now)+'  '+message);
end;

function TQueue.LogServerMess(path,Mess:string; time:boolean):boolean;
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
//======================TQueue===================


end.
