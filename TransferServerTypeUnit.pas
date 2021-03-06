unit TransferServerTypeUnit;

interface

uses Classes, ScktComp;

type
  TTransferServerType = (tstIncoming, tstOutgoing, tstTunnel);
  TTransferServerAction = (tsaReceiveUploadToFTP, tsaReceiveSaveToDisk, tsaUploadFileFromDisk, tsaTunnelOneWay, tsaTunnelBothWays);
  TTransferServerStatus = (tssWorking, tssError, tssTimeout, tssDone, tssDoneConfirmed);

  TTransferServerFTPSettings = record
    Host:string;
    Port:integer;
    User:string;
    Pass:string;
    Timeout:integer;
  end;

  TTransferServerInstructions = record
    TransferType:TTransferServerType;
    TransferAction:TTransferServerAction;
    ReadWriteTimeout:integer;
    ServerPort:integer;
    TransferCount:integer;  //���-�� ������ ��� �������� (�������������) (-1=�������������� ���-��)
    ZTransferCount:integer;   //���-�� ������ ��� �������� (������������)
    UseZLib:boolean;   //��������� ������������� ����������
    TransferCRC32:integer;    //����������� ����� CRC32 ������������� ������ ��� �������� ������������ ��������
    FTPSettings:TTransferServerFTPSettings;
    FirstIP:string;
    SecondIP:string;
    CommandsStr:string;
    TempStr:string;
  end;

  TTransferServer = class(TThread)
  private
    FOut_path:string;
    FStatus:TTransferServerStatus;
    FLastUpdateTime:TDateTime;  //��������� ����� ���������� (��� �������� ��������)
    FLastTime:int64;  //��������� ����� ���������� (��� ����������� ��������)
    FInstructions:TTransferServerInstructions;

    FCounter:integer;  //�������� ������� ���������� ����

    FServer:TServerSocket;  //��������� �������

    FRecv_buffer:array[0..8191] of byte;  //����� ��� ����� ������
    FBuffer1:array of byte;  //����� ��� ������� �������
    FBuffer2:array of byte;  //������ ��� ������� �������
    FSocketID1:integer;  //ID ������ ��� ������� �������
    FSocketID2:integer;  //ID ������ ��� ������� �������

    function CheckAndUnzip:boolean;  //������� �������� � ���������� ����������� ������
    function DumpReceivedBuffer:boolean;  //������� ��� ����������� ������ �� ���� ��� ���

    procedure LogInstructions;  //��������� ��� ����������� ���� �����������
    function LoadFileFromDisk(path:string):boolean;  //������� ��� ������ �����  ������ ��� � �����

    //���������-������� ��� �������
    procedure ClientListen(Sender: TObject; Socket: TCustomWinSocket);
    procedure ClientConnect(Sender: TObject; Socket: TCustomWinSocket);
    procedure ClientDisconnect(Sender: TObject; Socket: TCustomWinSocket);
    procedure ClientRead(Sender: TObject; Socket: TCustomWinSocket);
    procedure ClientError(Sender: TObject; Socket: TCustomWinSocket; ErrorEvent: TErrorEvent; var ErrorCode: Integer);
    procedure ClientWrite(Sender: TObject; Socket: TCustomWinSocket);
  public
    constructor Create(Inst:TTransferServerInstructions; output_path:string; SockID,ServerID:integer);
    destructor Destroy; override;

    procedure Execute; override;

    function GetStatus:TTransferServerStatus;

    procedure SetLastUpdateTime;
    function GetLastUpdateTime:TDateTime;

    procedure ConfirmDestroy;

    procedure Log(mess:string);
  end;

implementation

uses SysUtils, Windows, typesUnit, ZlibEx, IdFTP;

//----------------------TTransferServer-------------------
constructor TTransferServer.Create(Inst:TTransferServerInstructions; output_path:string; SockID,ServerID:integer);
begin
  inherited Create(true);

  //initialization
  FInstructions:=Inst;
  FStatus:=tssError;
  FOut_path:=output_path+'transfer_'+inttostr(gettickcount64)+'_S'+inttostr(SockID)+'_E'+inttostr(ServerID)+' ';
  case FInstructions.TransferAction of
    tsaReceiveUploadToFTP:FOut_path:=FOut_path+'FTP '+FInstructions.CommandsStr;
    tsaReceiveSaveToDisk:FOut_path:=FOut_path+'Disk '+ExtractFileName(FInstructions.CommandsStr);
    tsaUploadFileFromDisk:FOut_path:=FOut_path+'Upload '+ExtractFileName(FInstructions.CommandsStr)+'  '+FInstructions.TempStr;
    tsaTunnelOneWay:FOut_path:=FOut_path+'Tunnel1 '+FInstructions.FirstIP+'-'+FInstructions.SecondIP+'.log';
    tsaTunnelBothWays:FOut_path:=FOut_path+'Tunnel2 '+FInstructions.FirstIP+'-'+FInstructions.SecondIP+'.log';
  end;
  if ExtractFileExt(FOut_path)<>'.log' then FOut_path:=FOut_path+'.log';

  //������������� ����������
  if FInstructions.UseZLib=true then
    FCounter:=FInstructions.ZTransferCount
  else
    FCounter:=FInstructions.TransferCount;
  FLastUpdateTime:=now;
  setlength(FBuffer1,0);
  setlength(FBuffer2,0);
  FSocketID1:=-1;
  FSocketID2:=-1;
  FLastTime:=gettickcount64;

  Log('Creating ServerTransfer component begins');

  //�������� ����������
  LogInstructions;

  try
    FServer:=TServerSocket.Create(nil);
    FServer.Port:=FInstructions.ServerPort;
    FServer.ServerType:=stNonBlocking;
    FServer.OnListen:=ClientListen;
    FServer.OnClientConnect:=ClientConnect;
    FServer.OnClientDisconnect:=ClientDisconnect;
    FServer.OnClientRead:=ClientRead;
    FServer.OnClientError:=ClientError;
    FServer.OnClientWrite:=ClientWrite;
  except
    on e:exception do
    begin
      Log('WARNING! Exception on server create with message:'+e.message);
    end;
  end;

  Log('Creating ServerTransfer complete');

  Resume;
end;

destructor TTransferServer.Destroy;
begin
  Log('Destroy begin');

  try
    FServer.Free;
  except
    on e:exception do Log('WARNING! Exception on server free with message:'+e.Message);
  end;

  Log('Server closed end freed');

  setlength(FBuffer1,0);
  setlength(FBuffer2,0);

  Log('Buffers freed');

  Log('Destroy complete');
end;

procedure TTransferServer.Execute;
var msg:TMSG;
begin
  Log('Server Execute enter');

  FStatus:=tssWorking;
  FLastTime:=gettickcount64;

  //��������� ������
  Log('Opening server');
  FServer.Open;
  Log('Server opened');

  //���� �� ������� ���� �� ������, �� ���������� ��� ��� � �����
  if FInstructions.TransferAction=tsaUploadFileFromDisk then
  begin
    if LoadFileFromDisk(FInstructions.CommandsStr)=false then
    begin
      //��� �� �� ������ ��������� ����, ���������� ������ � ��� ����������
      Log('WARNING! Failed to read a file, setting error status and waiting for termination');
      FStatus:=tssError;
      //��������� ������
      FServer.Close;
      while not(Terminated) do
        sleep(100);
    end;
  end;

  try
    while not(Terminated) do
    begin
      if (FLastTime+FInstructions.ReadWriteTimeout)<gettickcount64 then
      begin
        Log('Timeout when receiving/sending data, waiting for terminate');
        if FStatus=tssWorking then FStatus:=tssTimeout;
        break;
      end;

      case FInstructions.TransferAction of
        tsaReceiveUploadToFTP, tsaReceiveSaveToDisk:
        begin
          if FCounter<=0 then
          begin
            if FStatus=tssWorking then
            begin
              Log('Transfer complete, counter='+inttostr(FCounter));
              Log('  Receive buffer size='+inttostr(length(FBuffer1)));

              //��������� � ������������� �����
              if CheckAndUnzip=false then
              begin
                Log('Error checking and unzipping buffer, setting error status and waiting for termination');
                FStatus:=tssError;
                break;
              end;

              //���������� ���������� ����� � ������ �����
              if DumpReceivedBuffer=false then
              begin
                Log('Error dumping buffer, setting error status and waiting for termination');
                FStatus:=tssError;
                break;
              end;

              //��������� ������
              if FServer.Socket.Connected then FServer.Close;

              FStatus:=tssDone;
              break;
            end
            else
            begin
              Log('Entered transfer completion portion in execute with wrong status, exiting');
              break;
            end;
          end;  //if FCounter<=0 then
        end;  //tsaReceiveUploadToFTP, tsaReceiveSaveToDisk:
        tsaUploadFileFromDisk:
        begin
          if FCounter<=0 then
          begin
            if FStatus=tssWorking then
            begin
              Log('Transfer complete, counter='+inttostr(FCounter));
              Log('  Sended buffer size='+inttostr(length(FBuffer1)));

              //��������� ������
              if FServer.Socket.Connected then FServer.Close;

              FStatus:=tssDone;
              break;
            end
            else
            begin
              Log('Entered transfer completion portion in execute with wrong status, exiting');
              break;
            end;
          end;  //if FCounter<=0 then
        end;  //tsaUploadFileFromDisk:
        tsaTunnelOneWay:
        begin
        end;
        tsaTunnelBothWays:
        begin
        end;
      end;  //case FInstructions.TransferAction of

      if PeekMessage(Msg, 0, 0, 0, PM_REMOVE) then
      begin
        TranslateMessage(Msg);
        DispatchMessage(Msg);
      end;

      sleep(100);
    end;  //while not(Terminated) do
  except
    on e:exception do
    begin
      Log('WARNING! Exception in first cicle of execute with message:'+e.Message);
      Log('Setting error status');
      FStatus:=tssError;
    end;
  end;

  Log('After main cycle, counter='+inttostr(FCounter));
  FServer.Close;

  while not(Terminated) do
    sleep(100);

  Log('After secondary cycle');
end;

function TTransferServer.CheckAndUnzip:boolean;
var i,j:integer;
mem:TMemoryStream;
compr:TZDecompressionStream;
buf:array of byte;
begin
  result:=false;
  Log('CheckAndUnzip function enter');

  //������� CRC32 �� ����������� ������
  i:=ZCRC32(0,FBuffer1[0],length(FBuffer1));

  //�������� CRC32
  if i<>FInstructions.TransferCRC32 then
  begin
    Log('CRC not matches, exiting CheckAndUnzip function');
    exit;
  end
  else
    Log('CRC matches, buffer CRC32='+inttohex(i,8)+'   , instructions CRC32='+inttohex(FInstructions.TransferCRC32,8));

  if FInstructions.UseZLib=false then
  begin  //���� �� ���������� ����������, �� ������ �� ������
    Log('Not using packing, exiting');
  end
  else
  begin  //���� ���������� ����������, �� ���� �����������
    mem:=nil;
    compr:=nil;

    //�������������
    try
      mem:=TMemoryStream.Create;
      mem.WriteBuffer(FBuffer1[0],length(FBuffer1));

      compr:=TZDecompressionStream.Create(mem);
      compr.Position:=0;
      setlength(buf,compr.size);
      compr.ReadBuffer(buf[0],length(buf));

      compr.Free;
      compr:=nil;
      mem.Free;
      mem:=nil;
    except
      on e:exception do
      begin
        Log('Exception on decompression with message:'+e.Message);
        if mem<>nil then mem.Free;
        if compr<>nil then compr.Free;
        exit;
      end;
    end;

    //������� CRC �� �������������� ������
    j:=ZCRC32(0,buf[0],length(buf));
    Log('Unpacking is done, size='+inttostr(length(buf))+', CRC32='+inttohex(j,8));

    //���������� ������
    if (length(buf)<>FInstructions.TransferCount)or(length(FBuffer1)<>FInstructions.ZTransferCount) then
    begin
      Log('Size not matches, exiting CheckAndUnzip function');
      setlength(buf,0);
      exit;
    end;

    //���������� �����
    setlength(FBuffer1,length(buf));
    move(buf[0],FBuffer1[0],length(buf));
    setlength(buf,0);

    Log('Moving buffers complete');
  end;


  Log('CheckAndUnzip function exit');
  result:=true;
end;

function TTransferServer.DumpReceivedBuffer:boolean;
var FTP:TIdFTP;
mem:TMemoryStream;
f:integer;
begin
  result:=false;
  Log('DumpReceivedBuffer function enter');

  //��������� �� �������� � ����������
  if FInstructions.TransferAction=tsaReceiveUploadToFTP then
  begin  //���������� �� ���
    //�������� ���
    Log('Creating FTP client');
    try
      FTP:=TIdFTP.Create(nil);
      FTP.Host:=FInstructions.FTPSettings.Host;
      FTP.Passive:=true;
      FTP.Password:=FInstructions.FTPSettings.Pass;
      FTP.Port:=FInstructions.FTPSettings.Port;
      FTP.ReadTimeout:=FInstructions.FTPSettings.Timeout;
      FTP.Username:=FInstructions.FTPSettings.User;
    except
      on e:exception do
      begin
        Log('Exception on creating FTP client with message:'+e.Message);
        exit;
      end;
    end;

    //������� � ��������
    Log('Connecting to FTP '+FTP.Host+':'+inttostr(FTP.Port)+', Timeout='+inttostr(FTP.ReadTimeout));
    mem:=nil;
    try
      FTP.Connect(true,FInstructions.FTPSettings.Timeout);

      //������������ ����� � �����
      mem:=TMemoryStream.Create;
      mem.Write(FBuffer1[0],length(FBuffer1));
      mem.Position:=0;

      //��������� �� FTP
      FTP.Put(mem,FInstructions.CommandsStr,false);
      FTP.Disconnect;
      FTP.DisconnectSocket;

      mem.Free;
      mem:=nil;
      FTP.Free;
      FTP:=nil;
    except
      on e:exception do
      begin
        Log('Exception on connect and upload with message:'+e.Message);
        if mem<>nil then mem.Free;
        if FTP<>nil then
        begin
          FTP.Disconnect;
          FTP.DisconnectSocket;
          FTP.Free;
        end;
        exit;
      end;
    end;

    Log('FTP upload completed sucsessfuly');
  end
  else if FInstructions.TransferAction=tsaReceiveSaveToDisk then
  begin  //���������� �� ����
    //���������, ���������� �� ����
    if FileExists(FInstructions.CommandsStr) then
    begin
      //���� ���� ����������, �� ������ ������
      Log('File already exists, exiting');
      exit;
    end;

    //������ ���� � ���������� ���� �����
    Log('Creating file and dumping buffer');
    f:=FileCreate(FInstructions.CommandsStr);
    if f<0 then
    begin
      Log('File create failed, exiting');
      exit;
    end;
    if FileWrite(f,FBuffer1[0],length(FBuffer1))=-1 then
    begin
      Log('File write failed, exiting');
      exit;
    end;
    FileClose(f);

    Log('Buffer sucsessfuly written into a file');
  end
  else
  begin  //���-�� ������ (�� ������ ����)
    Log('Server transfer actions is wrong, exiting');
    exit;
  end;

  Log('DumpReceivedBuffer function exit');
  result:=true;
end;

procedure TTransferServer.LogInstructions;
var str:string;
begin
  //���������� ��������� ������ ������ ����������� ����������
  Log('Begin to log instructions');
  //���������� ��� �������
  str:='ServerType:';
  case FInstructions.TransferType of
    tstIncoming:str:=str+'Incoming';
    tstOutgoing:str:=str+'Outgoing';
    tstTunnel:str:=str+'Tunnel';
    else str:=str+inttostr(ord(FInstructions.TransferType));
  end;
  Log(str);
  //���������� ��������
  str:='ServerAction:';
  case FInstructions.TransferAction of
    tsaReceiveUploadToFTP:str:=str+'Receive and upload to FTP';
    tsaReceiveSaveToDisk:str:=str+'Receive and save to disk';
    tsaUploadFileFromDisk:str:=str+'Upload file from disk';
    tsaTunnelOneWay:str:=str+'Create one way tunnel';
    tsaTunnelBothWays:str:=str+'Create both ways tunnel';
    else str:=str+inttostr(ord(FInstructions.TransferAction));
  end;
  //str:='ServerAction:'+GetEnumName(TypeInfo(TTransferServerAction),ord(FInstructions.TransferAction));
  Log(str);
  //���������� IP
  Log('First IP='+FInstructions.FirstIP);
  Log('Second IP='+FInstructions.SecondIP);
  //���������� ��������
  Log('ReadWriteTimeout:'+inttostr(FInstructions.ReadWriteTimeout));
  //���������� ���� �������
  Log('ServerPort:'+inttostr(FInstructions.ServerPort));
  //���������� ��������� FTP
  Log('FTP settings:');
  Log('   FTPHost:'+FInstructions.FTPSettings.Host);
  Log('   FTPPort:'+inttostr(FInstructions.FTPSettings.Port));
  Log('   FTPUser:'+FInstructions.FTPSettings.User);
  Log('   FTPPassword:'+FInstructions.FTPSettings.Pass);
  Log('   FTPTimeout:'+inttostr(FInstructions.FTPSettings.Timeout));
  //��������� ������
  Log('CommandString:'+FInstructions.CommandsStr);
  //�������������� ������
  Log('TempString:'+FInstructions.TempStr);
  //���-�� ������������� ������������ ����
  Log('Unpacket transfer size:'+inttostr(FInstructions.TransferCount));
  //���-�� ������������ ������������ ����
  Log('Packet transfer size:'+inttostr(FInstructions.ZTransferCount));
  //��������� ������������� ����������
  Log('Use ZLib:'+booltostr(FInstructions.UseZLib,true));
  //����������� ����� ������������ ������
  Log('CRC32:'+inttohex(FInstructions.TransferCRC32,8));
  //����� ������ ����������
  Log('Log instructions end ---------------------------');
end;

function TTransferServer.LoadFileFromDisk(path:string):boolean;
var hndl:integer;
i:integer;
begin
  result:=false;

  Log('Load file from disk enter with path='+path);

  hndl:=FileOpen(path,fmOpenRead or fmShareDenyNone);
  if hndl>0 then
  begin
    i:=FileSeek(hndl,0,2);
    if i<>-1 then
    begin
      FileSeek(hndl,0,0);
      setlength(FBuffer1,i);
      FileRead(hndl,FBuffer1[0],length(FBuffer1));
    end
    else
    begin
      Log('File seek failed');
      exit;
    end;

    FileClose(hndl);
  end
  else
  begin
    Log('File open failed');
    exit;
  end;

  Log('Load file from disk exit with readed filesize='+inttostr(length(FBuffer1)));

  result:=true;
end;

procedure TTransferServer.ClientListen(Sender: TObject; Socket: TCustomWinSocket);
begin
  Log('Server OnListen event');
end;

procedure TTransferServer.ClientConnect(Sender: TObject; Socket: TCustomWinSocket);
var i:integer;
str:string;
begin
  i:=socket.SocketHandle;
  str:=socket.RemoteAddress;

  Log('Server OnConnect event, IP='+str+', SocketID='+inttostr(i));

  //���������, ��������� �� IP
  if str=FInstructions.FirstIP then
  begin
    Log('IP matches first client');
    if FSocketID1<>-1 then  //���������, ����������� �� ������ ������
    begin
      Log('First client already connected with SocketID='+inttostr(FSocketID1)+', disconnecting new connection');
      socket.Close;
      exit;
    end;

    FSocketID1:=i;
    FLastTime:=gettickcount64;
    Log('Saving socket as first client');
    exit;
  end;

  //��������� �� ���������� ������ IP
  if str=FInstructions.SecondIP then
  begin
    Log('IP matches second client');
    if FSocketID2<>-1 then  //���������, ����������� �� ������ ������
    begin
      Log('Second client already connected with SocketID='+inttostr(FSocketID2)+', disconnecting new connection');
      socket.Close;
      exit;
    end;

    FSocketID2:=i;
    FLastTime:=gettickcount64;
    Log('Saving socket as second client');
    exit;
  end;

  //���� �� ������� ������ ���� ��� ����������� ����� IP
  Log('Connected wrong IP, disconnecting');
  socket.Close;
end;

procedure TTransferServer.ClientDisconnect(Sender: TObject; Socket: TCustomWinSocket);
var i:integer;
begin
  i:=socket.SocketHandle;

  Log('Server OnDisconnect event, SocketID='+inttostr(i));

  //��������� �� ������ ������� �� �������� ���������
  if i=FSocketID1 then
  begin
    Log('First client disconnecting');
    FSocketID1:=-1;
  end
  else if i=FSocketID2 then
  begin
    Log('Second client disconnecting');
    FSocketID2:=-1;
  end
  else
    Log('WARNING! Unknown client disconnecting');
end;

procedure TTransferServer.ClientRead(Sender: TObject; Socket: TCustomWinSocket);
var i,j,k:integer;
pint:^integer;
begin
  k:=socket.SocketHandle;
  i:=socket.ReceiveBuf(FRecv_buffer[0],8192);

  Log('Server OnRead event, SocketID='+inttostr(k)+', Readed '+inttostr(i)+' bytes');

  //��������� �� ��������� ������ �� ������ �������
  if (k<>FSocketID1)and(k<>FSocketID2) then
  begin
    Log('  WARNING! Received data from unknown client, disconnecting');
    socket.Close;
  end;

  //��������� ����� ��������
  case FInstructions.TransferAction of
    tsaReceiveUploadToFTP, tsaReceiveSaveToDisk:
    begin
      //��������� �� ������ ������� ������ ������
      //�� ������ ������ �� ������ ������, �� �� ������ ���� ���������
      if k=FSocketID1 then
      begin
        Log('  Received data from first client');

        //������ �������
        FCounter:=FCounter-i;

        //���������� �����
        j:=length(FBuffer1);
        setlength(FBuffer1,j+i);
        move(FRecv_buffer[0],FBuffer1[j],i);

        //��������� �� ��������� ������� �������� ������
        if FCounter<0 then
        begin
          Log('Received too much data, Counter='+inttostr(FCounter)+', disconnecting client and changing status');
          socket.Close;
          FStatus:=tssError;
          exit;
        end;

        //������� ���������� � ���������� ���-�� ����
        Log('  Left='+inttostr(FCounter));
        //������ ��������� ����� �����
        FLastTime:=gettickcount64;
      end  //if k=FSocketID1 then
      else
      begin
        Log('  WARNING! Received data from unknown client, disconnecting');
        socket.Close;
      end;
    end;  //tsaReceiveUploadToFTP, tsaReceiveSaveToDisk:
    tsaUploadFileFromDisk:
    begin
      //��������� �������
      if k=FSocketID1 then
      begin
        //��������� ������������ ����������� ������
        //��������� �� ������
        if i<12 then
        begin
          Log('  WARNING! Received packet is too small, ignoring. Packet:');
          for j:=0 to i-1 do
            Log('#'+inttostr(j)+'='+inttostr(FRecv_buffer[j]));
          exit;
        end;
        //��������� �� ������ ����
        if FRecv_buffer[0]<>158 then
        begin
          Log('  WARNING! Received packet''s first byte is wrong, ignoring. Packet:');
          for j:=0 to i-1 do
            Log('#'+inttostr(j)+'='+inttostr(FRecv_buffer[j]));
          exit;
        end;
        //��������� �� ��� ������
        j:=(FRecv_buffer[5] shl 8) or (FRecv_buffer[6]);
        if j<>4 then
        begin
          Log('  WARNING! Received packet''s type is wrong, ignoring. Packet:');
          for j:=0 to i-1 do
            Log('#'+inttostr(j)+'='+inttostr(FRecv_buffer[j]));
          exit;
        end;

        //��������� �� ��� ������
        j:=FRecv_buffer[7];
        if j=$FD then  //����� ����������� �����
        begin
          pint:=@FRecv_buffer[8];
          j:=pint^;
          Log('  Received keep-alive packet with counter='+inttostr(j));
          FCounter:=j;
          //������ ��������� ����� �����
          FLastTime:=gettickcount64;
        end
        else if j=$FE then  //����� ��������� ������
        begin
          pint:=@FRecv_buffer[8];
          j:=pint^;
          Log('  Received completion packet with counter='+inttostr(j));
          FCounter:=j;
          //������ ��������� ����� �����
          FLastTime:=gettickcount64;
        end
        else if j=$FF then  //����� ������ ������
        begin
          Log('  Received error packet, setting error, disconnecting and exiting');
          socket.Close;
          FStatus:=tssError;
        end
        else
        begin  //��� �� �������� ������������ ��� ������
          Log('  WARNING! Received packet''s answer type is wrong, ignoring. Packet:');
          for j:=0 to i-1 do
            Log('#'+inttostr(j)+'='+inttostr(FRecv_buffer[j]));
        end;
      end
      else
      begin
        Log('  WARNING! Received data from unknown client, disconnecting');
        socket.Close;
      end;
    end;  //tsaUploadFileFromDisk:
    tsaTunnelOneWay:
    begin
    end;  //tsaTunnelOneWay:
    tsaTunnelBothWays:
    begin
    end;  //tsaTunnelBothWays:
  end;
  
end;

procedure TTransferServer.ClientError(Sender: TObject; Socket: TCustomWinSocket; ErrorEvent: TErrorEvent; var ErrorCode: Integer);
var i:integer;
begin
  i:=socket.SocketHandle;

  Log('Server OnClientError event, ErrorEvent='+inttostr(ord(ErrorEvent))+',  ErrorCode='+inttostr(ErrorCode));

  //��������� �� ������ ������� �� �������� ������
  if i=FSocketID1 then
  begin
    Log('Error on first client, disconnecting and changing status');
    FSocketID1:=-1;
    FStatus:=tssError;
  end
  else if i=FSocketID2 then
  begin
    Log('Error on second client, disconnecting and changing status');
    FSocketID2:=-1;
    FStatus:=tssError;
  end
  else
    Log('WARNING! Error on unknown client, ignoring');

  socket.Close;

  ErrorCode:=0;
end;

procedure TTransferServer.ClientWrite(Sender: TObject; Socket: TCustomWinSocket);
var i:integer;
begin
  i:=socket.SocketHandle;

  Log('Server OnClientWrite event, SocketID='+inttostr(i));

  if FInstructions.TransferAction=tsaUploadFileFromDisk then
  begin
    Log('Sending buffer');

    socket.SendBuf(FBuffer1[0],length(FBuffer1));
  end;
end;

function TTransferServer.GetStatus:TTransferServerStatus;
begin
  Log('GetStatus enter, result='+inttostr(ord(FStatus)));

  result:=FStatus;
end;

procedure TTransferServer.SetLastUpdateTime;
begin
  Log('SetLastUpdateTime enter');

  FLastUpdateTime:=now;
end;

function TTransferServer.GetLastUpdateTime:TDateTime;
begin
  Log('GetLastUpdateTime enter');

  result:=FLastUpdateTime;
end;

procedure TTransferServer.ConfirmDestroy;
begin
  Log('ConfirmDestroy enter');

  FStatus:=tssDoneConfirmed;
end;

procedure TTransferServer.Log(mess:string);
var handl:integer;
temp_mess:string;
begin
  temp_mess:=FormatDateTime('dd.mm.yyyy hh:nn:ss.zzz',now)+'  '+Mess+#13+#10;

  if FOut_path<>'' then
  begin
    if FileExists(FOut_path) then
      handl:=FileOpen(FOut_path,fmOpenReadWrite or fmShareDenyNone)
    else
    begin
      handl:=FileCreate(FOut_path);
      temp_mess:=datetimetostr(now)+#13+#10+temp_mess;
    end;

    if handl<0 then exit;
    if FileSeek(handl,0,2)=-1 then exit;
    if FileWrite(handl,temp_mess[1],length(temp_mess))=-1 then exit;
    FileClose(handl);
  end;

  temp_mess:='';
end;
//======================TTransferServer===================

end.
