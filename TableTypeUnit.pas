unit TableTypeUnit;

interface

uses Windows, Classes, SysUtils, Variants, typesUnit, ZConnection, ZSqlProcessor, ZDataset, ExtCtrls;

type
  TDataTable=class(TObject)
  private
    //FForm:TDataForm;
    //компоненты для работы с БД
    ZConnect:TZConnection;  //соединение с БД
    ZSP:TZSQLProcessor;  //скрипт для разных нужд
    ZQ_request:TZQuery;  //запрос для разных нужд
    KeepAliveTimer:TTimer;  //таймер для вызова скрипта для поддержания связи с БД
    ZSP_KeepAlive:TZSQLProcessor;  //скрипт для поддержания связи с БД

    FCritSection:TRTLCriticalSection;   //критическая секция для доступа к соединению с БД
    FOut:TStrings;   //мемо для вывода информации
    FOut_path:string;    //путь к файлу логов
    FRegisterThread_arr:array of cardinal;   //список ID зарегистрированных потоков

    //процедура-событие для таймера
    procedure KeepAliveOnTimer(Sender: TObject);

    //процедура вывода информации в мемо
    procedure Log(message:string);
    function LogServerMess(path,Mess:string; time:boolean):boolean;
  public
    constructor Create(SQLHost,SQLUser,SQLPass,SQLDatabase:string; Output:TStrings; output_path:string);
    destructor Destroy; override;

    //процедуры работы с потоками
    function RegisterThread(ThrID:cardinal):boolean;
    function RemoveThread(ThrID:cardinal):boolean;
    procedure LogThreads;

    //процедуры работы с клиентами
    function RegisterNewClient(imei:int64; ip_adress,version:string; SockID,TeamViewerID:integer):integer;
    function ClientDisconnected(SockID:integer; Time:TDateTime; imei:int64):boolean;
    function RequestTabloList(var buf:string; SockID:integer):boolean;
    function CreateNewMessageLog(imei:int64; mess_type,mess_level:integer; time:TDateTime; message:string):boolean;

    //сервисные процедуры
    function AddMessageLog(client_id,mess_type,mess_level:integer; time:TDateTime; message:string):boolean;
    function CheckDBConnection:boolean;

    procedure ShowForm;
  end;

implementation

uses ZCompatibility;

constructor TDataTable.Create(SQLHost,SQLUser,SQLPass,SQLDatabase:string; Output:TStrings; output_path:string);
begin
  //назначаем мемо
  FOut:=output;
  //назначаем путь для файла логов
  FOut_path:=output_path;
  Log('Creating table started at '+datetimetostr(now));
  //создаём критическую секцию
  InitializeCriticalSectionAndSpinCount(FCritSection,$1000);
  Log('Critical section for DB acsess created');
  //обнуляем переменные
  setlength(FRegisterThread_arr,0);

  try
  Log('Creating DB connection');
  //создаём соединение
  ZConnect:=TZConnection.Create(nil);
  ZConnect.AutoEncodeStrings:=false;
  ZConnect.ClientCodepage:='cp1251';
  ZConnect.ControlsCodePage:=cGET_ACP;
  ZConnect.Port:=0;
  ZConnect.Protocol:='mysql-5';
  ZConnect.HostName:=SQLHost;
  ZConnect.Database:=SQLDatabase;
  ZConnect.User:=SQLUser;
  ZConnect.Password:=SQLPass;
  Log('Creating DB connection complete, connecting to DB');
  ZConnect.Connect;
  if CheckDBConnection=true then
  begin
    //создаём все скрипты и запросы
    Log('Connected to DB, creating scripts and requests');

    ZSP:=TZSQLProcessor.Create(nil);
    ZSP.Connection:=ZConnect;
    ZQ_request:=TZQuery.Create(nil);
    ZQ_request.Connection:=ZConnect;

    //создаём секцию для поддержания коннекта к БД
    Log('Creating KeepAlive section');
    ZSP_KeepAlive:=TZSQLProcessor.Create(nil);
    ZSP_KeepAlive.Connection:=ZConnect;
    ZSP_KeepAlive.Script.Clear;
    ZSP_KeepAlive.Script.Add('insert into log (client_id,message,message_type,message_level,datetime) values (0,''Echo keep alive connection'',0,0,:datetime)');
    KeepAliveTimer:=TTimer.Create(nil);
    KeepAliveTimer.Enabled:=false;
    KeepAliveTimer.Interval:=3600000;
    KeepAliveTimer.OnTimer:=KeepAliveOnTimer;
    KeepAliveTimer.Enabled:=true;

    //запускаем скрипты на инициализацию БД
    Log('Creating KeepAlive section complete, executing scripts');
    //подчищаем таблицу клиентов
    ZSP.Script.Clear;
    ZSP.Script.Add('update clients set connected=0, socket_id=0;');
    ZSP.Execute;
    Log('Clients initialized succsessfully');
    //делаем лог что сервер запускается
    ZSP.Script.Clear;
    ZSP.Script.Add('insert into log (client_id,message,message_type,message_level,datetime) values (0,''Server starting up'',0,0,'''+Formatdatetime('yyyy-mm-dd hh:nn:ss',now)+''');');
    ZSP.Execute;
    Log('Log updated succsessfully');
    Log('Database initialization complete sucsessfuly');
  end
  else Log('Connection to DB failed');

  except
    on e:exception do
      Log('WARNING! Database initialization failed with exception, message:'+e.Message);
  end;
end;

destructor TDataTable.Destroy;
begin
  Log('Starting to destroy Table +++++++++++++++++====================++++++++++++++++++++++');

  //делаем лог что сервер останавливается
  ZSP.Script.Clear;
  ZSP.Script.Add('insert into log (client_id,message,message_type,message_level,datetime) values (0,''Server shutdown'',0,0,'''+Formatdatetime('yyyy-mm-dd hh:nn:ss',now)+''');');
  ZSP.Execute;
  Log('Log updated succsessfully');

  //очищаем всех клиентов
  ZSP.Script.Clear;
  ZSP.Script.Add('update clients set connected=0;');
  ZSP.Execute;
  Log('Clients updated succsessfully');

  setlength(FRegisterThread_arr,0);

  //уничтожаем критическую секцию
  DeleteCriticalSection(FCritSection);
  Log('Critical cestion destroyed');

  Log('Table destroy complete ============================================================');
end;

function TDataTable.RegisterThread(ThrID:cardinal):boolean;
var i:integer;
begin
  result:=false;

  EnterCriticalSection(FCritSection);
  Log('Entered register thread');

  //проверяем, есть ли такой же зарегистрированный поток
  for i:=0 to length(FRegisterThread_arr)-1 do
    if FRegisterThread_arr[i]=ThrID then
    begin
      LeaveCriticalSection(FCritSection);
      exit;
    end;

  //добавляем новый поток в массив
  i:=length(FRegisterThread_arr);
  setlength(FRegisterThread_arr,i+1);
  FRegisterThread_arr[i]:=ThrID;

  Log('Register thread complete');
  LeaveCriticalSection(FCritSection);

  result:=true;
end;

function TDataTable.RemoveThread(ThrID:cardinal):boolean;
var i,j,k:integer;
begin
  result:=false;

  EnterCriticalSection(FCritSection);
  Log('Entered remove thread');

  //ищем поток
  j:=-1;
  k:=length(FRegisterThread_arr);
  for i:=0 to k-1 do
    if FRegisterThread_arr[i]=ThrID then
    begin
      j:=i;
      break;
    end;

  if j=-1 then
  begin
    LeaveCriticalSection(FCritSection);
    exit;
  end;

  //если находим элемент
  move(FRegisterThread_arr[j+1],FRegisterThread_arr[j],(k-j-1)*sizeof(cardinal));
  setlength(FRegisterThread_arr,k-1);

  Log('Remove thread complete');
  LeaveCriticalSection(FCritSection);

  result:=true;
end;

procedure TDataTable.LogThreads;
var i:integer;
begin
  EnterCriticalSection(FCritSection);
  Log('Registered threads:');
  for i:=0 to length(FRegisterThread_arr)-1 do
    Log(' #'+inttostr(i)+'='+inttostr(FRegisterThread_arr[i]));
  LeaveCriticalSection(FCritSection);
end;

function TDataTable.RegisterNewClient(imei:int64; ip_adress,version:string; SockID,TeamViewerID:integer):integer;
var str:string;
i:integer;
begin
  result:=-1;

  EnterCriticalSection(FCritSection);
  Log('RegisterNewClient enter with IMEI='+inttostr(imei)+'   IP='+ip_adress+'   Ver='+version+'   SockID='+inttostr(SockID)+'   TeamViewerID='+inttostr(TeamViewerID));

  //проверяем IMEI
  if ZQ_request.Active then ZQ_request.Close;
  str:=inttostr(imei);
  for i:=length(str) to 15 do
    str:='0'+str;
  ZQ_request.SQL.Clear;
  ZQ_request.SQL.Add('select * from clients where IMEI="'+str+'"');
  try
    ZQ_request.Open;
  except
    on e:exception do
    begin
      Log('Exit from RegisterNewClient with error: exception on opening querry with message:'+e.Message);
      LeaveCriticalSection(FCritSection);
      exit;
    end;
  end;

  //если IMEI вообще нет, то выходим и возвращаем -1
  if ZQ_request.RecordCount<1 then
  begin
    Log('Exit from RegisterNewClient with error: client is not in the database (-1)');
    LeaveCriticalSection(FCritSection);
    exit;
  end;

  ZQ_request.First;
  //если клиент уже подсоединен, возвращаем ошибку -2
  if ZQ_request.FieldByName('connected').AsInteger<>0 then
  begin
    //Log('Exit from RegisterNewClient with error: client is already connected');
    Log('Warning in RegisterNewClient: client is already connected');
    result:=-2;
    //LeaveCriticalSection(FCritSection);
    //exit;
  end;

  //если imei свободен, записываем в него данные
  ZQ_request.Edit;
  ZQ_request.FieldByName('connected').AsInteger:=1;
  ZQ_request.FieldByName('ip_adress').AsString:=ip_adress;
  ZQ_request.FieldByName('service_version').AsString:=version;
  ZQ_request.FieldByName('socket_id').AsInteger:=SockID;
  if TeamViewerID<>0 then ZQ_request.FieldByName('teamviewer_id').AsString:=inttostr(TeamViewerID);
  ZQ_request.Post;

  //записываем лог
  AddMessageLog(0,0,0,now,'Registered new client with IP='+ip_adress+';  SockID='+inttostr(SockID)+';  IMEI='+inttostr(imei)+';  Ver='+version);

  //возвращаем содержимое поля operator
  //0 - обычное табло
  //1 - оператор
  //todo: сделать администратора
  i:=ZQ_request.FieldByName('operator').AsInteger;

  ZQ_request.Close;

  if result<>-2 then result:=i;

  Log('Return from RegisterNewClient successfull with result='+inttostr(result));

  LeaveCriticalSection(FCritSection);
end;

function TDataTable.ClientDisconnected(SockID:integer; Time:TDateTime; imei:int64):boolean;
var str:string;
begin
  result:=false;

  EnterCriticalSection(FCritSection);
  Log('Entered DisconnectClient with SockID='+inttostr(SockID)+'  IMEI='+inttostr(imei));

  ZSP.Script.Clear;
  ZSP.Script.Add('update clients set connected=0, socket_id=0 where socket_id='+inttostr(SockID));

  try
    ZSP.Execute;
  except
    on e:exception do
    begin
      Log('Exception when executing script in DisconnectClient with message:'+e.Message);
      LeaveCriticalSection(FCritSection);
      exit;
    end;
  end;

  //записываем лог
  str:='Disconnected client with SockID='+inttostr(SockID);
  if imei=0 then str:=str+';  IMEI=unknown'
  else str:=str+';  IMEI='+inttostr(imei);
  AddMessageLog(0,0,0,time,str);

  result:=true;

  Log('Return from DisconnectClient successfully');

  LeaveCriticalSection(FCritSection);
end;

function TDataTable.RequestTabloList(var buf:string; SockID:integer):boolean;
var i,j:integer;
begin
  result:=false;

  EnterCriticalSection(FCritSection);
  Log('RequestTabloList enter, list requesting socket#'+inttostr(sockid));

  if ZQ_request.Active then ZQ_request.Close;
  ZQ_request.SQL.Clear;
  ZQ_request.SQL.Add('select * from clients where operator<>1');
  ZQ_request.Open;

  buf:=chr(ZQ_request.FieldCount);
  i:=ZQ_request.RecordCount;   //кол-во колонок
  buf:=buf+chr((i and $FF00)shr 8)+chr(i and $FF);  //кол-во записей

  Log('Request FieldCount='+inttostr(ZQ_request.FieldCount)+',  RecordCount='+inttostr(i));

  //названия колонок
  for i:=0 to ZQ_request.FieldCount-1 do
    buf:=buf+chr(length(ZQ_request.Fields[i].FieldName))+ZQ_request.Fields[i].FieldName;

  //данные
  if ZQ_request.RecordCount<>0 then
  begin
    ZQ_request.First;
    for j:=0 to ZQ_request.RecordCount-1 do
    begin
      for i:=0 to ZQ_request.FieldCount-1 do
        buf:=buf+chr(length(ZQ_request.FieldByName(ZQ_request.Fields[i].FieldName).AsString))+ZQ_request.FieldByName(ZQ_request.Fields[i].FieldName).AsString;

      ZQ_request.Next;
    end;
  end;

  ZQ_request.Close;

  //buf:='';

  Log('RequestTabloList exit, BufLen='+inttostr(length(buf)));
  result:=true;
  LeaveCriticalSection(FCritSection);
end;

function TDataTable.CreateNewMessageLog(imei:int64; mess_type,mess_level:integer; time:TDateTime; message:string):boolean;
var str:string;
i:integer;
begin
  result:=false;

  EnterCriticalSection(FCritSection);
  Log('Entered CreateNewMessage with IMEI='+inttostr(imei)+' ; MessType='+inttostr(mess_type)+' ; MessLevel='+inttostr(mess_level));

  //проверяем IMEI
  if ZQ_request.Active then ZQ_request.Close;
  str:=inttostr(imei);
  for i:=length(str) to 15 do
    str:='0'+str;
  ZQ_request.SQL.Clear;
  ZQ_request.SQL.Add('select * from clients where IMEI="'+str+'" and connected=1');
  try
    ZQ_request.Open;
  except
    on e:exception do
    begin
      Log('Exit from CreateNewMessage with error: exception on opening querry with message:'+e.Message);
      LeaveCriticalSection(FCritSection);
      exit;
    end;
  end;

  if ZQ_request.RecordCount<1 then
  begin
    Log('Exit from CreateNewMessage with error: no such clients found');
    LeaveCriticalSection(FCritSection);
    exit;
  end;

  //читаем ID
  ZQ_request.First;
  i:=ZQ_request.FieldByName('id').AsInteger;

  if AddMessageLog(i,mess_type,mess_level,time,message)=false then
  begin
    Log('Exit from CreateNewMessage with error: AddMessageLog returned false');
    LeaveCriticalSection(FCritSection);
    exit;
  end;

  result:=true;

  Log('Exit from CreateNewMessage succsessfilly');

  LeaveCriticalSection(FCritSection);
end;

function TDataTable.AddMessageLog(client_id,mess_type,mess_level:integer; time:TDateTime; message:string):boolean;
begin
  result:=false;

  EnterCriticalSection(FCritSection);

  ZSP.Script.Clear;
  ZSP.Script.Add('insert into log (client_id,message,message_type,message_level,datetime) values ('+
  '"'+inttostr(client_id)+'",'+
  '"'+message+'",'+
  '"'+inttostr(mess_type)+'",'+
  '"'+inttostr(mess_level)+'",'+
  '"'+FormatDateTime('yyyy.mm.dd hh:nn:ss.zzz',time)+'");');

  try
    ZSP.Execute;
  except
    on e:exception do
    begin
      Log('Exception in AddMessageLog with message:'+e.Message);
      LeaveCriticalSection(FCritSection);
      exit;
    end;
  end;

  result:=true;

  LeaveCriticalSection(FCritSection);
end;

function TDataTable.CheckDBConnection:boolean;
begin
  result:=ZConnect.Connected;
end;

procedure TDataTable.ShowForm;
begin
  //if FForm<>nil then FForm.Show;
end;

procedure TDataTable.KeepAliveOnTimer(Sender: TObject);
begin
  {ZSP_KeepAlive.ParamByName('client_id').AsInteger:=0;
  ZSP_KeepAlive.ParamByName('message').AsString:='Echo keep alive connection';
  ZSP_KeepAlive.ParamByName('message_type').AsInteger:=0;
  ZSP_KeepAlive.ParamByName('message_level').AsInteger:=0;   }

  Log('KeepAliveOnTimer');

  try
    ZSP_KeepAlive.ParamByName('datetime').AsDateTime:=now;
    ZSP_KeepAlive.Execute;
  except
    on e:exception do
      Log('WARNING! Exception in OnKeepAliveTimer event with message:'+e.Message);
  end;
end;

procedure TDataTable.Log(message:string);
begin
  //пишем в файл логов
  LogServerMess(FOut_path,message,true);
  //пишем в мемо
  if FOut<>nil then FOut.Add(FormatDateTime('hh:nn:ss.zzz',now)+'  '+message);
end;

function TDataTable.LogServerMess(path,Mess:string; time:boolean):boolean;
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

end.
