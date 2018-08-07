unit mainUnit;

interface

uses
  Windows, Messages, SysUtils, Classes, Graphics, Controls, SvcMgr, Dialogs,
  QueueTypeUnit, TableTypeUnit, ThreadTypeUnit, INIFiles;

const ServiceDescription='Rapid Maintence Server enables the administrator to monitor and maintain dysplays for Mosgortrans';

type
  TMainThread = class(TThread)
  private
    CleanQueueTime:int64;  //время последнего вызова процедуры очистки
    CleanQueueTimeInterval:int64;  //интервал вызова процедуры очистки
    ThreadDistributionTime:int64;  //время последнего вызова процедуры распределения нагрузки
    ThreadDistributionTimeInterval:int64;  //интервал вызова процедуры распределения нагрузки
    UpdateParametersTime:int64;  //время последнего вызова процедуры обновления параметров сервиса
    UpdateParametersTimeInterval:int64;  //интервал вызова процедуры обновления параметров сервиса

    procedure ServerShutdown;  //процедура для правильного выключения сервера
    procedure CleanOnTimer;  //процедура для подчищения динамической памяти
    procedure ThreadDistributionOnTimer;  //процедура для распределения нагрузки на потоки
    procedure UpdateParametersOnTimer;  //процедура для обновления параметров сервиса
  public
    constructor Create;
    destructor Destroy; override;

    procedure Execute; override;  //основной цикл
  end;

  TRMServer_main = class(TService)
    procedure ServiceStart(Sender: TService; var Started: Boolean);
    procedure ServiceStop(Sender: TService; var Stopped: Boolean);
    procedure ServiceShutdown(Sender: TService);
    procedure ServiceAfterInstall(Sender: TService);
  private
    { Private declarations }
  public
    function GetServiceController: TServiceController; override;
    { Public declarations }
  end;

var
  RMServer_main: TRMServer_main;

  //общие переменные для работы сервиса
  LogFileName:string;  //путь к файлу логов
  WorkThread:TMainThread;  //основной поток

  //внутренние переменные
  Queue:TQueue;  //главная очередь сервера (содержит TCP-сервер)
  Table:TDataTable;  //объект для работы с БД
  Threads:TStatisticsData;  //запись, содержащая всю инфу о потоках
  PThreads:PTStatisticsData;  //указатель на запись о потоках

  AutoCreateThreadsCount:integer;  //количество автоматически создаваемых потоков обработки
  Inifile:TINIFile;  //файл настроек
  QueuePort:integer;  //порт, на котором будет открыватся TCP-сервер
  QueueLogPath,ThreadsLogPath,TableLogPath:string;  //пути к файлам логов, где будут сохранятся логи от компонентов
  SQLHost,SQLDB,SQLUser,SQLPass:string;   //настройки для работы с БД
  FTPHost,FTPUser,FTPPass:string;     //настройки для работы с FTP
  FTPPort:integer;
  UpdatePath:string;  //путь к эталонному файлу клиента для апдейта клиентов
  UpdateVersion:string;  //версия эталонного клиента
  UpdateVersionMajor,UpdateVersionMinor,UpdateVersionRelease,UpdateVersionBuild:byte;
  UpdateCRC:integer;  //контрольная сумма эталонного клиента
  UpdateFileSize:integer;  //размер файла эталонного клиента
  UpdateSettingsPath:string;  //путь к эталонному файлу настроек клиента для апдейта настроек
  UpdateSettingsCRC:integer;  //контрольная сумма эталонных настроек
  UpdateSettingsFileSize:integer;  //размер файла эталонных настроек

function GetModuleFileNameStr(Instance: THandle): string;
function Log(Mess:string; time:boolean = true):boolean;
procedure ServiceStopShutdown(CheckMainThread:boolean);

function ChangeServiceConfig2A(hService: LongWord; dwInfoLevel: DWORD; var lpInfo): BOOL; stdcall; external advapi32 name 'ChangeServiceConfig2A';
function OpenSCManager(lpMachineName, lpDatabaseName: PChar; dwDesiredAccess: DWORD): LongWord; stdcall; external advapi32 name 'OpenSCManagerA';
function OpenService(hSCManager: LongWord; lpServiceName: PChar; dwDesiredAccess: DWORD): LongWord; stdcall; external advapi32 name 'OpenServiceA';
function CloseServiceHandle(hSCObject: LongWord): BOOL; stdcall; external advapi32 name 'CloseServiceHandle';

implementation

uses typesUnit, ZLibEx;

{$R *.DFM}

procedure ServiceController(CtrlCode: DWord); stdcall;
begin
  RMServer_main.Controller(CtrlCode);
end;

function TRMServer_main.GetServiceController: TServiceController;
begin
  Result := ServiceController;
end;

//-------------------TMainThread----------------------
constructor TMainThread.Create;
begin
  Log('Entered MainThread create');
  inherited Create(true);
  FreeOnTerminate:=false;
  Priority:=tpNormal;

  Resume;
  Log('Exit MainThread create');
end;

destructor TMainThread.Destroy;
begin
  Log('Entered MainThread destroy');

  Log('Exit MainThread destroy');
end;

procedure TMainThread.Execute;
var msg:TMSG;
t:int64;
i,j,k:integer;
str:string;
begin
  Log('Main execute enter');

  //инициализация
  t:=gettickcount64;
  CleanQueueTime:=t;
  CleanQueueTimeInterval:=60000;
  ThreadDistributionTime:=t;
  ThreadDistributionTimeInterval:=500;
  UpdateParametersTime:=t;
  UpdateParametersTimeInterval:=120000;
  UpdateVersion:='1.0.0.0';
  UpdateVersionMajor:=1;
  UpdateVersionMinor:=0;
  UpdateVersionRelease:=0;
  UpdateVersionBuild:=0;
  UpdateCRC:=0;
  UpdateFileSize:=0;
  UpdateSettingsCRC:=0;
  UpdateSettingsFileSize:=0;

  //обновляем параметры сервиса
  UpdateParametersOnTimer;

  sleep(2000);

  //создаём очередь
  Log('Begin to create queue');
  Queue:=TQueue.Create(QueuePort,nil,false,QueueLogPath);

  sleep(1000);

  //создаем таблицу
  Log('Begin to create Table');
  Table:=TDataTable.Create(SQLHost,SQLUser,SQLPass,SQLDB,nil,TableLogPath);

  sleep(200);
  //проверяем, всё ли нормально создалось в таблице
  if Table.CheckDBConnection=false then
  begin
    Log('WARNING! Table creation failed, exiting');
    ServiceStopShutdown(false);
    //todo: дописать освобождение компонентов как в Destroy если надо
    RMServer_main.Status:=csStopped;
    exit;
  end;

  sleep(1000);

  //создаём потоки
  for k:=1 to AutoCreateThreadsCount do
  begin
    Log('Begin to create thread #'+inttostr(k));

    //определяем имя файла логов для нового потока (чтобы каждый поток писал в свой файл логов)
    str:=ExtractFileName(ThreadsLogPath);
    i:=pos('.',str);
    j:=length(Threads.ThreadArr);
    if i=0 then str:=str+inttostr(j)
    else insert(inttostr(j),str,i);

    str:=ExtractFilePath(ThreadsLogPath)+str;

    //устанавливаем задержку (если что потоки сами уменьшат её)
    j:=20;

    //создаём новый поток
    TWorkThread.Create(false,Queue,Table,nil,false,j,PThreads,str);
    sleep(1000);
      {
    TWorkThread.Create(false,Queue,Table,nil,false,50,PThreads,ThreadsLogPath);
    sleep(1000);
    TWorkThread.Create(false,Queue,Table,nil,true,50,PThreads,ThreadsLogPath);
    sleep(1000);  }
  end;

  //создаём поток для сервисных клиентов
  Log('Begin to create operator thread #'+inttostr(AutoCreateThreadsCount+1));

  //определяем имя файла логов для нового потока (чтобы каждый поток писал в свой файл логов)
  str:=ExtractFileName(ThreadsLogPath);
  i:=pos('.',str);
  j:=length(Threads.ThreadArr);
  if i=0 then str:=str+inttostr(j)
  else insert(inttostr(j),str,i);

  str:=ExtractFilePath(ThreadsLogPath)+str;

  //устанавливаем задержку (если что потоки сами уменьшат её)
  j:=20;

  //создаём новый поток
  TWorkThread.Create(false,Queue,Table,nil,true,j,PThreads,str);
  sleep(1000);

  //открываем сервер
  Queue.OpenServer;

  Log('Creting complete, entering main cicle');

  while not(Terminated) do
  begin
    //Log('OnMainWorkCicle');

    t:=gettickcount64;

    //таймер на очистку
    if CleanQueueTime+CleanQueueTimeInterval<t then
    begin
      CleanOnTimer;
      CleanQueueTime:=t;
    end;

    //таймер на распределение нагрузки
    if ThreadDistributionTime+ThreadDistributionTimeInterval<t then
    begin
      ThreadDistributionOnTimer;
      ThreadDistributionTime:=t;
    end;

    //таймер для обновления параметров сервиса
    if UpdateParametersTime+UpdateParametersTimeInterval<t then
    begin
      UpdateParametersOnTimer;
      UpdateParametersTime:=t;
    end;


    //перехват и передача сообщений для дочерних потоков
    if PeekMessage(Msg, 0, 0, 0, PM_REMOVE) then
    begin
      TranslateMessage(Msg);
      DispatchMessage(Msg);
    end;

    sleep(100);
  end;

  Log('After main cicle enter, begining to shutdown the server');
  ServerShutdown;
  Log('Shutdown complete');
end;

procedure TMainThread.ServerShutdown;
var i,j,k:integer;
begin
  //закрываем сервер
  Queue.Shutdown;

  //ждём обработки всех пакетов
  j:=Queue.ElementCount;
  k:=0;
  while Queue.ElementCount<>0 do
  begin
    inc(k);
    sleep(1000);

    i:=Queue.ElementCount;
    if j<>i then j:=i
    else
    begin
      Queue.CleanSocketArrays;   //если кол-во пакетов в очереди не изменяется, очищаем
      sleep(1000);
    end;
  end;

  Log('Shutdown and cleaning complete (iteration count='+inttostr(k)+') ------------------------------');

  //уничтожаем потоки обработки
  for i:=0 to length(Threads.ThreadArr)-1 do
  begin
    Threads.ThreadArr[i].Terminate;
    Threads.ThreadArr[i].WaitFor;
    Threads.ThreadArr[i].Free;
    Threads.ThreadArr[i]:=nil;
  end;

  sleep(300);
  //application.ProcessMessages;

  //уничтожаем таблицу
  Table.Free;

  sleep(300);
  //application.ProcessMessages;

  //уничтожаем очередь
  Queue.Terminate;
  Queue.WaitFor;
  Queue.Free;

  sleep(300);
  //application.ProcessMessages;

  //sleep(1000);
end;

procedure TMainThread.CleanOnTimer;
var i,j:integer;
begin
  Log('CleanOnTimer event');

  if Queue<>nil then
  begin
    i:=Queue.CleanSocketArrays;

    j:=CleanQueueTimeInterval;
    if i=0 then j:=j*2
    else if i>0 then j:=j div 3;

    if j<60000 then j:=60000;
    if j>1200000 then j:=1200000;

    CleanQueueTimeInterval:=j;
  end;
end;

procedure TMainThread.ThreadDistributionOnTimer;
var i,j:integer;
str:string;
begin
  //переделать так, чтобы было ограничение на кол-во клиентов на один поток, которое можно регулировать с помощью INI

  //сначала проверяем время
  if (Threads.LastModify+180000)<gettickcount64 then  //каждые 3 минуты
  begin
    if Threads.CreatingNewThread=true then    //если мы создавали новый поток и попали сюда, то поидее уже произошло распределение сокетов и надо сбросить все параметры
    begin
      Threads.NeedNewThread:=0;
      Threads.CreatingNewThread:=false;
      exit;
    end;

    //проверяем, нужно ли создавать новый поток
    if Threads.NeedNewThread>0 then
    begin
      Log('WARNING! Creating new thread and relocation sockets');

      //определяем имя файла логов для нового потока (чтобы каждый поток писал в свой файл логов)
      str:=ExtractFileName(ThreadsLogPath);
      i:=pos('.',str);
      j:=length(Threads.ThreadArr);
      if i=0 then str:=str+inttostr(j)
      else insert(inttostr(j),str,i);

      str:=ExtractFilePath(ThreadsLogPath)+str;

      //устанавливаем задержку (если что потоки сами уменьшат её)
      j:=20;

      //создаём новый поток
      TWorkThread.Create(false,Queue,Table,nil,false,j,PThreads,str);

      //запоминаем время и сигнализируем о том, что мы создаём новый поток
      Threads.LastModify:=gettickcount64;
      Threads.CreatingNewThread:=true;

      //сигнализируем потокам о перераспределении сокетов и устанавливаем задержку для других потоков
      for i:=0 to length(Threads.ThreadArr)-2 do
      begin
        Threads.ThreadArr[i].SetRelocation;
        Threads.ThreadArr[i].SetWorkDelay(j);
      end;
    end;
  end;
end;

procedure TMainThread.UpdateParametersOnTimer;
var f:file;
i,j,k,i1,i2,i3,i4:integer;
buf:array of byte;
str,str1,str2:string;
begin
  Log('UpdateParametersOnTimer event');

  //обновляем параметры для настроек
  //читайем файл
  assignfile(f,UpdatePath);
  reset(f,1);
  j:=filesize(f);
  setlength(buf,j);
  blockread(f,buf[0],j);
  closefile(f);

  //считаем CRC
  k:=ZCRC32(0,buf[0],length(buf));
  setlength(buf,0);

  //считаем версию
  str:=FileVersion(UpdatePath);
  str2:=str;
  i1:=0;
  i2:=0;
  i3:=0;
  i4:=0;
  try
    if str<>'' then
    begin
      i:=pos('.',str);  //1.10.100.255
      if i<>0 then
      begin
        str1:=copy(str,1,i-1);
        delete(str,1,i);
        i1:=strtoint(str1);
        i:=pos('.',str);  //10.100.255
        if i<>0 then
        begin
          str1:=copy(str,1,i-1);
          delete(str,1,i);
          i2:=strtoint(str1);
          i:=pos('.',str);  //100.255
          if i<>0 then
          begin
            str1:=copy(str,1,i-1);
            delete(str,1,i);
            i3:=strtoint(str1);
            i:=pos('.',str);  //255
            if i=0 then i4:=strtoint(str);
          end;
        end;
      end;
    end;
  except
    on e:exception do
    begin
      i1:=0;
      i2:=0;
      i3:=0;
      i4:=0;
    end;
  end;

  //переписываем параметры
  UpdateVersion:=str2;
  UpdateCRC:=k;
  UpdateVersionMajor:=i1;
  UpdateVersionMinor:=i2;
  UpdateVersionRelease:=i3;
  UpdateVersionBuild:=i4;
  UpdateFileSize:=j;

  Log('ClientFileVersion='+UpdateVersion);
  Log('ClientFileCRC32='+inttohex(UpdateCRC,8));
  Log('ClientFileSize='+inttostr(UpdateFileSize));


  //обновляем параметры для настроек
  //читайем файл
  assignfile(f,UpdateSettingsPath);
  reset(f,1);
  j:=filesize(f);
  setlength(buf,j);
  blockread(f,buf[0],j);
  closefile(f);

  //считаем CRC
  k:=ZCRC32(0,buf[0],length(buf));
  setlength(buf,0);

  UpdateSettingsCRC:=k;
  UpdateSettingsFileSize:=j;

  Log('ClientSettingsCRC32='+inttohex(UpdateSettingsCRC,8));
  Log('ClientSettingsFileSize='+inttostr(UpdateSettingsFileSize));

  Log('UpdateParametersOnTimer event exit');
end;
//===================TMainThread======================

procedure ServiceStopShutdown(CheckMainThread:boolean);
begin
  Log('Stop/Shutdown procedure began');

  if Assigned(WorkThread)and(CheckMainThread=true) then
  begin
    Log('Begin to stop thread');
    if WorkThread.Suspended then WorkThread.Resume;
    WorkThread.Terminate;
    WorkThread.WaitFor;
    FreeAndNil(WorkThread);
    Log('Thread stopped sucsessfuly');
  end;

  Log('Stop/Shutdown procedure complete');
end;

procedure TRMServer_main.ServiceStart(Sender: TService; var Started: Boolean);
var str:string;
i:integer;
begin
  //меняем текущую папку
  SetCurrentDirectory(PChar(ExtractFilePath(GetModuleFileNameStr(0))));

  //проверка на существование нужных директорий
  if DirectoryExists('Logs')=false then
    CreateDirectory('Logs',nil);
  if DirectoryExists('TransferLogs')=false then
    CreateDirectory('TransferLogs',nil);

  //инициализация переменных
  LogFileName:='Logs\output.log';

  setlength(Threads.ThreadArr,0);
  Threads.LastModify:=0;
  Threads.NeedNewThread:=0;
  Threads.CreatingNewThread:=false;
  PThreads:=@Threads;

  Log('==================================================================================================',false);
  //определяем версию сервера и выводим в лог
  str:=FileVersion(GetModuleFileNameStr(0));
  if str='' then str:='unknown';
  Log('Starting server...  ver. '+str);

  //инициализация INI-файла
  Log('Creating INI file handle');
  str:=ChangeFileExt(GetModuleFileNameStr(0), '.INI');
  if not(FileExists(str)) then
  begin
    Log('WARNING! INI file doesn''t exists, creating');
    //todo: сделать создание INI-файла со всеи параметрами
    i:=FileCreate(str);
    FileClose(i);
  end;
  Inifile:=TINIFile.Create(str);

  //чтение параметров из INI-файла
  Log('INI file handle obtained, reading parameters');
  Log('-----------------------',false);
  QueuePort:=INIFile.ReadInteger('Main','Port',750);
  QueueLogPath:=INIFile.ReadString('Main','QueueLog','Logs\queue.log');
  ThreadsLogPath:=INIFile.ReadString('Main','ThreadsLog','Logs\threads.log');
  TableLogPath:=INIFile.ReadString('Main','TableLog','Logs\table.log');
  AutoCreateThreadsCount:=INIFile.ReadInteger('Main','WorkThreadsCount',1);
  SQLHost:=INIFile.ReadString('SQL','Host','localhost');
  SQLDB:=INIFile.ReadString('SQL','Database','client_server_test');
  SQLUser:=INIFile.ReadString('SQL','User','root');
  SQLPass:=INIFile.ReadString('SQL','Pass','1');
  FTPHost:=INIFile.ReadString('FTP','Host','46.61.227.42');
  FTPPort:=INIFile.ReadInteger('FTP','Port',21);
  FTPUser:=INIFile.ReadString('FTP','Login','mosgortrans');
  FTPPass:=INIFile.ReadString('FTP','Pass','12_QvTm75#');
  UpdatePath:=ExpandFileName(INIFile.ReadString('Update','ClientPath','Client\TabloService.exe'));
  UpdateSettingsPath:=ExpandFileName(INIFile.ReadString('Update','ClientSettingsPath','Client\TabloService.ini'));

  Log('Readed ServerPort='+inttostr(QueuePort));
  Log('Readed QueueLogPath='+QueueLogPath);
  Log('Readed ThreadsLogPath='+ThreadsLogPath);
  Log('Readed TableLogPath='+TableLogPath);
  Log('Readed WorkThreadCount='+inttostr(AutoCreateThreadsCount));
  Log('Readed SQLHost='+SQLHost);
  Log('Readed SQLDatabase='+SQLDB);
  Log('Readed SQLUser='+SQLUser);
  Log('Readed SQLPassword='+SQLPass);
  Log('Readed FTPHost:port='+FTPHost+':'+inttostr(FTPPort));
  Log('Readed FTPUser='+FTPUser);
  Log('Readed FTPPassword='+FTPPass);
  Log('Readed UpdatePath='+UpdatePath);
  Log('Readed UpdateSettingsPath='+UpdateSettingsPath);
  Log('-----------------------',false);


  //создание основного потока
  Log('Creating main thread');
  WorkThread:=TMainThread.Create;
  WorkThread.FreeOnTerminate:=false;
  WorkThread.Resume;

  Started:=true;
  Log('Start service complete');
end;

procedure TRMServer_main.ServiceStop(Sender: TService; var Stopped: Boolean);
begin
  Log('Stop service event enter');
  ServiceStopShutdown(true);

  Log('Stop service event complete');
  Stopped:=true;
end;

procedure TRMServer_main.ServiceShutdown(Sender: TService);
begin
  Log('Shutdown event enter');
  ServiceStopShutdown(True);

  Log('Shutdown event complete');
end;

procedure TRMServer_main.ServiceAfterInstall(Sender: TService);
type
  SERVICE_DESCRIPTION = record
    lpDescription : PAnsiChar;
  end;
var
sdBuf : SERVICE_DESCRIPTION;
h_manager, h_svc: LongWord;
begin
  //Изменение описания сервиса
  h_manager := OpenSCManager('' ,nil, $0001);  //h_manager := OpenSCManager('' ,nil, SC_MANAGER_CONNECT);
  if h_manager > 0 then
  begin
    h_svc := OpenService(h_manager, PChar(Name), $0002);   //h_svc := OpenService(h_manager, PChar(Name), SERVICE_CHANGE_CONFIG);
    if h_svc > 0 then
    begin
      sdBuf.lpDescription := PAnsiChar(ServiceDescription);
      ChangeServiceConfig2A(h_svc, 1, sdBuf);
      CloseServiceHandle(h_svc);
    end; //if h_svc > 0 then
    CloseServiceHandle(h_manager);
  end; //if h_manager > 0 then
end;

function GetModuleFileNameStr(Instance: THandle): string;
var
  buffer: array [0..MAX_PATH] of Char;
begin
  GetModuleFileName( Instance, buffer, MAX_PATH);
  Result := buffer;
end;

function Log(Mess:string; time:boolean = true):boolean;
var handl:integer;
temp_mess:string;
begin
  result:=false;
  temp_mess:=Mess+#13+#10;
  if time=true then temp_mess:=FormatDateTime('dd.mm.yyyy hh:nn:ss.zzz',now)+'  '+temp_mess;

  if LogFileName<>'' then
  begin
    if FileExists(LogFileName) then
      handl:=FileOpen(LogFileName,fmOpenReadWrite or fmShareDenyNone)
    else
      handl:=FileCreate(LogFileName);

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
