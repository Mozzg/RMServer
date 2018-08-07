unit ThreadTypeUnit;

interface

uses Classes, QueueTypeUnit, TableTypeUnit, typesUnit, ExtCtrls;

type
  PTStatisticsData=^TStatisticsData;

  TWorkThread=class(TThread)
  private
    FOut:TStrings;  //мемо для вывода
    FOut_path:string;    //путь к файлу логов
    FHandle:Cardinal;   //хендл для определения уникальности потока. Для того, чтобы не вызывать Self.handle
    FQue:TQueue;   //ссылка на объект очереди для обращения к его функциям
    FTable:TDataTable;  //ссылка на объект таблицы (взаимодействие с БД)
    FStats:PTStatisticsData;   //указатель на запись со статистикой
    FOperator:boolean;  //сигнализирует о том, что это поток для операторов
    FBuffer:byte_arr;   //массив-буфер для хранения пакетов для обработки
    FBuffer_len:integer;   //длинна пакета, который хранится в массиве-буфере
    FPacket_buf:byte_arr;   //массив-буфер для хранения пакетов на отправку
    FWorkDelay:cardinal;  //задержка основного цикла обработки в потоке в мс
    FLastDelayCorrection:int64;  //время последнего изменения задержки основного цикла потока (для динамической задержки основного потока при большой нагрузке)
    FDelayCorrectionDifference:int64;   //разница во времени получения пакета и его обработки. Если это число падает, значит изменения в задержке работает

    FRelocation:boolean;    //переменная для сигнализации о начале релокации

    First,Last:PTThreadSocketElement;   //указатели на первый и последний элементы для хранения информации о сокетах

    FAnswerData:TAnswerData;  //отпределяет, ждёт ли поток ответа

    //процедура для первичной обработки пакета
    procedure Work(SockID:integer; Time:TDateTime);
    function Work_packet(var tp:PTThreadSocketElement; SockID:integer; Time:TDateTime):integer;  //обработчик конкретного пакета для сокета
    procedure CreatePacket(packet_type:word; bufer:string); overload;   //процедура для формирования пакетов для отправки
    procedure CreatePacket(packet_type:word; bufer:byte_arr); overload;
    procedure CreateRelocationPacket(Element:PTThreadSocketElement);  //процедура создания пакета релокации сокета
    procedure Check_LastActive;    //процедура проверяющая последнее действие с сокетом. Если долго не было действий - отсоединяет сокет
    procedure Check_Relocation;    //процедура для проверки и выполнения релокации сокетов

    //процедура вывода информации во внешнее мемо
    procedure Log(message:string);
    function LogServerMess(path,Mess:string; time:boolean):boolean;
    //улучшеная процедура Sleep с проверкой на удаление потока
    procedure Delay(ms:cardinal);
    //процедуры для работы с пакетами
    function Test_disconnect_packet(buf:byte_arr; len:integer):boolean;
    function Test_packet(buf:byte_arr):integer; overload;
    function Test_packet(buf:byte_arr; len:integer):integer; overload;
    //процедуры для работы с сокетами и элементами, хранящими сокет
    function CreateSocketElement(SockID:integer; Time:TDateTime):boolean;
    function RemoveSocketElement(var ptr:PTThreadSocketElement):boolean;
  public
    //TransferTimer:TTransferLaunchTimer;  //таймер для создания внутреннего сервера для трансфера файлов для каждого сокета

    constructor Create(Suspended:boolean; Que:TQueue; Table:TDataTable; Output:TStrings; op:boolean; WorkDelay:cardinal; var Thr_rec:PTStatisticsData; output_path:string);
    destructor Destroy; override;
    procedure Execute; override;

    procedure LogSocketsInfo;
    //процедуры для изменения задержки основного потока
    function GetWorkDelay:cardinal;
    procedure SetWorkDelay(ms:cardinal);
    //процедура для сигнализации о релокации сокетов
    procedure SetRelocation;
  end;


  TStatisticsData = record
    ThreadArr:array of TWorkThread;
    LastModify:int64;
    NeedNewThread:integer;
    CreatingNewThread:boolean;
  end;


implementation

uses SysUtils, Windows, DateUtils, mainUnit, Math, TransferServerTypeUnit;

constructor TWorkThread.Create(Suspended:boolean; Que:TQueue; Table:TDataTable; Output:TStrings; op:boolean; WorkDelay:cardinal; var Thr_rec:PTStatisticsData; output_path:string);
var b:boolean;
i:integer;
begin
  inherited Create(true);
  FOut:=Output;
  FHandle:=Self.Handle;
  FQue:=Que;
  FTable:=Table;
  FOperator:=op;
  First:=nil;
  Last:=nil;
  FWorkDelay:=WorkDelay;
  FAnswerData.Awaiting:=false;
  FOut_path:=output_path;
  if gettickcount64<10000 then FLastDelayCorrection:=0
  else FLastDelayCorrection:=gettickcount64;
  FDelayCorrectionDifference:=0;
  FRelocation:=false;
  //делем масив-буфер для хранения пакетов
  setlength(FBuffer,1024);
  FBuffer_len:=0;

  Log('Creating Thread#'+inttostr(FHandle)+' begins at '+datetimetostr(now));

  Log('Registering thread in the queue, ThreadID='+inttostr(FHandle));
  if FQue<>nil then
  begin
    b:=Que.RegisterThread(FHandle,FOperator);
    if b=true then Log('Register sucsessful')
    else Log('Register failed');
  end
  else
    Log('Queue is nil');

  Log('Registering thread in the table, ThreadID='+inttostr(FHandle));
  if FTable<>nil then
  begin
    b:=Table.RegisterThread(FHandle);
    if b=true then Log('Register sucsessful')
    else Log('Register failed');
  end
  else
    Log('Table is nil');

  //выводим данные потока
  Log('Thread delay='+inttostr(GetWorkDelay));

  //создаём запись в элементе статистики
  i:=length(Thr_rec^.ThreadArr);
  setlength(Thr_rec^.ThreadArr,i+1);
  Thr_rec^.ThreadArr[i]:=Self;
  FStats:=Thr_rec;

  //создаём таймер для создания трансфер серверов
  //Log('Creating TransferServerTimer');
  //TransferTimer:=TTransferLaunchTimer.Create(nil);
  //Log('Creating TransferServerTimer complete');

  //запускаем поток если есть нужный параметр
  if Suspended=false then Self.Resume;
end;

destructor TWorkThread.Destroy;
var p,p1:PTThreadSocketElement;
begin
  Log('Entering destroy section for thread#'+inttostr(FHandle)+'  +++++++++==============+++++++++++++');

  Log('Removing thread from queue');
  FQue.RemoveThread(FHandle);

  Log('Removing thread from table');
  FTable.RemoveThread(FHandle);

  //очищаем массивы
  setlength(FBuffer,0);
  setlength(FPacket_buf,0);

  //уничтожаем таймер
  //TransferTimer.Enabled:=false;
  //TransferTimer.Free;

  //убираем элементы для хранения информации
  p:=First;
  while p<>nil do
  begin
    p1:=p^.Next_Element;

    setlength(p^.buffer,0);
    if p^.FileServer<>nil then
    begin
      p^.FileServer.Terminate;
      p^.FileServer.WaitFor;
      p^.FileServer.Free;
      p^.FileServer:=nil;
    end;
    Dispose(p);
    p:=p1;
  end;
  First:=nil;
  Last:=nil;

  Log('Destroy of thread#'+inttostr(FHandle)+' complete ===========================================');

  inherited Destroy;
end;

procedure TWorkThread.LogSocketsInfo;
var i:integer;
p:PTThreadSocketElement;
//p1:TTransferServer;
str:string;
begin
  Log('-------------');
  Log('Sockets array from thread#'+inttostr(FHandle));
  i:=1;
  p:=First;
  while p<>nil do
  begin
    Log(' #'+inttostr(i)+'  SockID='+inttostr(p^.SocketID)+'   Time='+FormatDateTime('hh:nn:ss.zzz',p^.LastTime)+'   BufLen='+inttostr(length(p^.buffer))+'    IMEI='+inttostr(p^.imei_reg)+'    Echo='+inttostr(p^.EchoInterval){+'    FileTransfer='+inttostr(p^.FileServer)});

    //делаем лог трансфера файлов
    //if p^.FileServer<>0 then p1:=FQue.GetTransferServer(p^.FileServer)
    //else p1:=nil;
    
    //if p1<>nil then
    if p^.FileServer<>nil then
    begin
      str:='     FileTransfer status:';
      case p^.FileServer.GetStatus of
        tssWorking:str:=str+'Working';
        tssError:str:=str+'Error';
        tssTimeout:str:=str+'Timeout';
        tssDone:str:=str+'Done';
        tssDoneConfirmed:str:=str+'Done and confirmed destroy';
      end;
      Log(str);
    end;

    p:=p^.Next_Element;
    inc(i);
  end;

  Log('-------------');
  {Log('Waiting for answer:'+booltostr(FAnswerData.Awaiting,true));
  if FAnswerData.Awaiting then
  begin
    Log('RequestSenderID='+inttostr(FAnswerData.RequestSenderSocket));
    Log('RequestReceiverID='+inttostr(FAnswerData.RequestReceiverSocket));
    Log('RequestTime='+timetostr(FAnswerData.SendTime));
    Log('RequestType='+inttostr(FAnswerData.RequestType));
  end;
  Log('-------------');  }
end;

function TWorkThread.GetWorkDelay:cardinal;
begin
  result:=FWorkDelay;
end;

procedure TWorkThread.SetWorkDelay(ms:cardinal);
begin
  if ms>0 then FWorkDelay:=ms;
end;

procedure TWorkThread.SetRelocation;
begin
  FRelocation:=true;
end;

procedure TWorkThread.Delay(ms:cardinal);
var i:int64;
begin
  i:=gettickcount64+ms;
  while gettickcount64<i do
  begin
    if Terminated=true then exit;
    sleep(1);
  end;
end;

procedure TWorkThread.Execute;
var buf:byte_arr;
sock,i,j:integer;
t:TDateTime;
//Msg:TMsg;
begin
  repeat
    Delay(FWorkDelay);

    //Log('Work thread#'+inttostr(FHandle));

    setlength(buf,0);
    if FQue.Pop(FHandle,buf,sock,t)=true then
    begin
      Log('Retrieved data from queue in thread#'+inttostr(FHandle)+'   Length:'+inttostr(length(buf))+'   From socket#'+inttostr(sock)+'     Time='+FormatDateTime('hh:nn:ss.zzz',t));

      i:=length(buf);
      j:=length(FBuffer);
      //setlength(FBuffer,i);
      if i>j then
      begin
        while i>j do j:=j*2;

        Log('WARNING! Relocating internal packet buffer from length='+inttostr(length(FBuffer))+' to length='+inttostr(j));
        setlength(FBuffer,j);
      end;

      move(buf[0],FBuffer[0],i);
      FBuffer_len:=i;

      //вызываем начальную обработку пакета
      Work(sock,t);
    end;

    //if FTransferServ=nil then CreateTransfer;

    try
      Check_LastActive;
    except
      on e:exception do
      begin
        Log('WARNING! Exception in Check last active procedure with message:'+e.Message);
      end;
    end;

    Check_Relocation;

    {if PeekMessage(Msg, 0, 0, 0, PM_REMOVE) then
    begin
      TranslateMessage(Msg);
      DispatchMessage(Msg);
    end;        }
  until Terminated=true;
end;

function TWorkThread.Test_disconnect_packet(buf:byte_arr; len:integer):boolean;
//var l:integer;
begin
  result:=false;

  //l:=length(buf);

  if len<>7 then exit;

  if (buf[0]=158)
  and(buf[1]=0)
  and(buf[2]=0)
  and(buf[3]=0)
  and(buf[4]=2)
  and(buf[5]=$FF)
  and(buf[6]=$FF) then result:=true;
end;

function TWorkThread.CreateSocketElement(SockID:integer; Time:TDateTime):boolean;
var p:PTThreadSocketElement;
begin
  result:=false;

  //создаём элемент для сокета
  New(p);
  if Last=nil then  //в очереди ничего нет, делаем очередь
  begin
    First:=p;
    Last:=p;
    p^.Next_Element:=nil;
    p^.Prev_Element:=nil;
  end
  else   //в очереди уже что-то есть, вставляем элемент в конец
  begin
    Last^.Next_Element:=p;
    p^.Prev_Element:=Last;
    p^.Next_Element:=nil;
    Last:=p;
  end;
  //заполняем поля
  p^.SocketID:=SockID;
  //p^.LastTime:=time;
  p^.LastTime:=now;
  //if FOperator=true then p^.Registered:=true
  //else
  p^.Registered:=false;
  p^.imei_reg:=0;
  p^.EchoInterval:=30000;
  //p^.FileServer:=0;
  p^.FileServer:=nil;
  p^.FileServerUpdateTime:=p^.LastTime;
  setlength(p^.buffer,0);

  result:=true;
end;

function TWorkThread.RemoveSocketElement(var ptr:PTThreadSocketElement):boolean;
var p_prev,p_next:PTThreadSocketElement;
begin
  result:=false;

  p_prev:=ptr^.Prev_Element;
  p_next:=ptr^.Next_Element;

  if p_prev<>nil then p_prev^.Next_Element:=p_next;
  if p_next<>nil then p_next^.Prev_Element:=p_prev;

  if ptr=First then First:=ptr^.Next_Element;
  if ptr=Last then Last:=ptr^.Prev_Element;

  //очистка
  setlength(ptr^.buffer,0);
  {if ptr^.FileServer<>nil then
  begin
    ptr^.FileServer.Terminate;
    ptr^.FileServer.WaitFor;
    ptr^.FileServer.Free;
    ptr^.FileServer:=nil;
  end;  }
  Dispose(ptr);
  ptr:=nil;

  result:=true;
end;

//возвращаемые значения:
//0 - пакет правильный и полный
//1 - пакет неполный
//-1 - пакет неправильный
function TWorkThread.Test_packet(buf:byte_arr):integer;
var l,i:integer;
begin
  l:=length(buf);

  //длинна пакета не может быть меньше 7 байт
  if l<7 then
  begin
    result:=-1;
    exit;
  end;

  //первый байт должен быть 158
  if buf[0]<>158 then
  begin
    result:=-1;
    exit;
  end;

  //читаем длинну пакета
  i:=(buf[1] shl 24)or(buf[2] shl 16)or(buf[3] shl 8)or(buf[4]);
  //пакет не соответствует длинне, указанной в заголовке
  if (l-5)<i then result:=1
  else if (l-5)>i then result:=2
  else result:=0;
end;

//возвращаемые значения:
//0 - пакет правильный и полный
//1 - пакет неполный
//-1 - пакет неправильный
function TWorkThread.Test_packet(buf:byte_arr; len:integer):integer;
var i:integer;
begin
  //l:=length(buf);

  //длинна пакета не может быть меньше 7 байт
  if len<7 then
  begin
    result:=-1;
    exit;
  end;

  //первый байт должен быть 158
  if buf[0]<>158 then
  begin
    result:=-1;
    exit;
  end;

  //читаем длинну пакета
  i:=(buf[1] shl 24)or(buf[2] shl 16)or(buf[3] shl 8)or(buf[4]);
  //пакет не соответствует длинне, указанной в заголовке
  if (len-5)<i then result:=1
  else if (len-5)>i then result:=2
  else result:=0;
end;

procedure TWorkThread.CreatePacket(packet_type:word; bufer:string);
var i:integer;
begin
  setlength(FPacket_buf,length(bufer)+7);

  FPacket_buf[0]:=158;
  i:=length(bufer)+2;
  FPacket_buf[1]:=(i and $FF000000)shr 24;
  FPacket_buf[2]:=(i and $00FF0000)shr 16;
  FPacket_buf[3]:=(i and $0000FF00)shr 8;
  FPacket_buf[4]:=(i and $000000FF);
  FPacket_buf[5]:=(packet_type shr 8);
  FPacket_buf[6]:=(packet_type and $00FF);

  for i:=1 to length(bufer) do
    FPacket_buf[6+i]:=ord(bufer[i]);
end;

procedure TWorkThread.CreatePacket(packet_type:word; bufer:byte_arr);
begin
  setlength(FPacket_buf,length(bufer));
  move(bufer[0],FPacket_buf[0],length(bufer));
end;

procedure TWorkThread.CreateRelocationPacket(Element:PTThreadSocketElement);
var pimei:^int64;
psock:^integer;
begin
  setlength(FPacket_buf,28);
  FillChar(FPacket_buf[0],28,0);

  if Element=nil then exit;

  FPacket_buf[0]:=158;  //символ начала пакета
  FPacket_buf[4]:=23;  //длинна пакета
  FPacket_buf[5]:=1;  //тип пакета (0100h)
  if Element^.Registered=true then FPacket_buf[15]:=1  //зарегистрирован ли клиент
  else FPacket_buf[15]:=0;

  pimei:=@FPacket_buf[7];    //IMEI
  pimei^:=Element^.imei_reg;
  psock:=@FPacket_buf[16];   //Echo-interval
  psock^:=Element^.EchoInterval;
  psock:=@FPacket_buf[24];  //SockID
  psock^:=Element^.SocketID;
end;

procedure TWorkThread.Check_LastActive;
var p:PTThreadSocketElement;
t:TDateTime;
interval:integer;
str:string;
//p1:TTransferServer;
begin
  t:=now;
  p:=First;
  while p<>nil do
  begin
    if p^.Registered=false then interval:=10
    else interval:=180;

    if IncSecond(p^.LastTime,interval)<t then
    begin
      //долго не было ответа, отсоединяем сокет
      Log('Disconnecting socket due to timeout, SocketID='+inttostr(p^.SocketID));
      //Log('LastTime='+FormatDateTime('hh:nn:ss.zzz',p^.LastTime));
      if FQue.DisconnectSocket(p^.SocketID)=false then
      begin  //если нет такого сокета в очереди, то убираем сами, т.к. пакет отсоединения не прийдёт
        FTable.ClientDisconnected(p^.SocketID,p^.LastTime,p^.imei_reg);
        RemoveSocketElement(p);
        exit;
      end
      else p^.LastTime:=IncSecond(p^.LastTime,5);
    end;

    if p^.FileServer<>nil then
    begin
      if IncSecond(p^.FileServerUpdateTime,5)<t then
      begin
        p^.FileServerUpdateTime:=t;
        //Log('Server check');

        if p^.FileServer.GetStatus=tssWorking then interval:=5
        else interval:=1;

        if IncSecond(p^.FileServer.GetLastUpdateTime,interval)<t then
        begin
          p^.FileServer.SetLastUpdateTime;
          case p^.FileServer.GetStatus of
            tssWorking:    //tssWorking------------------
            begin
              Log('FileTransfer on socket='+inttostr(p^.SocketID)+'  status=Working');

              //формируем пакет поддержания связи
              str:=chr($FD)+chr((p^.SocketID and $FF000000)shr 24)+chr((p^.SocketID and $00FF0000)shr 16)+chr((p^.SocketID and $0000FF00)shr 8)+chr(p^.SocketID and $000000FF);
              CreatePacket($0004,str);
              FQue.SendPacket(p^.SocketID,FPacket_buf);
            end;
            tssError:    //tssError-------------------
            begin
              Log('FileTransfer on socket='+inttostr(p^.SocketID)+'  status=Error');

              //формируем и отправляем пакет ошибки ответа
              str:=chr($FF)+chr((p^.SocketID and $FF000000)shr 24)+chr((p^.SocketID and $00FF0000)shr 16)+chr((p^.SocketID and $0000FF00)shr 8)+chr(p^.SocketID and $000000FF);
              CreatePacket($0004,str);
              FQue.SendPacket(p^.SocketID,FPacket_buf);
              //ставим статус сервера, чтобы он уничтожился
              //p1.ConfirmDestroy;
              p^.FileServer.ConfirmDestroy;
              //очищаем ID сервера
              p^.FileServer:=nil;

              {Log('Begining to destroy FileTransfer on socket='+inttostr(p^.SocketID));

              p^.FileServer.Terminate;
              p^.FileServer.WaitFor;
              p^.FileServer.Free;
              p^.FileServer:=nil;  }
            end;
            tssTimeout:      //tssTimeout-----------------
            begin
              Log('FileTransfer on socket='+inttostr(p^.SocketID)+'  status=Timeout');

              //формируем и отправляем пакет ошибки ответа
              str:=chr($FF)+chr((p^.SocketID and $FF000000)shr 24)+chr((p^.SocketID and $00FF0000)shr 16)+chr((p^.SocketID and $0000FF00)shr 8)+chr(p^.SocketID and $000000FF);
              CreatePacket($0004,str);
              FQue.SendPacket(p^.SocketID,FPacket_buf);
              //ставим статус сервера, чтобы он уничтожился
              //p1.ConfirmDestroy;
              p^.FileServer.ConfirmDestroy;
              //очищаем ID сервера
              p^.FileServer:=nil;

              {Log('Begining to destroy FileTransfer on socket='+inttostr(p^.SocketID));

              p^.FileServer.Terminate;
              p^.FileServer.WaitFor;
              p^.FileServer.Free;
              p^.FileServer:=nil;  }
            end;
            tssDone:      //tssDone--------------------
            begin
              Log('FileTransfer on socket='+inttostr(p^.SocketID)+'  status=Done');
              Log('Answering with good responce');
              //формируем и отправляем пакет окончания ответа
              str:=chr($FE)+chr((p^.SocketID and $FF000000)shr 24)+chr((p^.SocketID and $00FF0000)shr 16)+chr((p^.SocketID and $0000FF00)shr 8)+chr(p^.SocketID and $000000FF);
              CreatePacket($0004,str);
              FQue.SendPacket(p^.SocketID,FPacket_buf);
              //ставим статус сервера, чтобы он уничтожился
              //p1.ConfirmDestroy;
              p^.FileServer.ConfirmDestroy;
              //очищаем ID сервера
              p^.FileServer:=nil;

              {Log('Begining to destroy FileTransfer on socket='+inttostr(p^.SocketID));

              p^.FileServer.Terminate;
              p^.FileServer.WaitFor;
              p^.FileServer.Free;
              p^.FileServer:=nil;  }
            end;
            tssDoneConfirmed:      //tssDoneConfirmed---------------
            begin
              Log('FileTransfer on socket='+inttostr(p^.SocketID)+'  status=DoneConfirmed');
              Log('WARNING! This status should not be displayed');
            end;
          end;  //case p^.FileServer.GetStatus of
        end;  //if IncSecond(p^.FileServer.GetLastUpdateTime,interval)<t then
      end;  //if IncSecond(p^.FileServerUpdateTime,5)<t then
    end;  //if p^.FileServer<>nil then

    {if p^.FileServer<>0 then
    begin
      p1:=FQue.GetTransferServer(p^.FileServer);
      if p1<>nil then
      begin
        if p1.GetStatus=tssWorking then interval:=5
        else interval:=1;

        if IncSecond(p1.GetLastUpdateTime,interval)<t then
        begin
          p1.SetLastUpdateTime;
          case p1.GetStatus of
            tssWorking:    //tssWorking------------------
            begin
              Log('FileTransfer on socket='+inttostr(p^.SocketID)+'  status=Working');

              //формируем пакет поддержания связи
              str:=chr($FD)+chr((p^.SocketID and $FF000000)shr 24)+chr((p^.SocketID and $00FF0000)shr 16)+chr((p^.SocketID and $0000FF00)shr 8)+chr(p^.SocketID and $000000FF);
              CreatePacket($0004,str);
              FQue.SendPacket(p^.SocketID,FPacket_buf);
            end;
            tssError:    //tssError-------------------
            begin
              Log('FileTransfer on socket='+inttostr(p^.SocketID)+'  status=Error');

              //формируем и отправляем пакет ошибки ответа
              str:=chr($FF)+chr((p^.SocketID and $FF000000)shr 24)+chr((p^.SocketID and $00FF0000)shr 16)+chr((p^.SocketID and $0000FF00)shr 8)+chr(p^.SocketID and $000000FF);
              CreatePacket($0004,str);
              FQue.SendPacket(p^.SocketID,FPacket_buf);
              //ставим статус сервера, чтобы он уничтожился
              p1.ConfirmDestroy;
              //очищаем ID сервера
              p^.FileServer:=0;
            end;
            tssTimeout:      //tssTimeout-----------------
            begin
              Log('FileTransfer on socket='+inttostr(p^.SocketID)+'  status=Timeout');

              //формируем и отправляем пакет ошибки ответа
              str:=chr($FF)+chr((p^.SocketID and $FF000000)shr 24)+chr((p^.SocketID and $00FF0000)shr 16)+chr((p^.SocketID and $0000FF00)shr 8)+chr(p^.SocketID and $000000FF);
              CreatePacket($0004,str);
              FQue.SendPacket(p^.SocketID,FPacket_buf);
              //ставим статус сервера, чтобы он уничтожился
              p1.ConfirmDestroy;
              //очищаем ID сервера
              p^.FileServer:=0;
            end;
            tssDone:      //tssDone--------------------
            begin
              Log('FileTransfer on socket='+inttostr(p^.SocketID)+'  status=Done');
              Log('Answering with good responce');
              //формируем и отправляем пакет окончания ответа
              str:=chr($FE)+chr((p^.SocketID and $FF000000)shr 24)+chr((p^.SocketID and $00FF0000)shr 16)+chr((p^.SocketID and $0000FF00)shr 8)+chr(p^.SocketID and $000000FF);
              CreatePacket($0004,str);
              FQue.SendPacket(p^.SocketID,FPacket_buf);
              //ставим статус сервера, чтобы он уничтожился
              p1.ConfirmDestroy;
              //очищаем ID сервера
              p^.FileServer:=0;
            end;
            tssDoneConfirmed:      //tssDoneConfirmed---------------
            begin
              Log('FileTransfer on socket='+inttostr(p^.SocketID)+'  status=DoneConfirmed');
              Log('WARNING! This status should not be displayed');
            end;
          end;    //case p1.GetStatus of
        end;   //if IncSecond(p^.FileServer.GetLastUpdateTime,interval)<t then
      end  //if p1<>nil then
      else Log('Failed to obtain transfer server');
    end;  //if p^.FileServer<>0 then    }

    p:=p^.Next_Element;
  end;
end;

procedure TWorkThread.Check_Relocation;
var i,j,goal:integer;
p,p1:PTThreadSocketElement;
d:double;
begin
  if (FRelocation=true)and(FOperator=false) then    //поток для операторов не участвует в релокации
  begin
    //сичтаем кол-во сокетов
    p:=First;
    i:=0;
    while p<>nil do
    begin
      inc(i);
      p:=p^.Next_Element;
    end;

    //определяем кол-во сокетов, которых надо релоцировать
    j:=length(FStats^.ThreadArr)-2;   //кол-во потоков обработки (не считая поток для операторов и новый поток, который мы только что должны были создать)
    d:=j*100;  //кол-во сокетов во всех потоках, если в каждом потоке было бы 100 сокетов
    d:=d/(j+1);   //кол-во сокетов в каждом потоке после релокации (идеал)
    d:=ceil(100-d);   //кол-во сокетов, на которое уменьшается сокеты в каждом потоке (округленное вверх)
    d:=d/100;   //кол-во сокетов, на которое надо уменьшить каждый поток в виде %
    goal:=floor(i*d);    

    Log('WARNING! Relocation starts in thread#'+inttostr(FHandle)+', need to relocate '+inttostr(goal)+' out of '+inttostr(i));

    FRelocation:=false;
    if goal=0 then exit;  //если слишком мало элементов, то выходим

    //todo: дописать проверку на ожидание ответа

    //делаем цикл по всем сокетам и перераспределяем те, которые подходят по условию
    p:=First;
    i:=0;
    while (p<>nil)and(i<goal) do
    begin
      if (p^.Registered=true) then    //пропускаем незарегистрированные; передача файла не должна иметь значение
      begin
        //убираем сокет из очереди
        FQue.RemoveSocket(p^.SocketID);

        //добавляем сокет как новый сокет
        FQue.RegisterSocket(p^.SocketID,true,false);

        //формируем пакет релокации сокета на другой поток
        CreateRelocationPacket(p);
        FQue.Push(FPacket_buf,p^.SocketID,now,true);

        //убираем элемент связанный с сокетом в текущем потоке
        p1:=p;
        p:=p^.Next_Element;
        RemoveSocketElement(p1);
        inc(i);

        continue;
      end;

      p:=p^.Next_Element;
    end;   //while (p<>nil)and(i<goal) do
  end;   //if FRelocation=true then
end;

//начальная обработка
//---------
//помимо параметров передается буфер, содержащий пакет во внутренней переменной FBuffer
procedure TWorkThread.Work(SockID:integer; Time:TDateTime);
var p:PTThreadSocketElement;
b:boolean;
i:integer;
str:string;
begin
  Log('Work enter before element check');

  //ищем элемент, которому соответствует пакет
  p:=First;
  while p<>nil do
  begin
    if p^.SocketID=SockID then break;
    p:=p^.Next_Element;
  end;

  //разделение на то, ждёт ли поток ответа или нет
  if FAnswerData.Awaiting=true then
  begin   //поток ждёт ответа
    Log('Work enter when waiting for answer');

    //todo: дописать

    //если элемента нет, то игнорим
    if p=nil then exit;
  end
  else
  begin  //поток не ждёт ответа
    Log('Work enter when not waiting for answer');

    //проверяем на пакет отсоединения
    b:=Test_disconnect_packet(FBuffer,FBuffer_len);
    //и если нашли такой сокет в списке советов
    if (b=true)and(p<>nil) then
    begin  //то убираем его
      Log('Received disconnect packet from socket='+inttostr(SockID));

      FTable.ClientDisconnected(SockID,Time,p^.imei_reg);
      FQue.RemoveSocket(SockID);
      RemoveSocketElement(p);
      exit;
    end
    else if (b=true)and(p=nil) then exit;  //если пакет дисконекта, но элемента нет, то игнорим (хотя маловероятно)

    if p=nil then
    begin   //если соответствия элемента нет, то надо проверить на пакет инициализации
      if Test_packet(FBuffer,FBuffer_len)<>0 then exit;   //если пакет неправильный - игнорим

      //читаем тип пакета, он должен быть пакет инициализации
      i:=(FBuffer[5] shl 8)or(FBuffer[6]);
      if i<>0 then exit;   //если пришёл какойто другой пакет, игнорируем его

      //тут мы знаем, что пакет нормальный и это пакет соединения
      Log('Reseived register packet for socket='+inttostr(SockID));
      CreateSocketElement(SockID,Time);
      str:=FQue.GetSocketIP(SockID);
      FTable.AddMessageLog(0,0,0,Time,'Connected new client with IP='+str+';   SockID='+inttostr(SockID));
      Log('Created socket element for socket='+inttostr(SockID));
    end
    else
    begin   //если соответствие элемента есть, то идём на обработку
      //мы знаем, что это пакет не дисконекта, так что сразу переписываем буфер и идём на обработку
      //i:=length(FBuffer);
      i:=FBuffer_len;
      setlength(p^.buffer,i);
      move(FBuffer[0],p^.buffer[0],i);
      //setlength(FBuffer,0);
      FBuffer_len:=0;
      try
        Work_packet(p,SockID,Time);
      except
        on e:exception do
        begin
          Log('WARNING! Exception in Work packet procedure with message:'+e.Message);
        end;
      end;

      //подчищаем на всякий
      if p<>nil then setlength(p^.buffer,0);
    end;
  end;
end;


//обработка конктерного пакета
//----------------
function TWorkThread.Work_packet(var tp:PTThreadSocketElement; SockID:integer; Time:TDateTime):integer;
var i,j,k,z,pack_type:integer;
imei,diff:int64;
pimei:^int64;
pint:^integer;
pdouble:^double;
ip,ver,str:string;
TempTime:TDateTime;
buf:byte_arr;
b,b1:boolean;
TransferData:TTransferServerInstructions;
//p1:TTransferServer;
begin
  result:=-1;

  Log('Entered work packet for socket='+inttostr(SockID));

  //проверяем правильность пакета
  i:=Test_packet(tp^.buffer);

  if i=2 then  //скорее всего скрепленые пакеты
  begin
    Log('Entered sticked packet section, separating packets');
    //открепляем один и записываем всё остальное обратно в очередь
    //читаем длинну пакета
    j:=(tp^.buffer[1] shl 24)or(tp^.buffer[2] shl 16)or(tp^.buffer[3] shl 8)or(tp^.buffer[4]);
    k:=length(tp^.buffer);
    i:=k-j-5;
    setlength(buf,i);
    move(tp^.buffer[j+5],buf[0],i);
    setlength(tp^.buffer,j+5);

    FQue.Push(buf,SockID,Time,true);
    setlength(buf,0);

    i:=Test_packet(tp^.buffer);
  end;

  if i<>0 then
  begin  //если пакет неправильный
    setlength(tp^.buffer,0);  //очищаем буфер
    //если сокет ещё не зарегистрирован, то отсоединяем, чтобы избежать спама
    if tp^.Registered=false then
    begin
      Log('Received wrong packet while not registered, disconnecting');
      if FQue.DisconnectSocket(SockID)=false then
      begin
        FTable.ClientDisconnected(SockID,now,tp^.imei_reg);
        RemoveSocketElement(tp);
      end;
    end
    else
      Log('Received wrong packet structure');

    exit;  //и выходим
  end;

  //читаем тип пакета
  pack_type:=(tp^.buffer[5] shl 8)or(tp^.buffer[6]);

  //если сокет ждёт регистрации, то принимать только пакеты регистрации
  if tp^.Registered=false then
  begin
    Log('Enter register section for socket#'+inttostr(SockID));
    if (pack_type=$FF)and((length(tp^.buffer)=27)or(length(tp^.buffer)=23)) then   //если это пакет регистрации (ещё проверяем на длинну пакета, проверяем на старый и новый пакеты)
    begin
      Log('Receive register packet from socket #'+inttostr(sockID));

      //выделяем imei
      //imei:=(tp^.buffer[7] shl 56)+(tp^.buffer[8] shl 48)+(tp^.buffer[9] shl 40)+(tp^.buffer[10] shl 32)+(tp^.buffer[11] shl 24)+(tp^.buffer[12] shl 16)+(tp^.buffer[13] shl 8)+(tp^.buffer[14]);
      pimei:=@tp^.buffer[7];
      imei:=pimei^;
      //imei:=12345678901233;
      //узнаем IP
      ip:=FQue.GetSocketIP(SockID);
      //выделяем версию
      ver:=inttostr(tp^.buffer[15])+'.'+inttostr(tp^.buffer[16])+'.'+inttostr(tp^.buffer[17])+'.'+inttostr(tp^.buffer[18]);
      //выделяем TeamViewerID (если это новый пакет
      if length(tp^.buffer)=27 then
        j:=(tp^.buffer[23] shl 24)or(tp^.buffer[24] shl 16)or(tp^.buffer[25] shl 8)or(tp^.buffer[26])
      else
        j:=0;

      i:=FTable.RegisterNewClient(imei,ip,ver,SockID,j);

      //если всё норм или такой имей уже подсоединён (плохая связь, дисконекта ещё не было, но идёт уже реконнект со стороны табло), то
      if (i=0)or(i=-2) then
      begin
        Log('Client registered sucsessfuly, SocketID='+inttostr(SockID));
        //записываем в список IMEI
        tp^.imei_reg:=imei;
        //убираем статическую связь сокет-поток из очереди
        FQue.RemoveStaticSocket(SockID,FHandle);
        tp^.Registered:=true;
        //tp^.LastTime:=time;
        tp^.LastTime:=now;
        //выделяем эхо-интервал
        tp^.EchoInterval:=(tp^.buffer[19] shl 24)or(tp^.buffer[20] shl 16)or(tp^.buffer[21] shl 8)or(tp^.buffer[22]);

        //посылаем пакет $0005 с ID сокета (подтверждение регистрации)
        str:=chr((SockID and $FF000000)shr 24)+chr((SockID and $00FF0000)shr 16)+chr((SockID and $0000FF00)shr 8)+chr(SockID and $000000FF);
        //создаем пакет
        CreatePacket($0005,str);
        //отправляем пакет
        FQue.SendPacket(SockID,FPacket_buf);
      end
      else if i>0 then   //если это оператор
      begin
        //проверяем, является ли данный поток потоком для операторов
        if FOperator=true then
        begin      //поидее мы больше никогда сюда не придём
          Log('Entered registration section for OPERATOR in operator thread');

          Log('WARNING, this section should not be run, error');
          exit;

          {//записываем в список IMEI
          tp^.imei_reg:=imei;
          //убираем статическую связь сокет-поток из очереди
          FQue.RemoveStaticSocket(SockID,FHandle);
          tp^.Registered:=true;
          //tp^.LastTime:=time;
          tp^.LastTime:=now;

          //посылаем пакет $0005 с ID сокета (подтверждение регистрации)
          str:=chr((SockID and $FF000000)shr 24)+chr((SockID and $00FF0000)shr 16)+chr((SockID and $0000FF00)shr 8)+chr(SockID and $000000FF);
          //создаем пакет
          CreatePacket($0005,str);
          //отправляем пакет
          FQue.SendPacket(SockID,FPacket_buf);    }
        end
        else
        begin
          Log('Entered registration section for OPERATOR in non-operator thread');

          //тут мы знаем, что это оператор правильный, значит выставляем флаг регистрации
          tp^.Registered:=true;
          tp^.imei_reg:=imei;

          //убираем сокет из очереди, т.к. изначально это будет поток для обычных клиентов
          FQue.RemoveSocket(SockID);

          //добавляем сокет к потоку операторов
          FQue.RegisterOPSocket(SockID);

          //формируем пакет релокации сокета на другой поток
          CreateRelocationPacket(tp);
          FQue.Push(FPacket_buf,SockID,now,true);

          //убираем элемент связанный с сокетом в текущем потоке
          RemoveSocketElement(tp);

          Log('Reallocating operator socket complete');

          //exit;
        end;
      end
      else   //если нет, то отсоединяем клиента
      begin
        Log('Disconnecting socket due to incorrect registration data, SocketID='+inttostr(SockID));
        if FQue.DisconnectSocket(SockID)=false then
        begin
          FTable.ClientDisconnected(SockID,now,tp^.imei_reg);
          RemoveSocketElement(tp);
        end;
      end;
    end
    else if pack_type=$0100 then   //если это пакет релокации
    begin
      Log('Receive relocation packet for socket #'+inttostr(sockID));

      //проверяем на правильность длинны пакета
      i:=length(tp^.buffer);
      if i<>28 then
      begin
        Log('Wrong packet length in relocation packet');
        exit;
      end;

      //выделяем imei
      pimei:=@tp^.buffer[7];
      imei:=pimei^;
      //выделяем SocketID
      pint:=@tp^.buffer[24];
      i:=pint^;
      //выделяем регистрацию
      j:=tp^.buffer[15];
      //выделяем время эхо-интервала
      pint:=@tp^.buffer[16];
      tp^.EchoInterval:=pint^;

      //проверяем соответствие сокета
      if i<>SockID then
      begin
        Log('SocketID is not the same in relocation packet');
        exit;
      end;

      //переносим значения
      tp^.imei_reg:=imei;
      if j=0 then tp^.Registered:=false else tp^.Registered:=true;
      tp^.LastTime:=now;

      //проверяем поток на поток операторов (см чуть ниже)
      if FOperator=true then
      begin  //если в потоке оператора мы получили пакет релокации, это значит что этот оператор новый и ему надо ответить пакетом успешной регистрации
        //посылаем пакет $0005 с ID сокета (подтверждение регистрации)
        str:=chr((SockID and $FF000000)shr 24)+chr((SockID and $00FF0000)shr 16)+chr((SockID and $0000FF00)shr 8)+chr(SockID and $000000FF);
        //создаем пакет
        CreatePacket($0005,str);
        //отправляем пакет
        FQue.SendPacket(SockID,FPacket_buf);
      end;
    end
    else
    begin
      Log('Received non-register packet, while waiting for registration, disconnecting');
      //если это не пакет регистрации, то дисконектаем
      if FQue.DisconnectSocket(SockID)=false then
      begin
        FTable.ClientDisconnected(SockID,now,tp^.imei_reg);
        RemoveSocketElement(tp);
      end;
    end;
  end
  else
  begin  //здесь обычная обработка пакета
    Log('Entered normal work for socket='+inttostr(SockID)+' with packet type='+inttostr(pack_type));

    //обрабатываем пакет
    //***********************************
    //начало данных - 7
    //***********************************
    case pack_type of
      $0001:begin  //эхо-пакет
              //проверяем на правильность эхо-пакета
              //проверить надо только длинну пакета, т.к. данных нет, а всё остальное проверила процедура проверки пакета перед этим
              if length(tp^.buffer)<>7 then
                Log('Received wrong echo packet (0001h) from socket#'+inttostr(SockID))
              else
              begin
                Log('Received echo packet (0001h) from socket#'+inttostr(SockID));
                tp^.LastTime:=now;

                //проверяем время обработки
                diff:=Millisecondsbetween(time,now);
                if (FLastDelayCorrection+10000)<gettickcount64 then
                begin
                  //смотрим время
                  if diff>(500) then      //проверяем, не превысили ли мы время обработки в 500 мс
                  begin
                    //смотрим, падает ли разница
                    if diff>FDelayCorrectionDifference then
                    begin  //разница не падает, уменьшаем ещё
                      i:=FWorkDelay-3;
                      if i<=0 then
                      begin  //если некуда уже уменьшать, то сигнализируем о создании дополнительного потока обработки
                        Log('WARNING! Need to create a new thread (in thread#'+inttostr(FHandle)+')!!! Difference='+inttostr(diff));

                        FStats^.NeedNewThread:=1;
                      end
                      else
                      begin
                        Log('WARNING! Correcting work delay in thread#'+inttostr(FHandle)+' from '+inttostr(FWorkDelay)+' to '+inttostr(i));
                        Log('Difference='+inttostr(diff));

                        SetWorkDelay(i);
                      end;
                    end   //if diff<FDelayCorrectionDifference then
                    else
                    begin
                      Log('WARNING! In thread#'+inttostr(FHandle)+' difference='+inttostr(diff)+' but it is dropping, waiting');
                    end;

                    //ждём ещё

                    FLastDelayCorrection:=gettickcount64;
                    FDelayCorrectionDifference:=diff;
                  end;   //if diff>(500) then
                end;   //if (FLastDelayCorrection+10000)<gettickcount64 then
              end;  //if length(tp^.buffer)<>7 then else
            end;  //case $0001:begin
      $0002:begin  //пакет сообщения
              //проверяем минимальную длинну сообщения
              if length(tp^.buffer)<19 then
              begin   //если пакет слишком маленький
                Log('Reseived too small message packet');

                //формируем пакет ошибки ответа
                str:=chr($FF)+chr((SockID and $FF000000)shr 24)+chr((SockID and $00FF0000)shr 16)+chr((SockID and $0000FF00)shr 8)+chr(SockID and $000000FF)+str;
                CreatePacket($0004,str);
                FQue.SendPacket(SockID,FPacket_buf);

                exit;
              end
              else
              begin
                //проверяем длинну строки
                i:=(tp^.Buffer[17] shl 8)or(tp^.Buffer[18]);
                if length(tp^.buffer)<>(i+19) then
                begin   //если длинна строки неправильная
                  Log('Reseived message packet with wrong string length');

                  //формируем пакет ошибки ответа
                  str:=chr($FF)+chr((SockID and $FF000000)shr 24)+chr((SockID and $00FF0000)shr 16)+chr((SockID and $0000FF00)shr 8)+chr(SockID and $000000FF)+str;
                  CreatePacket($0004,str);
                  FQue.SendPacket(SockID,FPacket_buf);

                  exit;
                end
                else
                begin  //всё правильно, записываем сообщение в БД
                  //читаем время сообщения
                  pdouble:=@tp^.buffer[9];
                  TempTime:=pdouble^;
                  //читаем строку
                  str:='';
                  if i<>0 then
                  begin
                    k:=19;
                    while k<length(tp^.buffer) do
                    begin
                      str:=str+chr(tp^.buffer[k]);
                      inc(k);
                    end;
                  end;
                  //читаем тип сообщения
                  i:=tp^.buffer[7];
                  //читаем уровень сообщения
                  j:=tp^.buffer[8];

                  Log('Reseived message:'+str+'; MessType='+inttostr(i)+'; MessLevel='+inttostr(j)+'; Time='+datetimetostr(TempTime));
                  FTable.CreateNewMessageLog(tp^.imei_reg,i,j,TempTime,str);
                end;
              end;
            end;
      $0003:begin  //запрос
              Log('Reseived request packet of type='+inttostr(tp^.buffer[7]));
              //сокет ответа
              i:=(tp^.Buffer[8] shl 24)or(tp^.Buffer[9] shl 16)or(tp^.Buffer[10] shl 8)or(tp^.Buffer[11]);
              //сокет получения запроса
              j:=(tp^.Buffer[12] shl 24)or(tp^.Buffer[13] shl 16)or(tp^.Buffer[14] shl 8)or(tp^.Buffer[15]);

              //проверяем, является ли это поток операторов, т.к. запрос могут посылать только операторы
              if FOperator=false then
              begin
                Log('Reseived request from non-operator socket');

                //формируем пакет ошибки ответа
                str:=chr($FF)+chr((SockID and $FF000000)shr 24)+chr((SockID and $00FF0000)shr 16)+chr((SockID and $0000FF00)shr 8)+chr(SockID and $000000FF)+str;
                CreatePacket($0004,str);
                FQue.SendPacket(SockID,FPacket_buf);

                exit;
              end;

              //проверяем, соответствует ли ID сокета в пакете реальному ID, от которого мы получили запрос
              if i<>SockID then
              begin
                Log('Request socketID is different in Socket('+inttostr(SockID)+') and Packet('+inttostr(i)+')');

                //отвечаем неправильным пакетом
                str:=chr($FF)+chr((SockID and $FF000000)shr 24)+chr((SockID and $00FF0000)shr 16)+chr((SockID and $0000FF00)shr 8)+chr(SockID and $000000FF)+str;
                CreatePacket($0004,str);
                FQue.SendPacket(SockID,FPacket_buf);

                exit;
              end;

              //разделение по типам запросов
              case tp^.buffer[7] of
                1:begin  //запрос списка табло
                    //запрашиваем формирование строки для отправки у объекта таблицы
                    FTable.RequestTabloList(str,SockID);

                    //формируем данные пакета
                    str:=chr($01)+chr((SockID and $FF000000)shr 24)+chr((SockID and $00FF0000)shr 16)+chr((SockID and $0000FF00)shr 8)+chr(SockID and $000000FF)+str;

                    //создаем пакет
                    CreatePacket($0004,str);

                    //отправляем пакет
                    FQue.SendPacket(SockID,FPacket_buf);

                    Log('Answer for request (list of dysplay) is send');
                  end;
                else
                begin
                  Log('Unknow request type='+inttostr(tp^.buffer[7]));
                end;
              end;
            end;
      $0006:begin  //запрос синхронизации времени
              //проверяем правильность пакета
              if length(tp^.buffer)=7 then
              begin
                TempTime:=now+(1.15738657768816E-7)*2;  //поправка на формирование и отправку пакета + расшифровку данных на стороне клиента
                Log('Received time synchronization packet from client IMEI='+inttostr(tp^.imei_reg));

                //формируем данные пакета ответа
                setlength(buf,15);
                FillChar(buf[0],15,0);
                buf[0]:=158;  //начальный байт
                buf[4]:=10;  //длинна данных
                buf[6]:=6;  //тип пакета
                pdouble:=@buf[7];
                pdouble^:=TempTime;

                //отправляем пакет
                FQue.SendPacket(SockID,buf);

                Log('Answer for synchronization packet is send with value='+FormatDateTime('dd-mm-yyyy hh:nn:ss.zzz',TempTime));
              end;
            end;
      $0007:begin  //пакет инициализации бекапа
              //проверяем правильность пакета
              if length(tp^.buffer)>=25 then
              begin
                //читаем длинну названия файла
                z:=tp^.Buffer[24];
                if length(tp^.buffer)=(25+z) then
                begin
                  if z=0 then str:='backup_'+inttostr(tp^.imei_reg)+'_'+inttostr(gettickcount64)+'.log'
                  else
                  begin
                    str:='';
                    for z:=1 to tp^.buffer[24] do
                      str:=str+chr(tp^.buffer[24+z]);
                  end;

                  //if tp^.FileServer=0 then
                  if tp^.FileServer=nil then
                  begin
                    //получаем номер порта, на котором нужно открыть сервер
                    i:=FQue.GetNewServerPort;
                    //читаем таймаут, который прислал клиент
                    j:=(tp^.Buffer[20] shl 24)or(tp^.Buffer[21] shl 16)or(tp^.Buffer[22] shl 8)or(tp^.Buffer[23]);
                    //читаем размер файла, который собирается прислать нам клиент (распакованный)
                    k:=(tp^.Buffer[7] shl 24)or(tp^.Buffer[8] shl 16)or(tp^.Buffer[9] shl 8)or(tp^.Buffer[10]);
                    //читаем размер файла, который собирается прислать нам клиент (запакованный)
                    z:=(tp^.Buffer[11] shl 24)or(tp^.Buffer[12] shl 16)or(tp^.Buffer[13] shl 8)or(tp^.Buffer[14]);

                    Log('Received backup packet, creating server on port '+inttostr(i)+',  Timeout='+inttostr(j)+',  UnpackedFileSize='+inttostr(k)+', PackedFileSize='+inttostr(z)+',  FileName='+str);

                    Log('Launching CreateTransferServer');
                    TransferData.TransferType:=tstIncoming;
                    TransferData.TransferAction:=tsaReceiveUploadToFTP;
                    TransferData.FirstIP:=FQue.GetSocketIP(SockID);
                    TransferData.SecondIP:='';
                    TransferData.ReadWriteTimeout:=j;
                    TransferData.ServerPort:=i;
                    TransferData.CommandsStr:=str;
                    TransferData.TempStr:='';
                    TransferData.TransferCount:=k;
                    TransferData.ZTransferCount:=z;
                    if tp^.Buffer[15]=0 then
                      TransferData.UseZLib:=false
                    else
                      TransferData.UseZLib:=true;
                    TransferData.TransferCRC32:=(tp^.Buffer[16] shl 24)or(tp^.Buffer[17] shl 16)or(tp^.Buffer[18] shl 8)or(tp^.Buffer[19]);
                    TransferData.FTPSettings.Host:=FTPHost;
                    TransferData.FTPSettings.Port:=FTPPort;
                    TransferData.FTPSettings.User:=FTPUser;
                    TransferData.FTPSettings.Pass:=FTPPass;
                    TransferData.FTPSettings.Timeout:=TransferData.ReadWriteTimeout;

                    tp^.FileServer:=TTransferServer.Create(TransferData,ExtractFilePath(FOut_path)+'..\TransferLogs\',tp^.SocketID,i);
                    FQue.AddTransferElement(tp^.FileServer);

                    Log('Launch complete, waiting');
                    sleep(100);
                    Log('Wait complete');

                    //проверяем, создали ли мы сервер
                    b:=true;
                    if tp^.FileServer<>nil then
                    begin
                      if tp^.FileServer.GetStatus<>tssWorking then b:=false;
                    end
                    else b:=false;

                    if b=true then
                    begin
                      Log('Transfer server created sucsessfuly');

                      Log('Sending responce with port='+inttostr(i));

                      //формируем данные пакета
                      str:=chr((i and $FF000000)shr 24)+chr((i and $00FF0000)shr 16)+chr((i and $0000FF00)shr 8)+chr(i and $000000FF);
                      //создаем пакет
                      CreatePacket($0007,str);
                      //отправляем пакет
                      FQue.SendPacket(SockID,FPacket_buf);
                    end
                    else
                    begin
                      Log('Transfer server create failed, sending error responce');

                      //формируем данные пакета (ошибка ответа)
                      str:=chr($FF)+chr((SockID and $FF000000)shr 24)+chr((SockID and $00FF0000)shr 16)+chr((SockID and $0000FF00)shr 8)+chr(SockID and $000000FF);
                      //создаем пакет
                      CreatePacket($0004,str);
                      //отправляем пакет
                      FQue.SendPacket(SockID,FPacket_buf);
                    end;   //if b=true then else
                  end   //if tp^.FileServer=0 then
                  else Log('Received backup packet, but FileTransfer is already running, ignoring');
                end   //if length(tp^.buffer)=(25+z) then
                else
                begin
                  Log('Received backup packet, but FileName string field is wrong, ignoring');
                end;  
              end  //if length(tp^.buffer)>=16 then
              else
                Log('Wrong backup packet, ignoring');
            end;
      $0009:begin  //пакет инициализации обновления
              //проверяем правильность пакета
              if length(tp^.buffer)=15 then
              begin
                //читаем версию в виде единого числа
                j:=(tp^.Buffer[7] shl 24)or(tp^.Buffer[8] shl 16)or(tp^.Buffer[9] shl 8)or(tp^.Buffer[10]);
                //читаем контрольную сумму
                k:=(tp^.Buffer[11] shl 24)or(tp^.Buffer[12] shl 16)or(tp^.Buffer[13] shl 8)or(tp^.Buffer[14]);
                //формируем читаемую версию
                str:=inttostr((j shr 24)and $FF)+'.'+inttostr((j shr 16)and $FF)+'.'+inttostr((j shr 8)and $FF)+'.'+inttostr(j and $FF);

                Log('Received update packet with version='+str+'   CRC32='+inttohex(k,8));
                Log('Current client version='+UpdateVersion+'  CRC32='+inttohex(UpdateCRC,8));

                //получаем номер порта, на котором нужно открыть сервер
                i:=FQue.GetNewServerPort;

                b1:=false;

                //сравниваем версию и контрольную сумму
                if (str<>UpdateVersion)or(k<>UpdateCRC) then
                begin
                  Log('Version or CRC is different, need to update a client');

                  if tp^.FileServer=nil then
                  begin
                    Log('Creating update server on port '+inttostr(i)+',  Timeout=15000'+',  FileSize='+inttostr(UpdateFileSize)+', FileVersion='+UpdateVersion+',  FileName='+UpdatePath);

                    Log('Launching CreateTransferServer');
                    TransferData.TransferType:=tstOutgoing;
                    TransferData.TransferAction:=tsaUploadFileFromDisk;
                    TransferData.FirstIP:=FQue.GetSocketIP(SockID);
                    TransferData.SecondIP:='';
                    TransferData.ReadWriteTimeout:=15000;
                    TransferData.ServerPort:=i;
                    TransferData.CommandsStr:=UpdatePath;
                    TransferData.TempStr:=inttostr(tp^.imei_reg);
                    TransferData.TransferCount:=UpdateFileSize;
                    TransferData.ZTransferCount:=0;
                    TransferData.UseZLib:=false;
                    TransferData.TransferCRC32:=UpdateCRC;
                    TransferData.FTPSettings.Host:='';
                    TransferData.FTPSettings.Port:=21;
                    TransferData.FTPSettings.User:='';
                    TransferData.FTPSettings.Pass:='';
                    TransferData.FTPSettings.Timeout:=15000;

                    tp^.FileServer:=TTransferServer.Create(TransferData,ExtractFilePath(FOut_path)+'..\TransferLogs\',tp^.SocketID,i);
                    FQue.AddTransferElement(tp^.FileServer);

                    Log('Launch complete, waiting');
                    sleep(100);
                    Log('Wait complete');

                    //проверяем, создали ли мы сервер
                    b:=true;
                    if tp^.FileServer<>nil then
                    begin
                      if tp^.FileServer.GetStatus<>tssWorking then b:=false;
                    end
                    else b:=false;

                    if b=true then
                    begin
                      Log('Transfer server created sucsessfuly');

                      Log('Sending update responce with port='+inttostr(i));

                      b1:=true;
                    end
                    else
                    begin
                      Log('Transfer server create failed, answering with "no need to update" responce');
                    end;   //if b=true then else
                  end   //if tp^.FileServer=0 then
                  else
                    Log('Received backup packet, but FileTransfer is already running, answering with "no need to update" responce');
                end  //if (str<>UpdateVersion)or(k<>UpdateCRC) then
                else
                  Log('Version and CRC are same, don''t need to update');

                //формируем пакет для отправки ответа
                //порт
                str:=chr((i and $FF000000)shr 24)+chr((i and $00FF0000)shr 16)+chr((i and $0000FF00)shr 8)+chr(i and $000000FF)+chr(0);
                //версия
                str:=str+chr(UpdateVersionMajor)+chr(UpdateVersionMinor)+chr(UpdateVersionRelease)+chr(UpdateVersionBuild);
                //размер файла
                z:=UpdateFileSize;
                str:=str+chr((z and $FF000000)shr 24)+chr((z and $00FF0000)shr 16)+chr((z and $0000FF00)shr 8)+chr(z and $000000FF);
                //контрольная сумма файла
                z:=UpdateCRC;
                str:=str+chr((z and $FF000000)shr 24)+chr((z and $00FF0000)shr 16)+chr((z and $0000FF00)shr 8)+chr(z and $000000FF);
                //таймаут
                z:=15000;
                str:=str+chr((z and $FF000000)shr 24)+chr((z and $00FF0000)shr 16)+chr((z and $0000FF00)shr 8)+chr(z and $000000FF);

                if b1=false then
                begin  //нужно посылать пакет ответа с флагом "не нужно обновление"
                  //создаем пакет
                  CreatePacket($0009,str);
                  //отправляем пакет
                  FQue.SendPacket(SockID,FPacket_buf);
                end
                else
                begin  //нужно посылать пакет ответа с флагом "нужно обновление"
                  //изменяем флаг
                  str[5]:=chr(1);
                  //создаем пакет
                  CreatePacket($0009,str);
                  //отправляем пакет
                  FQue.SendPacket(SockID,FPacket_buf);
                end;
              end  //if length(tp^.buffer)=15 then
              else
                Log('Wrong update packet, ignoring');
            end;
      $000A:begin  //пакет идентификации TeamViewerID

            end;
      $000B:begin  //пакет инициализации обновления настроек
              //проверяем правильность пакета
              if length(tp^.buffer)=11 then
              begin
                //читаем контрольную сумму
                k:=(tp^.Buffer[7] shl 24)or(tp^.Buffer[8] shl 16)or(tp^.Buffer[9] shl 8)or(tp^.Buffer[10]);

                Log('Received settings update packet with CRC32='+inttohex(k,8));
                Log('Current client settings CRC32='+inttohex(UpdateSettingsCRC,8));

                //получаем номер порта, на котором нужно открыть сервер
                i:=FQue.GetNewServerPort;

                b1:=false;

                //сравниваем версию и контрольную сумму
                if (k<>UpdateSettingsCRC) then
                begin
                  Log('CRC is different, need to update a client');

                  if tp^.FileServer=nil then
                  begin
                    Log('Creating update server on port '+inttostr(i)+',  Timeout=15000'+',  FileSize='+inttostr(UpdateSettingsFileSize)+',  FileName='+UpdateSettingsPath);

                    Log('Launching CreateTransferServer');
                    TransferData.TransferType:=tstOutgoing;
                    TransferData.TransferAction:=tsaUploadFileFromDisk;
                    TransferData.FirstIP:=FQue.GetSocketIP(SockID);
                    TransferData.SecondIP:='';
                    TransferData.ReadWriteTimeout:=15000;
                    TransferData.ServerPort:=i;
                    TransferData.CommandsStr:=UpdateSettingsPath;
                    TransferData.TempStr:=inttostr(tp^.imei_reg);
                    TransferData.TransferCount:=UpdateSettingsFileSize;
                    TransferData.ZTransferCount:=0;
                    TransferData.UseZLib:=false;
                    TransferData.TransferCRC32:=UpdateSettingsCRC;
                    TransferData.FTPSettings.Host:='';
                    TransferData.FTPSettings.Port:=21;
                    TransferData.FTPSettings.User:='';
                    TransferData.FTPSettings.Pass:='';
                    TransferData.FTPSettings.Timeout:=15000;

                    tp^.FileServer:=TTransferServer.Create(TransferData,ExtractFilePath(FOut_path)+'..\TransferLogs\',tp^.SocketID,i);
                    FQue.AddTransferElement(tp^.FileServer);

                    Log('Launch complete, waiting');
                    sleep(100);
                    Log('Wait complete');

                    //проверяем, создали ли мы сервер
                    b:=true;
                    if tp^.FileServer<>nil then
                    begin
                      if tp^.FileServer.GetStatus<>tssWorking then b:=false;
                    end
                    else b:=false;

                    if b=true then
                    begin
                      Log('Transfer server created sucsessfuly');

                      Log('Sending update responce with port='+inttostr(i));

                      b1:=true;
                    end
                    else
                    begin
                      Log('Transfer server create failed, answering with "no need to update" responce');
                    end;   //if b=true then else
                  end   //if tp^.FileServer=0 then
                  else
                    Log('Received backup packet, but FileTransfer is already running, answering with "no need to update" responce');
                end  //if (str<>UpdateVersion)or(k<>UpdateCRC) then
                else
                  Log('CRC are same, don''t need to update');

                //формируем пакет для отправки ответа
                //порт
                str:=chr((i and $FF000000)shr 24)+chr((i and $00FF0000)shr 16)+chr((i and $0000FF00)shr 8)+chr(i and $000000FF)+chr(0);
                //размер файла
                z:=UpdateSettingsFileSize;
                str:=str+chr((z and $FF000000)shr 24)+chr((z and $00FF0000)shr 16)+chr((z and $0000FF00)shr 8)+chr(z and $000000FF);
                //контрольная сумма файла
                z:=UpdateSettingsCRC;
                str:=str+chr((z and $FF000000)shr 24)+chr((z and $00FF0000)shr 16)+chr((z and $0000FF00)shr 8)+chr(z and $000000FF);
                //таймаут
                z:=15000;
                str:=str+chr((z and $FF000000)shr 24)+chr((z and $00FF0000)shr 16)+chr((z and $0000FF00)shr 8)+chr(z and $000000FF);

                if b1=false then
                begin  //нужно посылать пакет ответа с флагом "не нужно обновление"
                  //создаем пакет
                  CreatePacket($000B,str);
                  //отправляем пакет
                  FQue.SendPacket(SockID,FPacket_buf);
                end
                else
                begin  //нужно посылать пакет ответа с флагом "нужно обновление"
                  //изменяем флаг
                  str[5]:=chr(1);
                  //создаем пакет
                  CreatePacket($000B,str);
                  //отправляем пакет
                  FQue.SendPacket(SockID,FPacket_buf);
                end;

              end  //if length(tp^.buffer)=11 then
              else
                Log('Wrong update settings packet, ignoring');
            end;
      else
      begin
        Log('Received unknown packet type:'+inttostr(pack_type)+' from socket#'+inttostr(SockID));
      end;
    end;
  end;
end;

procedure TWorkThread.Log(message:string);
begin
  //пишем в файл логов
  LogServerMess(FOut_path,message,true);
  //пишем в мемо
  if FOut<>nil then FOut.Add(FormatDateTime('hh:nn:ss.zzz',now)+'  '+message);
end;

function TWorkThread.LogServerMess(path,Mess:string; time:boolean):boolean;
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
