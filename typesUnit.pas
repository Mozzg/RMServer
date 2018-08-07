unit typesUnit;

interface

uses TransferServerTypeUnit;

type
  Byte_arr = array of byte;

  PTQueueElement = ^TQueueElement;
  TQueueElement = record
    SocketID:integer;
    data:Byte_arr;
    ReceiveTime:TDateTime;
    NextElement:PTQueueElement;
  end;

  TRegisterThreadElement = record
    ThreadID:cardinal;
    Operator:boolean;
    SocketID_arr:array of integer;
  end;

  TCorespElement = record
    ThreadID:cardinal;
    SocketID:integer;
    Time:TDateTime;
  end;

  PTTransferElement = ^TTransferElement;
  TTransferElement = record
    id:cardinal;
    TransferServer:TTransferServer;
    NextElement:PTTransferElement;
  end;

  PTThreadSocketElement = ^TThreadSocketElement;
  TThreadSocketElement = record
    SocketID:integer;
    LastTime:TDateTime;
    EchoInterval:integer;
    Registered:boolean;
    imei_reg:int64;
    //FileServer:cardinal;   //ID трансфер сервера для получения к нему доступа
    FileServer:TTransferServer;
    FileServerUpdateTime:TDateTime;
    buffer:byte_arr;
    Next_Element,Prev_Element:PTThreadSocketElement;
  end;

  TAnswerData = record
    Awaiting:boolean;
    SendTime:TDateTime;
    RequestReceiverSocket:integer;  //тот, кто должен послать ответ
    RequestSenderSocket:integer;   //тот, кто послал запрос
    RequestType:word;
  end;


function GetTickCount64:int64; stdcall;
function FileVersion(AFileName: string): string;

implementation

uses Windows, SysUtils;

function GetTickCount64; external 'kernel32.dll' name 'GetTickCount64';

function FileVersion(AFileName: string): string;
var
  szName: array[0..255] of Char;
  P: Pointer;
  Value: Pointer;
  Len: UINT;
  GetTranslationString: string;
  FFileName: PChar;
  FValid: boolean;
  FSize: DWORD;
  FHandle: DWORD;
  FBuffer: PChar;
begin
  result:='';

  try
    FFileName := StrPCopy(StrAlloc(Length(AFileName) + 1), AFileName);
    FValid := False;
    FSize := GetFileVersionInfoSize(FFileName, FHandle);
    FBuffer:=nil;
    if FSize > 0 then
    try
      GetMem(FBuffer, FSize);
      FValid := GetFileVersionInfo(FFileName, FHandle, FSize, FBuffer);
    except
      FValid := False;
      raise;
    end;
    Result := '';
    if FValid then
      VerQueryValue(FBuffer, '\VarFileInfo\Translation', p, Len)
    else
      p := nil;
    if P <> nil then
      GetTranslationString := IntToHex(MakeLong(HiWord(Longint(P^)),
        LoWord(Longint(P^))), 8);
    if FValid then
    begin
      StrPCopy(szName, '\StringFileInfo\' + GetTranslationString +
        '\FileVersion');
      if VerQueryValue(FBuffer, szName, Value, Len) then
        Result := StrPas(PChar(Value));
    end;
  finally
    try
      if FBuffer <> nil then
        FreeMem(FBuffer, FSize);
    except
    end;
    try
      StrDispose(FFileName);
    except
    end;
  end;
end;

end.
