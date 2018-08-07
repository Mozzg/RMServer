program RMServer;

uses
  SvcMgr,
  mainUnit in 'mainUnit.pas' {RMServer_main: TService},
  typesUnit in 'typesUnit.pas',
  TransferServerTypeUnit in 'TransferServerTypeUnit.pas',
  QueueTypeUnit in 'QueueTypeUnit.pas',
  SrvClientUnit in 'SrvClientUnit.pas',
  ThreadTypeUnit in 'ThreadTypeUnit.pas',
  TableTypeUnit in 'TableTypeUnit.pas';

{$R *.RES}

begin
  Application.Initialize;
  Application.Title := 'Rapid Maintence Server';
  Application.CreateForm(TRMServer_main, RMServer_main);
  Application.Run;
end.
