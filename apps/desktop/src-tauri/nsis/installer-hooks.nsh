!macro OrdaDBRunServiceCommand ACTION
  nsExec::ExecToLog '"$INSTDIR\ordadb-server.exe" service ${ACTION}'
  Pop $0
  ${If} $0 != 0
    Abort "OrdaDB service ${ACTION} failed with exit code $0."
  ${EndIf}
!macroend

!macro NSIS_HOOK_PREINSTALL
  ${If} ${FileExists} "$INSTDIR\ordadb-server.exe"
    !insertmacro OrdaDBRunServiceCommand "stop"
  ${EndIf}
!macroend

!macro NSIS_HOOK_POSTINSTALL
  !insertmacro OrdaDBRunServiceCommand "install"
  !insertmacro OrdaDBRunServiceCommand "start"
!macroend

!macro NSIS_HOOK_PREUNINSTALL
  ${If} ${FileExists} "$INSTDIR\ordadb-server.exe"
    !insertmacro OrdaDBRunServiceCommand "stop"
    !insertmacro OrdaDBRunServiceCommand "uninstall"
  ${EndIf}
!macroend

!macro NSIS_HOOK_POSTUNINSTALL
  ; ProgramData is intentionally preserved. Data deletion is a separate
  ; administrator action and is never part of the default uninstall path.
!macroend
