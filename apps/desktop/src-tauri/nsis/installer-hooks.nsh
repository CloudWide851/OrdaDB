!define ORDA_INSTALLER_CLI_SOURCE "${__FILEDIR__}\..\staging\windows-x64\ordadb.exe"

Var OrdaInstallerCli
Var OrdaInstallerDataDir
Var OrdaInstallerReceipt
Var OrdaInstallerState
Var OrdaInstallerReport
Var OrdaInstallerExit
Var OrdaInstallerOutput
Var OrdaInstallerDisposition
Var OrdaInstallerSafe
Var OrdaInstallerSummary
Var OrdaInstallerRequiredBytes
Var OrdaInstallerFreeBytes
Var OrdaInstallerBackupPath
Var OrdaInstallerRollbackPath
Var OrdaInstallerFailureState
Var OrdaInstallerFailurePhase
Var OrdaInstallerFailureReason
Var OrdaInstallerFailureHint
Var OrdaInstallerReceiptDigest
Var OrdaInstallerOriginalDigest
Var OrdaInstallerOriginalError
Var OrdaInstallerConfirmed
Var OrdaInstallerPassive

!macro OrdaDBResolveDataDir
  ; Tauri sets ShellVarContext to all for this per-machine installer, so
  ; $APPDATA resolves to the common ProgramData application-data directory.
  StrCpy $OrdaInstallerDataDir "$APPDATA\OrdaDB\data"
  ClearErrors
  ${GetOptions} $CMDLINE "/DATA-DIR=" $OrdaInstallerOutput
  ${IfNot} ${Errors}
    ${If} $OrdaInstallerOutput != ""
      StrCpy $OrdaInstallerDataDir $OrdaInstallerOutput
    ${EndIf}
  ${EndIf}
  StrCpy $OrdaInstallerPassive 0
  ClearErrors
  ${GetOptions} $CMDLINE "/P" $OrdaInstallerOutput
  ${IfNot} ${Errors}
    StrCpy $OrdaInstallerPassive 1
  ${EndIf}
!macroend

!macro OrdaDBRunServiceCommand ACTION
  nsExec::ExecToStack '"$INSTDIR\ordadb-server.exe" service ${ACTION} --data-dir "$OrdaInstallerDataDir"'
  Pop $OrdaInstallerExit
  Pop $OrdaInstallerOutput
  ${If} $OrdaInstallerExit != 0
    Abort "OrdaDB service ${ACTION} failed with exit code $OrdaInstallerExit.$\r$\n$\r$\n$OrdaInstallerOutput"
  ${EndIf}
!macroend

Function OrdaDBExtractInstallerCli
  InitPluginsDir
  StrCpy $OrdaInstallerCli "$PLUGINSDIR\ordadb-installer-cli.exe"
  StrCpy $OrdaInstallerReceipt "$PLUGINSDIR\ordadb-installer-storage-receipt-v1.json"
  StrCpy $OrdaInstallerState "$PLUGINSDIR\ordadb-installer-storage-state-v1.ini"
  StrCpy $OrdaInstallerReport "$PLUGINSDIR\ordadb-installer-storage-report-v1.json"
  Delete $OrdaInstallerCli
  Delete $OrdaInstallerReceipt
  Delete $OrdaInstallerState
  Delete $OrdaInstallerReport
  File "/oname=$PLUGINSDIR\ordadb-installer-cli.exe" "${ORDA_INSTALLER_CLI_SOURCE}"
FunctionEnd

Function OrdaDBRunStoragePreflight
  Delete $OrdaInstallerReceipt
  Delete $OrdaInstallerState
  nsExec::ExecToStack '"$OrdaInstallerCli" installer-storage --preflight "$OrdaInstallerReceipt" --state "$OrdaInstallerState" --data-dir "$OrdaInstallerDataDir"'
  Pop $OrdaInstallerExit
  Pop $OrdaInstallerOutput
  ReadINIStr $OrdaInstallerDisposition $OrdaInstallerState "installer" "disposition"
  ReadINIStr $OrdaInstallerSafe $OrdaInstallerState "installer" "safeToApply"
  ReadINIStr $OrdaInstallerSummary $OrdaInstallerState "installer" "summary"
  ReadINIStr $OrdaInstallerRequiredBytes $OrdaInstallerState "installer" "requiredBytes"
  ReadINIStr $OrdaInstallerFreeBytes $OrdaInstallerState "installer" "freeBytes"
  ReadINIStr $OrdaInstallerBackupPath $OrdaInstallerState "installer" "backupPath"
  ReadINIStr $OrdaInstallerRollbackPath $OrdaInstallerState "installer" "rollbackPath"
  ReadINIStr $OrdaInstallerFailureState $OrdaInstallerState "installer" "failureSqlState"
  ReadINIStr $OrdaInstallerFailurePhase $OrdaInstallerState "installer" "failurePhase"
  ReadINIStr $OrdaInstallerFailureReason $OrdaInstallerState "installer" "failureReason"
  ReadINIStr $OrdaInstallerFailureHint $OrdaInstallerState "installer" "failureHint"
  ReadINIStr $OrdaInstallerReceiptDigest $OrdaInstallerState "installer" "receiptDigest"
FunctionEnd

Function OrdaDBRequireSafePreflight
  ${If} $OrdaInstallerExit != 0
    Abort "OrdaDB storage preflight failed with exit code $OrdaInstallerExit.$\r$\nSQLSTATE: $OrdaInstallerFailureState$\r$\nPhase: $OrdaInstallerFailurePhase$\r$\nReason: $OrdaInstallerFailureReason$\r$\nHint: $OrdaInstallerFailureHint$\r$\n$\r$\n$OrdaInstallerOutput"
  ${EndIf}
  ${If} $OrdaInstallerSafe != "1"
    Abort "OrdaDB storage is not safe to upgrade.$\r$\nDisposition: $OrdaInstallerDisposition$\r$\n$OrdaInstallerSummary$\r$\nSQLSTATE: $OrdaInstallerFailureState$\r$\nPhase: $OrdaInstallerFailurePhase$\r$\nReason: $OrdaInstallerFailureReason$\r$\nHint: $OrdaInstallerFailureHint"
  ${EndIf}
FunctionEnd

Function OrdaDBConfirmLegacyPlan
  StrCpy $OrdaInstallerConfirmed 1
  ${If} $OrdaInstallerDisposition == "legacyV1"
  ${AndIfNot} ${Silent}
  ${AndIf} $OrdaInstallerPassive != 1
    MessageBox MB_ICONEXCLAMATION|MB_YESNO|MB_DEFBUTTON2 \
      "OrdaDB must migrate the existing storage before this upgrade.$\r$\n$\r$\n$OrdaInstallerSummary$\r$\nRequired space: $OrdaInstallerRequiredBytes bytes$\r$\nAvailable space: $OrdaInstallerFreeBytes bytes$\r$\nLogical backup: $OrdaInstallerBackupPath$\r$\nRollback copy: $OrdaInstallerRollbackPath$\r$\n$\r$\nContinue with the offline migration?" \
      IDYES confirmed IDNO declined
    declined:
      StrCpy $OrdaInstallerConfirmed 0
      Return
    confirmed:
  ${EndIf}
FunctionEnd

Function OrdaDBApplyStorage
  Delete $OrdaInstallerReport
  nsExec::ExecToStack '"$OrdaInstallerCli" installer-storage --apply "$OrdaInstallerReceipt" --report "$OrdaInstallerReport" --data-dir "$OrdaInstallerDataDir"'
  Pop $OrdaInstallerExit
  Pop $OrdaInstallerOutput
FunctionEnd

Function OrdaDBTryRestorePreviousService
  ${If} ${FileExists} "$INSTDIR\ordadb-server.exe"
    nsExec::ExecToLog '"$INSTDIR\ordadb-server.exe" service start --data-dir "$OrdaInstallerDataDir"'
    Pop $OrdaInstallerExit
    ${If} $OrdaInstallerExit != 0
      DetailPrint "The previous OrdaDB service could not be restarted (exit $OrdaInstallerExit)."
    ${EndIf}
  ${EndIf}
FunctionEnd

!macro NSIS_HOOK_PREINSTALL
  !insertmacro OrdaDBResolveDataDir
  Call OrdaDBExtractInstallerCli
  Call OrdaDBRunStoragePreflight
  Call OrdaDBRequireSafePreflight
  StrCpy $OrdaInstallerOriginalDigest $OrdaInstallerReceiptDigest
  Call OrdaDBConfirmLegacyPlan
  ${If} $OrdaInstallerConfirmed != 1
    Abort "OrdaDB storage migration was cancelled before the service or product files were changed."
  ${EndIf}

  ${If} ${FileExists} "$INSTDIR\ordadb-server.exe"
    !insertmacro OrdaDBRunServiceCommand "stop"
  ${EndIf}

  Call OrdaDBApplyStorage
  ${If} $OrdaInstallerExit != 0
    StrCpy $OrdaInstallerOriginalError $OrdaInstallerOutput
    Call OrdaDBRunStoragePreflight
    ${If} $OrdaInstallerExit != 0
      Abort "OrdaDB storage migration failed and a fresh preflight could not be produced.$\r$\n$\r$\n$OrdaInstallerOriginalError$\r$\n$\r$\n$OrdaInstallerOutput"
    ${EndIf}
    ${If} $OrdaInstallerSafe != "1"
      Call OrdaDBTryRestorePreviousService
      Abort "OrdaDB storage changed to an unsafe state after confirmation.$\r$\nDisposition: $OrdaInstallerDisposition$\r$\n$OrdaInstallerSummary$\r$\nReason: $OrdaInstallerFailureReason$\r$\nHint: $OrdaInstallerFailureHint"
    ${EndIf}
    ${If} $OrdaInstallerReceiptDigest == $OrdaInstallerOriginalDigest
      Abort "OrdaDB storage migration failed without a source-plan change. The service remains stopped to protect the authoritative data.$\r$\n$\r$\n$OrdaInstallerOriginalError"
    ${EndIf}

    Call OrdaDBConfirmLegacyPlan
    ${If} $OrdaInstallerConfirmed != 1
      Call OrdaDBTryRestorePreviousService
      Abort "The changed OrdaDB storage migration plan was declined. Product files were not copied."
    ${EndIf}
    Call OrdaDBApplyStorage
    ${If} $OrdaInstallerExit != 0
      Abort "OrdaDB storage migration failed after one safe replan. The service remains stopped to protect the authoritative data.$\r$\n$\r$\n$OrdaInstallerOutput"
    ${EndIf}
  ${EndIf}
!macroend

!macro NSIS_HOOK_POSTINSTALL
  !insertmacro OrdaDBRunServiceCommand "install"
  !insertmacro OrdaDBRunServiceCommand "start"
!macroend

!macro NSIS_HOOK_PREUNINSTALL
  !insertmacro OrdaDBResolveDataDir
  ${If} ${FileExists} "$INSTDIR\ordadb-server.exe"
    !insertmacro OrdaDBRunServiceCommand "stop"
    !insertmacro OrdaDBRunServiceCommand "uninstall"
  ${EndIf}
!macroend

!macro NSIS_HOOK_POSTUNINSTALL
  ; ProgramData is intentionally preserved. Data deletion is a separate
  ; administrator action and is never part of the default uninstall path.
!macroend
