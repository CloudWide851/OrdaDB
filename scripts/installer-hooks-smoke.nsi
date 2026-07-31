Unicode true

!include FileFunc.nsh
!include LogicLib.nsh
!include "${__FILEDIR__}\..\apps\desktop\src-tauri\nsis\installer-hooks.nsh"

Name "OrdaDB Installer Hooks Smoke"
OutFile "${__FILEDIR__}\..\target\installer-hooks-smoke.exe"
InstallDir "$TEMP\OrdaDB-Installer-Hooks-Smoke"
RequestExecutionLevel user

Section "Install"
  SetShellVarContext all
  SetOutPath "$INSTDIR"
  !insertmacro NSIS_HOOK_PREINSTALL
  !insertmacro NSIS_HOOK_POSTINSTALL
  WriteUninstaller "$INSTDIR\uninstall.exe"
SectionEnd

Section "Uninstall"
  SetShellVarContext all
  !insertmacro NSIS_HOOK_PREUNINSTALL
  !insertmacro NSIS_HOOK_POSTUNINSTALL
SectionEnd
