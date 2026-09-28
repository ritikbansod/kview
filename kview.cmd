@echo off
rem Kview CLI (direct mode) — wraps the released/built kview jar.
rem Usage: kview topics --bootstrap-server localhost:9092
setlocal enabledelayedexpansion
set "DIR=%~dp0"
if defined KVIEW_JAR (
  set "JAR=%KVIEW_JAR%"
) else (
  for /f "delims=" %%f in ('dir /b /o:n "%DIR%kview-*.jar" 2^>nul') do set "JAR=%DIR%%%f"
)
if not defined JAR (
  echo kview jar not found in %DIR% — build one ^(mvn package^) or set KVIEW_JAR >&2
  exit /b 1
)
java -jar "%JAR%" --cli %*
