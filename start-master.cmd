@echo off
CHCP 65001
setlocal enabledelayedexpansion
set SYSID=batch-master
title %SYSID% Service
set RPATH=%~dp0

rem
rem custom jdk 
rem
if "%JAVA_HOME%"=="" (
	set JAVA_HOME=D:\Java\jdk1.8.0_121
)
if not exist %JAVA_HOME% (
	echo "No JAVA_HOME setting, application will exit."
	pause
	exit
)
set Path=%JAVA_HOME%/bin;%Path%

echo ============================
echo Loading Jars...
echo ============================
set CLASSPATH=.;%JAVA_HOME%\lib;%JAVA_HOME%\lib\tools.jar
set TMPFILE=%RPATH%\%SYSID%.tmp
dir /b *.jar >%TMPFILE%
for /f "tokens=*" %%i in (%TMPFILE%) do (
	set JAR=%%i
	set CLASSPATH=!CLASSPATH!;!JAR!
)
del /f /s /q %TMPFILE%
cd lib
dir /b *.jar >%TMPFILE%
for /f "tokens=*" %%i in (%TMPFILE%) do (
	set JAR=lib/%%i
	set CLASSPATH=!CLASSPATH!;!JAR!
)
del /f /s /q %TMPFILE%
echo %CLASSPATH%
cd %RPATH%

echo ============================
echo Start Service
echo ============================
if "%JAVA_OPTS%"=="" (
	set JAVA_OPTS=-Xms64m -Xmx1024m
)
set JAVA_PROP=-Dbootstrap.appid=%SYSID%
set LOGDIR=%RPATH%\logs\master

if exist %LOGDIR% (
	del /f /s /q %LOGDIR%
)

cd %RPATH%
java -server %JAVA_OPTS% %JAVA_PROP% -cp %CLASSPATH% -Dspring.profiles.active=master com.iceps.spring.launch.ApplicationRmq

pause
