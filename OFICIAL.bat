@echo off
cd /d "%~dp0"

echo Iniciando o serviço com.docker.service...
net start com.docker.service

echo.
echo Iniciando Docker Desktop...
start "" "C:\Program Files\Docker\Docker\Docker Desktop.exe"

echo Aguardando 15 segundos para o Docker Desktop inicializar...
timeout /t 15 /nobreak >nul

echo.
pause

echo Limpando containers parados e existentes...
FOR /f "tokens=*" %%i IN ('docker ps -q -a') DO docker stop %%i >nul 2>&1
FOR /f "tokens=*" %%i IN ('docker ps -q -a') DO docker rm %%i >nul 2>&1

echo.
echo Iniciando containers com docker-compose...
docker-compose up -d 

echo.
pause