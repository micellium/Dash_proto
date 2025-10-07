@echo off
REM Este script configura o ambiente virtual e instala as dependências.

REM Verifica se o diretório .venv existe.
IF NOT EXIST .venv (
    echo Criando ambiente virtual...
    REM Certifique-se de que 'python' se refere ao Python 3.10 ou superior.
    python -m venv .venv
) ELSE (
    echo Ambiente virtual .venv já existe.
)

REM Ativa o ambiente virtual e instala os requisitos.
call .venv\Scripts\activate.bat
echo Instalando dependências de requirements.txt...
pip install -r requirements.txt

echo.
echo Configuração concluída. Para iniciar a aplicação, execute o arquivo 'run.bat'.
pause