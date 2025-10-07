@echo off
REM Este script ativa o ambiente virtual e inicia a aplicação Streamlit.

REM Ativa o ambiente virtual.
call .venv\Scripts\activate.bat

REM Inicia a aplicação.
echo Iniciando a aplicação Streamlit...
streamlit run dash_proto.py
