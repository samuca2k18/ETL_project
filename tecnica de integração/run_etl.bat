@echo off
setlocal
pushd "%~dp0"

REM Ativa venv se existir
if exist ..\.venv\Scripts\activate.bat (
  call ..\.venv\Scripts\activate.bat
)

python etl.py --config config.yaml

popd
endlocal

