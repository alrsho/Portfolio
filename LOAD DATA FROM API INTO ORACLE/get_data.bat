echo =====START %1 == %DATE% %TIME% ===

python.exe "D:\get_data.py" PRD > "D:\Logs\log_get_data_%date:~10%%date:~4,2%%date:~7,2%_%time:~0,2%%time:~3,2%%time:~6,2%.txt"

if %errorlevel% neq 0 (
    echo [ERROR] Python script failed with exit code %errorlevel%
    exit /b %errorlevel%
)


echo =====FINISH  %1 == %DATE% %TIME% ===