@echo off
:: Directory containing the compressed files
set "source_dir=C:\EPAM\spark\weather"
:: Destination directory for extracted files
set "output_dir=C:\EPAM\spark\weather"

:: Ensure the output directory exists
if not exist "%output_dir%" mkdir "%output_dir%"

:: Navigate to the source directory
pushd "%source_dir%"

:: Loop through each compressed file in the directory
for %%f in (*.zip *.7z *.rar *.tar *.gz *.bz2) do (
    echo Extracting "%%f"...
    "C:\Program Files\7-Zip\7z" x "%%f" -o"%output_dir%" -y
)

:: Return to the original directory
popd

echo All files extracted!
pause