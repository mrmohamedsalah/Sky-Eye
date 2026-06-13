# Concatenate split tar archive files and extract them.

$archivePrefix = "v2026.04.08-planes-readsb-staging-0.tar.a*"
$outputTar = "full.tar"
$outputFolder = "extracted"

Write-Host "Combining archive parts..."
$outputStream = [System.IO.File]::Create((Resolve-Path .).Path + [System.IO.Path]::DirectorySeparatorChar + $outputTar)
try {
    Get-ChildItem -Path $archivePrefix | Sort-Object Name | ForEach-Object {
        $inputStream = [System.IO.File]::OpenRead($_.FullName)
        try {
            $inputStream.CopyTo($outputStream)
        }
        finally {
            $inputStream.Close()
        }
    }
}
finally {
    $outputStream.Close()
}

Write-Host "Creating output folder..."
mkdir $outputFolder -Force

Write-Host "Extracting archive..."
tar -xf $outputTar -C $outputFolder

Write-Host "Extraction complete."
