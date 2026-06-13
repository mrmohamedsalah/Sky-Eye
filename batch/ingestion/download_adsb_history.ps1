# Download ADS-B historical archive release page / files.
# Replace the URL with the exact release or archive files you need.

$releaseUrl = "https://github.com/adsblol/globe_history_2026/releases?q=2026.04.26&expanded=true"

Write-Host "Downloading ADS-B archive from: $releaseUrl"

wget $releaseUrl
