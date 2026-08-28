# 从本地 config.yaml 生成 ConfigMap YAML
# 用法: .\scripts\gen-configmap.ps1
#       .\scripts\gen-configmap.ps1 -Src config\config.yaml
# 输出: deploy/configmap.yaml（含密钥，勿提交 git）

param(
    [string]$Src = "",
    [string]$Out = "deploy/configmap.yaml",
    [string]$Name = "es-adb-config",
    [string]$Namespace = "metabase"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

if (-not $Src) {
    if (Test-Path "config\config.yaml") { $Src = "config\config.yaml" }
    elseif (Test-Path "config.yaml") { $Src = "config.yaml" }
    else { throw "未找到配置文件，请指定 -Src 或放置 config/config.yaml" }
}

if (-not (Test-Path $Src)) { throw "文件不存在: $Src" }

$content = Get-Content $Src -Raw -Encoding UTF8
# ConfigMap 多行字符串每行前加 4 空格
$indented = ($content -split "`n" | ForEach-Object { "    " + $_.TrimEnd("`r") }) -join "`n"

$yaml = @"
# 由 scripts/gen-configmap.ps1 从本地配置生成，含密钥勿提交
# kubectl apply -f deploy/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: $Name
  namespace: $Namespace
data:
  config.yaml: |
$indented
"@

$dir = Split-Path $Out -Parent
if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
Set-Content -Path $Out -Value $yaml -Encoding UTF8 -NoNewline
Write-Host "generated: $Out from $Src"
