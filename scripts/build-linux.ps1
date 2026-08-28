# 交叉编译 Linux amd64 并构建 Docker 镜像
# 用法: .\scripts\build-linux.ps1
#       .\scripts\build-linux.ps1 -Push   # 构建后推送到仓库

param(
    [string]$Image = "prohub.hzbxhd.com/middleware/es-adb:1.0",
    [bool]$Push = $false
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

Write-Host "==> 交叉编译 Linux amd64 ..."
$env:GOOS = "linux"
$env:GOARCH = "amd64"
$env:CGO_ENABLED = "0"
go build -ldflags="-s -w" -o es-adb .

Write-Host "==> 构建镜像 $Image ..."
docker build -t $Image .

if ($Push) {
    Write-Host "==> 推送镜像 ..."
    docker push $Image
}

Write-Host "完成: $Image"
