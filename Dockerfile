# 基础镜像（与 sql-plugs 一致）
FROM hub.hzbxhd.com/middleware/go-mini:zh

WORKDIR /go/src/es-adb

COPY es-adb .

# 配置由 K8s 挂载到 /config/config.yaml，不打进镜像
EXPOSE 8080

CMD ["./es-adb", "-c", "/config/config.yaml"]
