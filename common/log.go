package common

import (
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// 日志级别：debug < info < warn < error < off
const (
	LevelDebug = 0
	LevelInfo  = 1
	LevelWarn  = 2
	LevelError = 3
	LevelOff   = 4
)

var (
	infoLogger  = log.New(os.Stdout, "", 0)
	errorLogger = log.New(os.Stderr, "", 0)
	curLevel    atomic.Int32
)

func init() {
	curLevel.Store(LevelOff)
}

// ParseLevel 解析级别字符串
func ParseLevel(s string) int32 {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	case "off", "none", "silent", "":
		return LevelOff
	default:
		return LevelInfo
	}
}

// LevelName 级别名
func LevelName(lv int32) string {
	switch lv {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "off"
	}
}

// SetLevel 设置日志级别
func SetLevel(level string) {
	curLevel.Store(ParseLevel(level))
}

// GetLevel 当前级别
func GetLevel() int32 {
	return curLevel.Load()
}

func enabled(lv int32) bool {
	cur := curLevel.Load()
	if cur >= LevelOff {
		return false
	}
	return lv >= cur
}

func ts() string {
	return time.Now().Format("2006-01-02 15:04:05")
}

func Debug(format string, args ...interface{}) {
	if !enabled(LevelDebug) {
		return
	}
	infoLogger.Printf("[DEBUG] %s "+format, append([]interface{}{ts()}, args...)...)
}

func Info(format string, args ...interface{}) {
	if !enabled(LevelInfo) {
		return
	}
	infoLogger.Printf("[INFO] %s "+format, append([]interface{}{ts()}, args...)...)
}

func Warn(format string, args ...interface{}) {
	if !enabled(LevelWarn) {
		return
	}
	infoLogger.Printf("[WARN] %s "+format, append([]interface{}{ts()}, args...)...)
}

func Error(format string, args ...interface{}) {
	if !enabled(LevelError) {
		return
	}
	errorLogger.Printf("[ERROR] %s "+format, append([]interface{}{ts()}, args...)...)
}
