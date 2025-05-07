package main

import (
	"github.com/fatih/color"
)

// Logger provides colorful logging functionality
type Logger struct {
	info    *color.Color
	success *color.Color
	warning *color.Color
	error   *color.Color
	debug   *color.Color
}

// NewLogger creates a new Logger instance
func NewLogger() *Logger {
	return &Logger{
		info:    color.New(color.FgCyan),
		success: color.New(color.FgGreen),
		warning: color.New(color.FgYellow),
		error:   color.New(color.FgRed),
		debug:   color.New(color.FgMagenta),
	}
}

// Info logs informational messages in cyan
func (l *Logger) Info(format string, args ...interface{}) {
	l.info.Printf("[INFO] "+format+"\n", args...)
}

// Success logs success messages in green
func (l *Logger) Success(format string, args ...interface{}) {
	l.success.Printf("[SUCCESS] "+format+"\n", args...)
}

// Warning logs warning messages in yellow
func (l *Logger) Warning(format string, args ...interface{}) {
	l.warning.Printf("[WARNING] "+format+"\n", args...)
}

// Error logs error messages in red
func (l *Logger) Error(format string, args ...interface{}) {
	l.error.Printf("[ERROR] "+format+"\n", args...)
}

// Debug logs debug messages in magenta
func (l *Logger) Debug(format string, args ...interface{}) {
	l.debug.Printf("[DEBUG] "+format+"\n", args...)
}

// HighlightValue returns a highlighted version of a value
func (l *Logger) HighlightValue(value interface{}) string {
	return color.New(color.FgHiWhite, color.Bold).Sprint(value)
}

// HighlightFile returns a highlighted version of a file path
func (l *Logger) HighlightFile(path string) string {
	return color.New(color.FgHiBlue).Sprint(path)
}

// HighlightID returns a highlighted version of an ID
func (l *Logger) HighlightID(id string) string {
	return color.New(color.FgHiGreen).Sprint(id)
}

// HighlightDate returns a highlighted version of a date
func (l *Logger) HighlightDate(date string) string {
	return color.New(color.FgHiYellow).Sprint(date)
} 