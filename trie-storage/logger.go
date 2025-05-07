package main

import (
	"fmt"
	"strings"
	"github.com/fatih/color"
)

// Logger provides colorful logging functionality
type Logger struct {
	info    *color.Color
	success *color.Color
	warning *color.Color
	error   *color.Color
	debug   *color.Color
	progress *color.Color
	bar     *color.Color
}

// NewLogger creates a new Logger instance
func NewLogger() *Logger {
	return &Logger{
		info:    color.New(color.FgCyan),
		success: color.New(color.FgGreen),
		warning: color.New(color.FgYellow),
		error:   color.New(color.FgRed),
		debug:   color.New(color.FgMagenta),
		progress: color.New(color.FgHiCyan),
		bar:     color.New(color.FgHiGreen),
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

// HighlightFile highlights a file path in bright blue
func (l *Logger) HighlightFile(file string) string {
	return fmt.Sprintf("\033[1;36m%s\033[0m", file) // Cyan color for files
}

// HighlightID returns a highlighted version of an ID
func (l *Logger) HighlightID(id string) string {
	return color.New(color.FgHiGreen).Sprint(id)
}

// HighlightDate returns a highlighted version of a date
func (l *Logger) HighlightDate(date string) string {
	return color.New(color.FgHiYellow).Sprint(date)
}

// ProgressBar displays a progress bar with percentage
func (l *Logger) ProgressBar(filename string, percentage float64) {
	const width = 50
	filled := int(float64(width) * percentage / 100)
	empty := width - filled

	// Create the progress bar
	bar := fmt.Sprintf("[%s%s] %.1f%%",
		strings.Repeat("█", filled),
		strings.Repeat("░", empty),
		percentage)

	// Clear the current line and print the progress
	fmt.Print("\r")
	l.progress.Printf("Processing %s: %s", l.HighlightFile(filename), l.bar.Sprint(bar))
}

// ClearProgress clears the progress bar line
func (l *Logger) ClearProgress() {
	fmt.Print("\r\033[K") // Clear the current line
} 