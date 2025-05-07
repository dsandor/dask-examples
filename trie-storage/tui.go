package main

import (
	"fmt"
	"sync"

	"github.com/rivo/tview"
)

// TUI manages the terminal user interface
type TUI struct {
	app         *tview.Application
	progress    []*tview.TextView
	status      *tview.TextView
	mu          sync.Mutex
	workerCount int
}

// NewTUI creates a new TUI instance
func NewTUI(workerCount int) *TUI {
	return &TUI{
		app:         tview.NewApplication(),
		progress:    make([]*tview.TextView, workerCount),
		status:      tview.NewTextView().SetDynamicColors(true),
		workerCount: workerCount,
	}
}

// Start begins the TUI application
func (t *TUI) Start() {
	grid := tview.NewGrid().
		SetRows(0, 1).
		SetColumns(-1).
		SetBorders(true)

	// Create progress bars for each worker
	for i := 0; i < t.workerCount; i++ {
		t.progress[i] = tview.NewTextView().
			SetDynamicColors(true).
			SetText(fmt.Sprintf("Worker %d: [░░░░░░░░░░░░░░░░░░░░] 0%%", i+1))
		grid.AddItem(t.progress[i], i, 0, 1, 1, 0, 0, false)
	}

	// Add status text at the bottom
	grid.AddItem(t.status, t.workerCount, 0, 1, 1, 0, 0, false)

	t.app.SetRoot(grid, true)
	go t.app.Run()
}

// Stop ends the TUI application
func (t *TUI) Stop() {
	t.app.Stop()
}

// UpdateProgress updates the progress of a specific worker
func (t *TUI) UpdateProgress(workerID int, progress float64, status string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if workerID >= 0 && workerID < len(t.progress) {
		bar := createProgressBar(progress)
		t.progress[workerID].SetText(fmt.Sprintf("Worker %d: %s %.1f%%", workerID+1, bar, progress))
	}

	if status != "" {
		t.status.SetText(status)
	}
}

// createProgressBar creates a visual progress bar
func createProgressBar(progress float64) string {
	const width = 20
	filled := int(float64(width) * progress / 100)
	empty := width - filled

	bar := ""
	for i := 0; i < filled; i++ {
		bar += "█"
	}
	for i := 0; i < empty; i++ {
		bar += "░"
	}

	return fmt.Sprintf("[%s]", bar)
} 