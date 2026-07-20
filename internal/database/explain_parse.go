package database

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// ExplainNode captures one line from EXPLAIN (ANALYZE, BUFFERS) output.
type ExplainNode struct {
	Indent        int
	NodeType      string
	Relation      string
	ActualRows    int64
	ActualLoops   int64
	RowsRemoved   int64
	SharedHit     int64
	SharedRead    int64
	SharedDirtied int64
	SharedWritten int64
	Filter        string
	SortMethod    string
	Raw           string
}

// ExplainPlan is a parsed EXPLAIN ANALYZE result.
type ExplainPlan struct {
	PlanningTimeMS  float64
	ExecutionTimeMS float64
	Nodes           []ExplainNode
	Raw             string
}

// ExplainFinding flags a potentially suboptimal plan characteristic.
type ExplainFinding struct {
	Severity string // "warn" | "info"
	Query    string
	Message  string
	Detail   string
}

var (
	rePlanning   = regexp.MustCompile(`Planning Time: ([0-9.]+) ms`)
	reExecution  = regexp.MustCompile(`Execution Time: ([0-9.]+) ms`)
	reActualRows = regexp.MustCompile(`\(actual time=[^)]*rows=(\d+)`)
	reLoops      = regexp.MustCompile(`loops=(\d+)`)
	reRemoved    = regexp.MustCompile(`Rows Removed by Filter: (\d+)`)
	reBuffers    = regexp.MustCompile(`Buffers: shared hit=(\d+)(?: read=(\d+))?(?: dirtied=(\d+))?(?: written=(\d+))?`)
	reSortMethod = regexp.MustCompile(`Sort Method: ([^;]+)`)
)

// ParseExplainPlan parses PostgreSQL EXPLAIN ANALYZE text output.
func ParseExplainPlan(raw string) ExplainPlan {
	plan := ExplainPlan{Raw: raw}
	if m := rePlanning.FindStringSubmatch(raw); len(m) == 2 {
		plan.PlanningTimeMS, _ = strconv.ParseFloat(m[1], 64)
	}
	if m := reExecution.FindStringSubmatch(raw); len(m) == 2 {
		plan.ExecutionTimeMS, _ = strconv.ParseFloat(m[1], 64)
	}
	for _, line := range strings.Split(raw, "\n") {
		trim := strings.TrimSpace(line)
		if trim == "" || strings.HasPrefix(trim, "Planning") || strings.HasPrefix(trim, "Execution") {
			continue
		}
		indent := 0
		for _, ch := range line {
			if ch == ' ' {
				indent++
			} else {
				break
			}
		}
		node := ExplainNode{Indent: indent / 2, Raw: trim}
		if idx := strings.Index(trim, " on "); idx > 0 && strings.Contains(trim, "Scan") {
			parts := strings.SplitN(trim[idx+4:], " ", 2)
			node.Relation = strings.TrimSpace(parts[0])
		}
		switch {
		case strings.Contains(trim, "Seq Scan"):
			node.NodeType = "Seq Scan"
		case strings.Contains(trim, "Index Scan"):
			node.NodeType = "Index Scan"
		case strings.Contains(trim, "Index Only Scan"):
			node.NodeType = "Index Only Scan"
		case strings.Contains(trim, "Bitmap Index Scan"):
			node.NodeType = "Bitmap Index Scan"
		case strings.Contains(trim, "Bitmap Heap Scan"):
			node.NodeType = "Bitmap Heap Scan"
		case strings.Contains(trim, "Nested Loop"):
			node.NodeType = "Nested Loop"
		case strings.Contains(trim, "Hash Join"):
			node.NodeType = "Hash Join"
		case strings.Contains(trim, "Merge Join"):
			node.NodeType = "Merge Join"
		case strings.Contains(trim, "Aggregate"):
			node.NodeType = "Aggregate"
		case strings.Contains(trim, "Sort"):
			node.NodeType = "Sort"
		case strings.Contains(trim, "Limit"):
			node.NodeType = "Limit"
		default:
			if paren := strings.Index(trim, "("); paren > 0 {
				node.NodeType = strings.TrimSpace(trim[:paren])
			}
		}
		if m := reActualRows.FindStringSubmatch(trim); len(m) == 2 {
			node.ActualRows, _ = strconv.ParseInt(m[1], 10, 64)
		}
		if m := reLoops.FindStringSubmatch(trim); len(m) == 2 {
			node.ActualLoops, _ = strconv.ParseInt(m[1], 10, 64)
		}
		if m := reRemoved.FindStringSubmatch(trim); len(m) == 2 {
			removed, _ := strconv.ParseInt(m[1], 10, 64)
			if last := lastScanNode(plan.Nodes); last >= 0 {
				plan.Nodes[last].RowsRemoved = removed
			} else {
				node.RowsRemoved = removed
			}
			continue
		}
		if m := reBuffers.FindStringSubmatch(trim); len(m) >= 2 {
			node.SharedHit, _ = strconv.ParseInt(m[1], 10, 64)
			if len(m) > 2 && m[2] != "" {
				node.SharedRead, _ = strconv.ParseInt(m[2], 10, 64)
			}
			if len(m) > 3 && m[3] != "" {
				node.SharedDirtied, _ = strconv.ParseInt(m[3], 10, 64)
			}
			if len(m) > 4 && m[4] != "" {
				node.SharedWritten, _ = strconv.ParseInt(m[4], 10, 64)
			}
		}
		if m := reSortMethod.FindStringSubmatch(trim); len(m) == 2 {
			node.SortMethod = strings.TrimSpace(m[1])
		}
		if fidx := strings.Index(trim, "Filter:"); fidx >= 0 {
			filter := strings.TrimSpace(trim[fidx+7:])
			if last := lastScanNode(plan.Nodes); last >= 0 {
				plan.Nodes[last].Filter = filter
			} else {
				node.Filter = filter
			}
			continue
		}
		plan.Nodes = append(plan.Nodes, node)
	}
	return plan
}

func lastScanNode(nodes []ExplainNode) int {
	for i := len(nodes) - 1; i >= 0; i-- {
		if strings.Contains(nodes[i].NodeType, "Scan") {
			return i
		}
	}
	return -1
}

// AnalyzeExplainPlan returns findings for a query plan.
// smallTableRows: seq scans on relations with <= this many actual rows are ignored.
func AnalyzeExplainPlan(queryName string, plan ExplainPlan, hotPath bool, smallTableRows int64) []ExplainFinding {
	var out []ExplainFinding
	execLimit := 500.0
	if hotPath {
		execLimit = 50.0
	}
	if plan.ExecutionTimeMS > execLimit {
		out = append(out, ExplainFinding{
			Severity: "warn",
			Query:    queryName,
			Message:  fmt.Sprintf("execution time %.2f ms exceeds %.0f ms budget", plan.ExecutionTimeMS, execLimit),
		})
	}
	for _, n := range plan.Nodes {
		if n.NodeType == "Seq Scan" && n.ActualRows > smallTableRows {
			if n.RowsRemoved > n.ActualRows {
				out = append(out, ExplainFinding{
					Severity: "warn",
					Query:    queryName,
					Message:  fmt.Sprintf("seq scan on %s (%d rows) with %d rows removed by filter", n.Relation, n.ActualRows, n.RowsRemoved),
					Detail:   n.Raw,
				})
			} else if n.ActualRows > smallTableRows*10 {
				out = append(out, ExplainFinding{
					Severity: "info",
					Query:    queryName,
					Message:  fmt.Sprintf("seq scan on large relation %s (%d rows) — verify index coverage at scale", n.Relation, n.ActualRows),
					Detail:   n.Raw,
				})
			}
		}
		if n.NodeType == "Nested Loop" && n.ActualLoops > 1000 {
			out = append(out, ExplainFinding{
				Severity: "warn",
				Query:    queryName,
				Message:  fmt.Sprintf("nested loop with %d loops", n.ActualLoops),
				Detail:   n.Raw,
			})
		}
		if n.NodeType == "Sort" && (strings.Contains(n.SortMethod, "external") || strings.Contains(n.Raw, "Disk:")) {
			out = append(out, ExplainFinding{
				Severity: "warn",
				Query:    queryName,
				Message:  "sort spilled to disk",
				Detail:   n.Raw,
			})
		}
		if n.SharedRead > 1000 {
			out = append(out, ExplainFinding{
				Severity: "info",
				Query:    queryName,
				Message:  fmt.Sprintf("%s read %d shared buffers from disk", n.NodeType, n.SharedRead),
				Detail:   n.Raw,
			})
		}
	}
	return out
}

func collectExplainText(rows interface {
	Next() bool
	Scan(...any) error
}) (string, error) {
	var b strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", err
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String(), nil
}
