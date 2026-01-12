package main

import (
	"context"
	"fmt"

	"github.com/google/go-github/v57/github"
)

func main() {
	var token string
	fmt.Print("Enter GitHub token: ")
	fmt.Scanf("%s", &token)

	client := github.NewClient(nil).WithAuthToken(token)

	temp := "## Bug Report\n\nPlease answer these questions before submitting your issue. Thanks!\n\n### 1. Minimal reproduce step (Required)\n\n<!-- a step by step guide for reproducing the bug. -->\n\n### 2. What did you expect to see? (Required)\n\n### 3. What did you see instead (Required)\n\n### 4. What is your TiDB version? (Required)\n\n<!-- Paste the output of SELECT tidb_version() -->\n"

	ir := &github.IssueRequest{
		Title:  github.String("testapi"),
		Body:   github.String(temp),
		Labels: &[]string{"bug"},
	}
	_, _, err := client.Issues.Create(context.Background(), "name", "tidb", ir)
	if err != nil {
		fmt.Println(err)
	}
}
