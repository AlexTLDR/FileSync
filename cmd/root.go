/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "FileSync",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("\nvalid location\nscan complete\n")
		return nil
	},

	Args: func(cmd *cobra.Command, args []string) error {
		if len(os.Args) < 2 {
			fmt.Println("Usage: go run scanner.go <directory>")
			os.Exit(1)
		}

		root := os.Args[1]

		fmt.Println("Scanning directory:", root)

		err := filepath.Walk(root, visitFile)

		if err != nil {
			fmt.Println("Error:", err)
		}
		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.FileSync.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func visitFile(path string, info os.FileInfo, err error) error {
	if err != nil {
		fmt.Println("Error:", err)
		return nil
	}

	if !info.IsDir() {
		fmt.Println("File:", path)
	}

	return nil
}
