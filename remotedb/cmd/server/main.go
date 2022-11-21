package main

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/tendermint/tm-db/remotedb/grpcdb"
)

var (
	rootCmd = &cobra.Command{
		Use:   "remptedb sever",
		Short: "remptedb sever",
		Long:  "remptedb sever",
	}

	serverCmd = &cobra.Command{
		Use:   "server",
		Short: "启动http服务,使用方法: app server --addr=?",
		Run: func(cmd *cobra.Command, args []string) {
			if addr == "" {
				fmt.Println("addr不能为空!")
				os.Exit(-1)
			}
			if err := grpcdb.ListenAndServe(addr); err != nil {
				log.Fatalf("BindServer: %v", err)
			}
		},
	}
	addr string
)

func main() {
	// rootCmd.AddCommand(serverCmd)
	// serverCmd.Flags().StringVar(&addr, "addr", "", "地址")

	// if err := rootCmd.Execute(); err != nil {
	// 	fmt.Println(err)
	// 	os.Exit(1)
	// }

	if err := grpcdb.ListenAndServe("127.0.0.1:4321"); err != nil {
		log.Fatalf("BindServer: %v", err)
	}
}
