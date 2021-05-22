package main

import (
	"flag"
	"log"

	"github.com/kcp-dev/kcp/pkg/reconciler/cluster"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/pkg/controlplane/clientutils"
)

const numThreads = 2

var (
	kubeconfigPath = flag.String("kubeconfig", "", "Path to kubeconfig")
	syncerImage    = flag.String("syncer_image", "", "Syncer image to install on clusters")
	pullMode       = flag.Bool("pull_mode", true, "Deploy the syncer in registered physical clusters in POD, and have it sync resources from KCP")
	pushMode       = flag.Bool("push_mode", false, "If true, run syncer for each cluster from inside cluster controller")
)

func main() {
	flag.Parse()

	configLoader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: *kubeconfigPath},
		&clientcmd.ConfigOverrides{})

	r, err := configLoader.ClientConfig()
	if err != nil {
		log.Fatal(err)
	}
	clientutils.EnableMultiCluster(r, nil, "clusters", "customresourcedefinitions")
	kubeconfig, err := configLoader.RawConfig()
	if err != nil {
		log.Fatal(err)
	}

	resourcesToSync := flag.Args()
	if len(resourcesToSync) == 0 {
		resourcesToSync = []string{"pods", "deployments"}
	}

	if *pullMode && *pushMode {
		log.Fatal("can't set --push_mode and --pull_mode")
	}
	syncerMode := cluster.SyncerModeNone
	if *pullMode {
		syncerMode = cluster.SyncerModePull
	}
	if *pushMode {
		syncerMode = cluster.SyncerModePush
	}


	for _, c := range kubeconfig.Clusters {
		c.Server ="https://host.docker.internal:6443"
	}
	cluster.NewController(r, *syncerImage, kubeconfig, resourcesToSync, syncerMode).Start(numThreads)
}
